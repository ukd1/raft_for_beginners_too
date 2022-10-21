mod follower;
mod candidate;
mod leader;
mod state;

use std::any::Any;
use std::{result::Result as StdResult, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio::{task::JoinHandle, time::{Duration, Instant, sleep_until}};
use tracing::{info, debug};

use state::ServerState;

use crate::journal::Journal;
use crate::connection::{ConnectionError, Packet, Connection};

use self::state::{Follower, Candidate, Leader};

pub type Result<T> = std::result::Result<T, ServerError>;
type StateResult<T> = Result<(T, Option<Packet>)>;
pub type ServerHandle = JoinHandle<Result<()>>;

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error(transparent)]
    ConnectionFailed(#[from] ConnectionError),
    #[error(transparent)]
    TaskPanicked(#[from] tokio::task::JoinError),
    #[error("could not convert native integer to wire size")]
    IntegerOverflow(#[from] std::num::TryFromIntError),
}

#[derive(Debug)]
pub struct Server<S: ServerState, C: Connection> {
    connection: C,
    config: crate::config::Config,
    journal: Journal,
    pub state: S,
    pub term: AtomicU64,
    span: std::sync::Mutex<tracing::Span>,
}

enum ServerImpl<'s, C: Connection> {
    Follower(&'s Server<Follower, C>),
    Candidate(&'s Server<Candidate, C>),
    Leader(&'s Server<Leader, C>),
}

pub(crate) enum HandlePacketAction {
    MaintainState(Option<Packet>),
    ChangeState(Option<Packet>),
}

impl<C: Connection> ServerImpl<'_, C> {
    pub async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        match self {
            ServerImpl::Follower(f) => f.handle_packet(packet).await,
            ServerImpl::Candidate(c) => c.handle_packet(packet).await,
            ServerImpl::Leader(l) => l.handle_packet(packet).await,
        }
    }

    pub async fn handle_timeout(&self) -> Result<HandlePacketAction> {
        match self {
            ServerImpl::Follower(f) => f.handle_timeout().await,
            ServerImpl::Candidate(c) => c.handle_timeout().await,
            ServerImpl::Leader(_) => unimplemented!("no Leader timeout"),
        }
    }
}

impl<S: ServerState, C: Connection> Server<S, C> {
    fn generate_random_timeout(min: Duration, max: Duration) -> Instant {
        let min = min.as_millis() as u64;
        let max = max.as_millis() as u64;
        let new_timeout_millis = rand::thread_rng().gen_range(min..max);
        let new_timeout = Duration::from_millis(new_timeout_millis);
        Instant::now() + new_timeout
    }

    /// get the timeout for a term; either when an election times out,
    /// or timeout for followers not hearing from the leader
    async fn reset_term_timeout(&self) {
        // https://stackoverflow.com/questions/19671845/how-can-i-generate-a-random-number-within-a-range-in-rust
        let new_timeout = Self::generate_random_timeout(self.config.election_timeout_min, self.config.election_timeout_max);
        self.state.set_timeout(new_timeout);
    }
   
    async fn update_term(&self, packet: &Packet) -> StdResult<u64, u64> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term > current_term {
            info!(term = %packet.term, "newer term in packet");
            self.term.store(packet.term, Ordering::Release);
            Err(packet.term)
        } else {
            Ok(packet.term)
        }
    }

    fn in_state<T: ServerState>(&self) -> bool {
        let dyn_state = &self.state as &dyn Any;
        dyn_state.is::<T>()
    }

    fn downcast(&self) -> ServerImpl<C> {
        let dyn_self = self as &dyn Any;
        if let Some(follower) = dyn_self.downcast_ref::<Server<Follower, C>>() {
            ServerImpl::Follower(follower)
        } else if let Some(candidate) = dyn_self.downcast_ref::<Server<Candidate, C>>() {
            ServerImpl::Candidate(candidate)
        } else if let Some(leader) = dyn_self.downcast_ref::<Server<Leader, C>>() {
            ServerImpl::Leader(leader)
        } else {
            unimplemented!("Invalid server state: {}", self.state);
        }
    }

    #[tracing::instrument(
        level = tracing::Level::TRACE,
        name = "handle_packet",
        parent = self.span.try_lock()
            .map_or_else(|_| tracing::Span::current(), |s| s.clone().or_current())
    )]
    async fn handle_packet_downcast(&self, packet: Packet) -> Result<HandlePacketAction> {
        use HandlePacketAction::*;

        if let Err(_) = self.update_term(&packet).await {
            // Term from packet was greater than our term,
            // transition to Follower
            if !self.in_state::<Follower>() {
                // A new term should trigger a state transition for Leaders
                // and Candidates, but a Follower should remain a Follower
                // in the new term
                return Ok(ChangeState(Some(packet)));
            }
        }

        let mut action = self.downcast().handle_packet(packet).await?;
        if let MaintainState(maybe_reply) = &mut action {
            if let Some(reply) = maybe_reply.take() {
                self.connection.send(reply).await?;
            }
        }
        Ok(action)
    }

    async fn handle_timeout_downcast(&self) -> Result<HandlePacketAction> {
        use HandlePacketAction::*;

        let mut action = self.downcast().handle_timeout().await?;
        if let MaintainState(maybe_reply) = &mut action {
            if let Some(reply) = maybe_reply.take() {
                self.connection.send(reply).await?;
            }
        }
        Ok(action)
    }

    async fn main(self: Arc<Self>, first_packet: Option<Packet>) -> Result<Option<Packet>> {
        use HandlePacketAction::*;

        info!(state = %self.state, term = %self.term.load(Ordering::Relaxed), "state change");

        let mut main_tasks = JoinSet::new();
        main_tasks.spawn(Arc::clone(&self).signal_handler());

        if let Some(packet) = first_packet {
            // The following code is duplicated from the loop. If it
            // were not, then a check for first_packet would have to
            // be run in every loop iteration, every time a packet
            // is received--an unnecessary performance penalty.
            let action = self.handle_packet_downcast(packet).await?;
            assert!(!matches!(action, MaintainState(Some(_))), "reply packet should have been taken and sent");
            if let ChangeState(maybe_packet) = action {
                main_tasks.shutdown().await;
                return Ok(maybe_packet);
            }
        }

        loop {
            let timeout = async {
                if let Some(t) = self.state.get_timeout() {
                    sleep_until(t).await
                } else {
                    // Disable the timeout arm of select by awaiting
                    // a future that never resolves
                    std::future::pending().await
                }
            };

            let action = tokio::select! {
                result = self.connection.receive() => self.handle_packet_downcast(result?).await?,
                _ = timeout => self.handle_timeout_downcast().await?,
            };
            assert!(!matches!(action, MaintainState(Some(_))), "reply packet should have been taken and sent");
            if let ChangeState(maybe_packet) = action {
                main_tasks.shutdown().await;
                return Ok(maybe_packet);
            }
        }
    }

    async fn signal_handler(self: Arc<Self>) -> Result<()> {
        use tokio::signal::unix::{signal, SignalKind};

        let mut usr1_stream = signal(SignalKind::user_defined1()).expect("signal handling failed");

        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        let mut stdout = tokio::io::stdout();

        loop {
            tokio::select! {
                _ = usr1_stream.recv() => {
                    info!(state = %self.state, "SIGUSR1");
                    debug!(journal = %self.journal, "SIGUSR1");
                },
                _ = status_interval.tick() => {
                    let term = self.term.load(Ordering::Relaxed);
                    let last_index = self.journal.last_index().saturating_sub(1);
                    let status_string = format!("\x1Bk{}[t{},i{}]\x1B", self.state, term, last_index);
                    // println!("\x1Bk{:?}\x1B", *server_state);
                    let _yeet = stdout.write_all(status_string.as_bytes()).await;
                    let _yeet = stdout.flush().await;
                }
            }
        }
    }
}
