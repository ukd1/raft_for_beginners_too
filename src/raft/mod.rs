mod follower;
mod candidate;
mod leader;
mod state;

use std::any::Any;
use std::{result::Result as StdResult, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

use rand::Rng;
use tokio::{task::JoinHandle, sync::{mpsc::{Receiver, Sender}, Mutex}, time::{Duration, Instant, sleep_until}};
use tracing::{trace, info};

use state::ServerState;

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
    #[error("Server shutting down")]
    ShuttingDown,
}

#[derive(Debug)]
pub struct Server<S: ServerState> {
    connection_h: ServerHandle,
    packets_in: Mutex<Receiver<Packet>>,
    packets_out: Sender<Packet>,
    config: crate::config::Config,
    pub state: S,
    pub term: AtomicU64,
}

enum ServerImpl<'s> {
    Follower(&'s Server<Follower>),
    Candidate(&'s Server<Candidate>),
    Leader(&'s Server<Leader>),
}

pub(crate) enum HandlePacketAction {
    MaintainState(Option<Packet>),
    ChangeState(Option<Packet>),
}

impl ServerImpl<'_> {
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

impl<S: ServerState> Server<S> {
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

    fn state_name(&self) -> &'static str {
        std::any::type_name::<S>()
    }

    fn in_state<T: ServerState>(&self) -> bool {
        let dyn_state = &self.state as &dyn Any;
        dyn_state.is::<T>()
    }

    fn downcast(&self) -> ServerImpl {
        let dyn_self = self as &dyn Any;
        if let Some(follower) = dyn_self.downcast_ref::<Server<Follower>>() {
            ServerImpl::Follower(follower)
        } else if let Some(candidate) = dyn_self.downcast_ref::<Server<Candidate>>() {
            ServerImpl::Candidate(candidate)
        } else if let Some(leader) = dyn_self.downcast_ref::<Server<Leader>>() {
            ServerImpl::Leader(leader)
        } else {
            unimplemented!("Invalid server state: {}", self.state_name());
        }
    }

    async fn connection_loop(connection: impl Connection, incoming: Sender<Packet>, mut outgoing: Receiver<Packet>) -> Result<()> {
        loop {
            tokio::select! {
                packet = connection.receive() => {
                    let packet = packet?;
                    trace!(?packet, "receive");
                    //incoming.try_send(packet).expect("TODO: ConnectionError");
                    let _ = incoming.send(packet).await; // DEBUG: try ignoring send errors
                },
                Some(packet) = outgoing.recv() => {
                    connection.send(packet).await?;
                },
            }
        }
    }

    async fn handle_packet_generic(&self, packet: Packet) -> Result<HandlePacketAction> {
        if let Err(_) = self.update_term(&packet).await {
            // Term from packet was greater than our term,
            // transition to Follower
            if !self.in_state::<Follower>() {
                // A new term should trigger a state transition for Leaders
                // and Candidates, but a Follower should remain a Follower
                // in the new term
                return Ok(HandlePacketAction::ChangeState(Some(packet)));
            }
        }

        self.downcast().handle_packet(packet).await
    }

    async fn incoming_loop(self: Arc<Self>, first_packet: Option<Packet>) -> Result<Option<Packet>> {
        use HandlePacketAction::*;

        let mut incoming = self.packets_in.lock().await;

        if let Some(packet) = first_packet {
            // The following code is duplicated from the loop. If it
            // were not, then a check for first_packet would have to
            // be run in every loop iteration, every time a packet
            // is received--an unnecessary performance penalty.
            let action = self.handle_packet_generic(packet).await?;
            match action {
                MaintainState(Some(reply)) => {
                    self.send(reply).await?
                },
                MaintainState(None) => {},
                ChangeState(maybe_packet) => return Ok(maybe_packet),
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
                Some(packet) = incoming.recv() => self.handle_packet_generic(packet).await?,
                _ = timeout => self.downcast().handle_timeout().await?,
                else => break Err(ServerError::ShuttingDown),  // No more packets, shutting down
            };
            match action {
                MaintainState(Some(reply)) => {
                    self.send(reply).await?
                },
                MaintainState(None) => {},
                ChangeState(maybe_packet) => break Ok(maybe_packet),
            }
        }
    }

    async fn send(&self, packet: Packet) -> StdResult<(), ConnectionError> {
        self.packets_out.send(packet).await?;
        Ok(())
    }
}
