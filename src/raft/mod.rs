mod follower;
mod candidate;
mod leader;

use std::any::Any;
use std::future::Future;
use std::task::Poll;
use std::{result::Result as StdResult, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

use pin_project::pin_project;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, watch};
use tokio::sync::{oneshot, mpsc};
use tokio::task::JoinSet;
use tokio::{task::JoinHandle, time::{Duration, Instant, sleep_until}};
use tracing::{info, debug, warn, info_span, Instrument, error};

use crate::journal::{Journal, JournalValue};
use crate::connection::{ConnectionError, Packet, Connection};

use self::state::{ServerState, Follower, Candidate, Leader};

pub type Result<T> = std::result::Result<T, ServerError>;
type StateResult<T, V> = Result<(T, Option<Packet<V>>)>;
type ClientRequest<V> = (V, oneshot::Sender<Result<()>>);

mod state {
    use std::{fmt::{Debug, Display}, any::Any};

    use tokio::time::Instant;

    pub use super::{
        follower::Follower,
        candidate::Candidate,
        leader::Leader,
    };

    pub trait ServerState: Debug + Display + Any + Send + Sync {
        fn get_timeout(&self) -> Option<Instant>;
        fn set_timeout(&self, timeout: Instant);
    }
}

#[pin_project]
pub struct ServerHandle<V> {
    #[pin]
    inner: Option<JoinHandle<Result<()>>>,
    requests: mpsc::Sender<ClientRequest<V>>,
    state: watch::Receiver<()>, // TODO: enum for state
}

impl<V> ServerHandle<V> {
    fn new(inner: JoinHandle<Result<()>>, requests: mpsc::Sender<ClientRequest<V>>, state: watch::Receiver<()>) -> Self {
        Self { inner: Some(inner), requests, state }
    }

    pub async fn send(&self, value: V) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.requests.send((value, response_tx)).await
            .or(Err(ServerError::RequestFailed))?;
        response_rx.await
            .or(Err(ServerError::RequestFailed))?
    }

    pub async fn state_change(&self) {
        let mut state = self.state.clone();
        state.changed().await
            .expect("state sender dropped")
    }
}

impl<V> Future for ServerHandle<V> {
    type Output = <JoinHandle<Result<()>> as Future>::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match self.project().inner.as_pin_mut() {
            Some(f) => f.poll(cx),
            None => Poll::Ready(Ok(Err(ServerError::HandleCloned))),
        }
    }
}

impl<V> Clone for ServerHandle<V> {
    fn clone(&self) -> Self {
        Self {
            inner: None,
            requests: self.requests.clone(),
            state: self.state.clone(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error(transparent)]
    ConnectionFailed(#[from] ConnectionError),
    #[error(transparent)]
    TaskPanicked(#[from] tokio::task::JoinError),
    #[error("could not convert native integer to wire size")]
    IntegerOverflow(#[from] std::num::TryFromIntError),
    #[error("client request failed")]
    RequestFailed,
    #[error("client request timed out")]
    RequestTimeout(#[from] tokio::time::error::Elapsed),
    #[error("not cluster leader, can't accept requests")] // TODO: forward/redirect
    NotLeader,
    #[error("ServerHandle cloned: can only await first handle")]
    HandleCloned,
}

#[derive(Debug)]
pub struct Server<S, C, V>
where
    S: ServerState,
    C: Connection<V>,
    V: JournalValue,
{
    connection: C,
    requests: Mutex<mpsc::Receiver<ClientRequest<V>>>,
    config: crate::config::Config,
    journal: Journal<V>,
    pub state: S,
    state_tx: watch::Sender<()>,
    pub term: AtomicU64,
}

enum ServerImpl<'s, C, V>
where
    C: Connection<V>,
    V: JournalValue,
{
    Follower(&'s Server<Follower, C, V>),
    Candidate(&'s Server<Candidate, C, V>),
    Leader(&'s Server<Leader, C, V>),
}

pub(crate) enum HandlePacketAction<V: JournalValue> {
    MaintainState(Option<Packet<V>>),
    ChangeState(Option<Packet<V>>),
}

impl<C, V> ServerImpl<'_, C, V>
where
    C: Connection<V>,
    V: JournalValue,
{
    pub async fn handle_packet(&self, packet: Packet<V>) -> Result<HandlePacketAction<V>> {
        match self {
            ServerImpl::Follower(f) => f.handle_packet(packet).await,
            ServerImpl::Candidate(c) => c.handle_packet(packet).await,
            ServerImpl::Leader(l) => l.handle_packet(packet).await,
        }
    }

    pub async fn handle_timeout(&self) -> Result<HandlePacketAction<V>> {
        match self {
            ServerImpl::Follower(f) => f.handle_timeout().await,
            ServerImpl::Candidate(c) => c.handle_timeout().await,
            ServerImpl::Leader(_) => unimplemented!("no Leader timeout"),
        }
    }

    pub async fn handle_clientrequest(&self, value: V) -> Result<()> {
        match self {
            ServerImpl::Follower(_) => Err(ServerError::NotLeader), // TODO: request forwarding or redirecting
            ServerImpl::Candidate(_) => Err(ServerError::NotLeader), // TODO: request forwarding or redirecting
            ServerImpl::Leader(l) => l.handle_clientrequest(value).await,
        }
    }
}

impl<S, C, V> Server<S, C, V>
where
    S: ServerState,
    C: Connection<V>,
    V: JournalValue,
{
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
   
    async fn update_term(&self, packet: &Packet<V>) -> StdResult<u64, u64> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term > current_term {
            info!(%packet.term, "newer term in packet");
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

    fn quorum(&self) -> usize {
        // len+1 because quorum counts this node
        // and final +1 to break a tie; comparisons
        // should be count >= quorum.
        let node_cnt = self.config.peers.len() + 1;
        node_cnt / 2 + 1
    }

    fn downcast(&self) -> ServerImpl<C, V> {
        let dyn_self = self as &dyn Any;
        if let Some(follower) = dyn_self.downcast_ref::<Server<Follower, C, V>>() {
            ServerImpl::Follower(follower)
        } else if let Some(candidate) = dyn_self.downcast_ref::<Server<Candidate, C, V>>() {
            ServerImpl::Candidate(candidate)
        } else if let Some(leader) = dyn_self.downcast_ref::<Server<Leader, C, V>>() {
            ServerImpl::Leader(leader)
        } else {
            unimplemented!("Invalid server state: {}", self.state);
        }
    }

    #[tracing::instrument(
        name = "handle_request",
        skip_all,
        fields(
            state = %self.state,
        ),
    )]

    async fn handle_request_downcast(&self, value: V) -> Result<()> {
        self.downcast().handle_clientrequest(value).await
    }

    #[tracing::instrument(
        name = "handle_packet",
        skip_all,
        fields(
            term = %self.term.load(Ordering::Relaxed),
            state = %self.state,
        ),
    )]
    async fn handle_packet_downcast(&self, packet: Packet<V>) -> Result<HandlePacketAction<V>> {
        use HandlePacketAction::*;

        if self.update_term(&packet).await.is_err() {
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

    #[tracing::instrument(
        name = "handle_timeout",
        skip_all,
        fields(
            term = %self.term.load(Ordering::Relaxed),
            state = %self.state,
        ),
    )]
    async fn handle_timeout_downcast(&self) -> Result<HandlePacketAction<V>> {
        use HandlePacketAction::*;

        let mut action = self.downcast().handle_timeout().await?;
        if let MaintainState(maybe_reply) = &mut action {
            if let Some(reply) = maybe_reply.take() {
                self.connection.send(reply).await?;
            }
        }
        Ok(action)
    }

    async fn main(self: Arc<Self>, first_packet: Option<Packet<V>>) -> Result<Option<Packet<V>>> {
        use HandlePacketAction::*;

        let current_term = self.term.load(Ordering::Relaxed);
        let startup_span = info_span!("startup", term = %current_term, state = %self.state);
        if current_term > 0 {
            startup_span.in_scope(|| warn!("state change"));
            self.state_tx.send(())
                .expect("state change notification failed");
        }

        let mut main_tasks = JoinSet::new();
        main_tasks.spawn(Arc::clone(&self).signal_handler());

        if let Some(packet) = first_packet {
            // The following code is duplicated from the loop. If it
            // were not, then a check for first_packet would have to
            // be run in every loop iteration, every time a packet
            // is received--an unnecessary performance penalty.
            let action = self.handle_packet_downcast(packet).instrument(startup_span).await?;
            assert!(!matches!(action, MaintainState(Some(_))), "reply packet should have been taken and sent");
            if let ChangeState(maybe_packet) = action {
                main_tasks.shutdown().await;
                return Ok(maybe_packet);
            }
        }

        let mut client_requests = self.requests.lock().await;
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
                Some((request, response_h)) = client_requests.recv() => {
                    let this = Arc::downgrade(&self);
                    tokio::spawn(async move {
                        match this.upgrade() {
                            Some(this) => {
                                let response = this.handle_request_downcast(request).await
                                    .map(|_| ());
                                let result = response_h.send(response);
                                if result.is_err() {
                                    error!("sending client response failed");
                                }
                            },
                            None => warn!("server exited or changed state before client request completed"),
                        }
                    });
                    HandlePacketAction::MaintainState(None)
                },
                _ = timeout => self.handle_timeout_downcast().await?,
            };
            assert!(!matches!(action, MaintainState(Some(_))), "reply packet should have been taken and sent");
            if let ChangeState(maybe_packet) = action {
                main_tasks.shutdown().await;
                return Ok(maybe_packet);
            }
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            term = %self.term.load(Ordering::Relaxed),
            state = %self.state,
        ),
    )]
    async fn signal_handler(self: Arc<Self>) -> Result<()> {
        use tokio::signal::unix::{signal, SignalKind};

        let mut usr1_stream = signal(SignalKind::user_defined1()).expect("signal handling failed");

        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        let mut stdout = tokio::io::stdout();

        loop {
            tokio::select! {
                _ = usr1_stream.recv() => {
                    info!("SIGUSR1");
                    debug!(target: concat!(env!("CARGO_PKG_NAME"), "::journal"), journal = %self.journal, "SIGUSR1");
                },
                _ = status_interval.tick() => {
                    let term = self.term.load(Ordering::Relaxed);
                    let last_index = self.journal.last_index().map(|i| i.to_string()).unwrap_or_else(|| "X".to_string());
                    let commit_index = self.journal.commit_index();
                    let status_string = format!("\x1Bk{}[t{},i{},c{}]\x1B", self.state, term, last_index, commit_index);
                    let _yeet = stdout.write_all(status_string.as_bytes()).await;
                    let _yeet = stdout.flush().await;
                }
            }
        }
    }
}
