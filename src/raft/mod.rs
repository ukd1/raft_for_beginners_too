mod candidate;
mod follower;
mod leader;
mod state;

use std::any::Any;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::Poll;
use std::{result::Result as StdResult, sync::Arc};

use pin_project::pin_project;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio::{
    task::JoinHandle,
    time::{sleep_until, Duration, Instant},
};
use tracing::{debug, error, info, info_span, warn, Instrument};

use self::state::{Candidate, CurrentState, Follower, Leader, ServerState};
use crate::connection::{Connection, ConnectionError, Packet, ServerAddress};
use crate::journal::{ApplyResult, Journal, Journalable};

pub type Term = u32;
pub type AtomicTerm = AtomicU32;
pub type Result<T, V> = std::result::Result<T, ServerError<V>>;
type StateResult<T, D, V> = Result<(T, Option<Packet<D, V>>), V>;
type ClientResultSender<V, R> = oneshot::Sender<Result<R, V>>;
type ClientRequest<V, R> = (V, ClientResultSender<V, R>);

#[pin_project]
pub struct ServerHandle<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    #[pin]
    inner: Option<JoinHandle<Result<(), V>>>,
    requests: mpsc::Sender<ClientRequest<V, R>>,
    state: watch::Receiver<CurrentState>,
    timeout: Duration,
}

impl<V, R> ServerHandle<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    fn new(
        inner: JoinHandle<Result<(), V>>,
        requests: mpsc::Sender<ClientRequest<V, R>>,
        state: watch::Receiver<CurrentState>,
        timeout: Duration,
    ) -> Self {
        Self {
            inner: Some(inner),
            requests,
            state,
            timeout,
        }
    }

    pub async fn send(&self, value: V) -> Result<R, V> {
        let (result_tx, result_rx) = oneshot::channel();
        self.requests
            .send((value, result_tx))
            .await
            .or(Err(ServerError::RequestFailed))?;
        let result = timeout(self.timeout, result_rx)
            .await?
            .or(Err(ServerError::RequestFailed))?;
        result.map_err(|e| ServerError::RequestError(Box::new(e)))
    }

    pub async fn state_change(&self) -> CurrentState {
        let mut state_rx = self.state.clone();
        state_rx.changed().await.expect("state sender dropped");
        let state = *state_rx.borrow();
        state
    }
}

impl<V, R> Future for ServerHandle<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    type Output = <JoinHandle<Result<(), V>> as Future>::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.project().inner.as_pin_mut() {
            Some(f) => f.poll(cx),
            None => Poll::Ready(Ok(Err(ServerError::HandleCloned))),
        }
    }
}

impl<V, R> Clone for ServerHandle<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    fn clone(&self) -> Self {
        Self {
            inner: None,
            requests: self.requests.clone(),
            state: self.state.clone(),
            timeout: self.timeout,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError<V: Journalable> {
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
    #[error(transparent)]
    RequestError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("not cluster leader, can't accept requests")]
    NotLeader { leader: ServerAddress, value: V },
    #[error("election in progress; try request again")]
    Unavailable(V),
    #[error("ServerHandle cloned: can only await first handle")]
    HandleCloned,
}

#[derive(Debug)]
pub struct Server<S, C, J>
where
    S: ServerState,
    J: Journal,
    C: Connection<J::Snapshot, J::Value>,
{
    connection: C,
    requests: Mutex<mpsc::Receiver<ClientRequest<J::Value, J::Applied>>>,
    config: crate::config::Config,
    journal: J,
    pub state: S,
    state_tx: watch::Sender<CurrentState>,
    pub term: AtomicTerm,
}

enum ServerImpl<'s, C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    Follower(&'s Server<Follower, C, J>),
    Candidate(&'s Server<Candidate, C, J>),
    Leader(&'s Server<Leader<J::Value, J::Applied>, C, J>),
}

pub(crate) enum HandlePacketAction<D, V>
where
    D: Journalable,
    V: Journalable,
{
    MaintainState(Option<Packet<D, V>>),
    ChangeState(Option<Packet<D, V>>),
}

impl<C, J> ServerImpl<'_, C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    pub async fn handle_packet(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        match self {
            ServerImpl::Follower(f) => f.handle_packet(packet).await,
            ServerImpl::Candidate(c) => c.handle_packet(packet).await,
            ServerImpl::Leader(l) => l.handle_packet(packet).await,
        }
    }

    pub async fn handle_timeout(
        &self,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        match self {
            ServerImpl::Follower(f) => f.handle_timeout().await,
            ServerImpl::Candidate(c) => c.handle_timeout().await,
            ServerImpl::Leader(_) => unimplemented!("no Leader timeout"),
        }
    }

    pub async fn handle_clientrequest(
        &self,
        value: J::Value,
        result_tx: ClientResultSender<J::Value, J::Applied>,
    ) {
        let send_result = match self {
            ServerImpl::Follower(f) => result_tx.send(Err(
                match &*f.state.leader.lock().expect("leader lock poisoned") {
                    Some(l) => ServerError::NotLeader {
                        leader: l.clone(),
                        value,
                    },
                    None => ServerError::Unavailable(value),
                },
            )),
            ServerImpl::Candidate(_) => result_tx.send(Err(ServerError::Unavailable(value))),
            // TODO: remove allow when returning a result value in the future
            #[allow(clippy::unit_arg)]
            ServerImpl::Leader(l) => Ok(l.handle_clientrequest(value, result_tx).await),
        };
        if let Err(Err(e)) = send_result {
            error!(error = %e, "could not send client error");
        }
    }

    pub async fn update_leader(&self, leader: &ServerAddress) {
        match self {
            ServerImpl::Follower(f) => {
                *f.state.leader.lock().expect("leader lock poisoned") = Some(leader.clone())
            }
            _ => unimplemented!("Leader and Candidate do not track current leader"),
        }
    }
}

impl<S, C, J> Server<S, C, J>
where
    S: ServerState,
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
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
        let new_timeout = Self::generate_random_timeout(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
        self.state.set_timeout(new_timeout);
    }

    async fn update_term(&self, packet: &Packet<J::Snapshot, J::Value>) -> StdResult<Term, Term> {
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

    fn downcast(&self) -> ServerImpl<C, J> {
        let dyn_self = self as &dyn Any;
        if let Some(follower) = dyn_self.downcast_ref::<Server<Follower, C, J>>() {
            ServerImpl::Follower(follower)
        } else if let Some(candidate) = dyn_self.downcast_ref::<Server<Candidate, C, J>>() {
            ServerImpl::Candidate(candidate)
        } else if let Some(leader) =
            dyn_self.downcast_ref::<Server<Leader<J::Value, J::Applied>, C, J>>()
        {
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

    async fn handle_request_downcast(
        &self,
        value: J::Value,
        result_tx: ClientResultSender<J::Value, J::Applied>,
    ) {
        self.downcast().handle_clientrequest(value, result_tx).await;
    }

    #[tracing::instrument(
        name = "handle_packet",
        skip_all,
        fields(
            term = %self.term.load(Ordering::Relaxed),
            state = %self.state,
        ),
    )]
    async fn handle_packet_downcast(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        use HandlePacketAction::*;

        if self.update_term(&packet).await.is_err() {
            // Term from packet was greater than our term,
            // transition to Follower
            if self.in_state::<Follower>() {
                // Update the Follower's leader value
                self.downcast().update_leader(&packet.peer).await;
            } else {
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
    async fn handle_timeout_downcast(
        &self,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        use HandlePacketAction::*;

        let mut action = self.downcast().handle_timeout().await?;
        if let MaintainState(maybe_reply) = &mut action {
            if let Some(reply) = maybe_reply.take() {
                self.connection.send(reply).await?;
            }
        }
        Ok(action)
    }

    async fn main(
        self: Arc<Self>,
        first_packet: Option<Packet<J::Snapshot, J::Value>>,
    ) -> Result<Option<Packet<J::Snapshot, J::Value>>, J::Value> {
        use HandlePacketAction::*;

        let current_term = self.term.load(Ordering::Relaxed);
        let startup_span = info_span!("startup", term = %current_term, state = %self.state);
        if current_term > 0 {
            startup_span.in_scope(|| warn!("state change"));
            self.state_tx
                .send(self.downcast().into())
                .expect("state change notification failed");
        }

        let mut main_tasks = JoinSet::new();
        main_tasks.spawn(Arc::clone(&self).signal_handler());

        if let Some(packet) = first_packet {
            // The following code is duplicated from the loop. If it
            // were not, then a check for first_packet would have to
            // be run in every loop iteration, every time a packet
            // is received--an unnecessary performance penalty.
            let action = self
                .handle_packet_downcast(packet)
                .instrument(startup_span)
                .await?;
            assert!(
                !matches!(action, MaintainState(Some(_))),
                "reply packet should have been taken and sent"
            );
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
                Some((request, result_tx)) = client_requests.recv() => {
                    self.handle_request_downcast(request, result_tx).await;
                    HandlePacketAction::MaintainState(None)
                },
                _ = timeout => self.handle_timeout_downcast().await?,
            };
            assert!(
                !matches!(action, MaintainState(Some(_))),
                "reply packet should have been taken and sent"
            );
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
    async fn signal_handler(self: Arc<Self>) -> Result<(), J::Value> {
        use tokio::signal::unix::{signal, SignalKind};

        let mut usr1_stream = signal(SignalKind::user_defined1()).expect("signal handling failed");

        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        let mut stdout = tokio::io::stdout();

        loop {
            tokio::select! {
                _ = usr1_stream.recv() => {
                    let snapshot = self.journal.snapshot_without_commit().await; // DEBUG
                    info!(?snapshot, "SIGUSR1");
                    debug!(target: concat!(env!("CARGO_PKG_NAME"), "::journal"), journal = %self.journal, "SIGUSR1");
                },
                _ = status_interval.tick() => {
                    let term = self.term.load(Ordering::Relaxed);
                    let last_index = self.journal.last_index().await.map(|i| i.to_string()).unwrap_or_else(|| "X".to_string());
                    let commit_index = self.journal.commit_index().await.map(|i| i.to_string()).unwrap_or_else(|| "X".to_string());
                    let status_string = format!("\x1Bk{}[t{},i{},c{}]\x1B", self.state, term, last_index, commit_index);
                    let _yeet = stdout.write_all(status_string.as_bytes()).await;
                    let _yeet = stdout.flush().await;
                }
            }
        }
    }
}
