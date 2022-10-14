mod follower;
mod candidate;
mod leader;
mod state;

use std::any::Any;
use std::{result::Result as StdResult, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::{task::JoinHandle, sync::{mpsc::{Receiver, Sender}, Mutex}, time::sleep_until};

use state::ServerState;
use tracing::info;

use crate::connection::{ConnectionError, Packet};

use self::state::{Follower, Candidate, Leader};

pub type Result<T> = std::result::Result<T, ServerError>;
pub type ServerHandle = JoinHandle<Result<()>>;

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error(transparent)]
    ConnectionFailed(#[from] ConnectionError),
    #[error(transparent)]
    TaskPanicked(#[from] tokio::task::JoinError),
}

#[derive(Debug)]
pub struct Server<S: ServerState> {
    connection_h: JoinHandle<Result<()>>,
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

impl ServerImpl<'_> {
    pub async fn handle_packet(&self, packet: Packet) -> Result<Option<Packet>> {
        match self {
            ServerImpl::Follower(f) => f.handle_packet(packet).await,
            ServerImpl::Candidate(c) => c.handle_packet(packet).await,
            ServerImpl::Leader(l) => l.handle_packet(packet).await,
        }
    }

    pub async fn handle_timeout(&self) -> Result<()> {
        match self {
            ServerImpl::Follower(f) => f.handle_timeout().await,
            ServerImpl::Candidate(c) => c.handle_timeout().await,
            ServerImpl::Leader(_) => unimplemented!("No Leader timeout"),
        }
    }
}

impl<S: ServerState> Server<S> {
    async fn update_term(&self, packet: &Packet) -> StdResult<u64, ()> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term > current_term {
            self.term.store(packet.term, Ordering::Release);
            Err(())
        } else {
            Ok(packet.term)
        }
    }

    fn in_state<T: ServerState>(&self) -> bool {
        (&self.state as &dyn Any).is::<T>()
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
            let state_type = std::any::type_name::<S>();
            unimplemented!("Invalid server state: {}", state_type);
        }
    }

    async fn incoming_loop(self: Arc<Self>) -> Result<()> {
        let timeout = self.state.get_timeout();
        let timeout = timeout.map(|t| sleep_until(t));
        let incoming = self.packets_in.lock().await;
        let outgoing = self.packets_out;

        loop {
            tokio::select! {
                Some(packet) = incoming.recv() => {
                    if let Err(_) = self.update_term(&packet).await {
                        // Term from packet was greater than our term,
                        // transition to Follower
                        if !self.in_state::<Follower>() {
                            // Follower should not return and progress
                            // to another state but remain a follower
                            // in the new term
                            return Ok(());
                        }
                    }

                    let reply = self.downcast().handle_packet(packet).await?;
                    if let Some(reply_packet) = reply {
                        outgoing.send(reply_packet).await
                            .map_err(ConnectionError::from)?;
                    }

                },
                _ = timeout.unwrap(), if timeout.is_some() => self.downcast().handle_timeout().await,
                else => return Ok(()),  // No more packets, shutting down
            }
        }
    }

    // async fn send(&self, packet: Packet) -> StdResult<(), ConnectionError> {
    //     self.packets_out.send(packet).await?;
    //     Ok(())
    // }

    // async fn recv(&self, packet: Packet) -> StdResult<Packet, ConnectionError> {
    //     let mut packets_in = self.packets_in.lock().await;
    //     let packet = packets_in.recv().await
    //         .ok_or(ConnectionError::SenderDropped)?;
    //     Ok(packet)
    // }
}
