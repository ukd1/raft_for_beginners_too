mod follower;
mod candidate;
mod leader;
mod state;

use std::any::{Any, TypeId};
use std::{result::Result as StdResult, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

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

    async fn incoming_loop(self: Arc<Self>) -> Result<()> {
        let timeout = self.state.get_timeout();
        let timeout = timeout.map(|t| sleep_until(t));
        let incoming = self.packets_in.lock().await;
        let outgoing = self.packets_out;

        loop {
            tokio::select! {
                Some(packet) = incoming.recv() => {
                    use crate::connection::PacketType::*;

                    if let Err(_) = self.update_term(&packet).await {
                        // Term from packet was greater than our term,
                        // transition to follower
                        if self.state.type_id() != TypeId::of::<Follower>() {
                            return Ok(()); // TODO: Do this only for Candidate & Leader
                        }
                    }

                    match packet.message_type {
                        VoteRequest { .. } => {
                            let reply = self.handle_voterequest(&packet).await;
                            outgoing.send(reply).await.unwrap(); // TODO
                        },
                        VoteResponse { .. } => {
                            self.handle_voteresponse(&packet).await;
                        },
                        AppendEntries => {
                            if let Some(reply) = self.handle_appendentries(&packet).await {
                                outgoing.send(reply).await.unwrap(); // TODO
                            }
                        },
                        AppendEntriesAck { .. } => {
                            // TODO: commit in log
                        },
                    }
                },
                _ = timeout.unwrap(), if timeout.is_some() => {
                    info!("Election timeout; restarting election");
                    self.start_election().await
                },
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
