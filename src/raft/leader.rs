use std::sync::{atomic::Ordering, Arc};

use tracing::info;

use crate::{connection::{PacketType, Packet, ConnectionError}, raft::HandlePacketAction};

use super::{Result, Server, state::{Leader, Follower, Candidate}, StateResult};

impl Server<Leader> {
    pub(super) async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        use PacketType::*;

        match packet.message_type {
            AppendEntriesAck { .. } => Ok(HandlePacketAction::MaintainState(None)), // TODO: commit in log
            // Leaders ignore these packets
            AppendEntries | VoteRequest { .. } | VoteResponse { .. } => Ok(HandlePacketAction::MaintainState(None)),
        }
    }

    pub(super) async fn run(self, next_packet: Option<Packet>) -> StateResult<Server<Follower>> {
        let current_term = self.term.load(Ordering::Acquire);
        let this = Arc::new(self);
        info!(term = %current_term, "Leader started");

        let heartbeat_handle = tokio::spawn(Arc::clone(&this).heartbeat_loop());
        let incoming_loop_result = tokio::spawn(Arc::clone(&this).incoming_loop(next_packet)).await;
        // 1. Shut down heartbeat_loop as soon as incoming_loop is done
        heartbeat_handle.abort();
        // 2. Raise any error from the incoming loop
        let packet_for_next_state = incoming_loop_result??;
        // 3. incoming_loop exited without error, so wait for
        //    heartbeat_loop to exit and check for errors
        match heartbeat_handle.await {
            Err(join_err) if !join_err.is_cancelled() => Err(join_err)?,
            Ok(Err(heartbeat_err)) => Err(heartbeat_err)?,
            _ => {}, // Either exited normally or was cancelled
        }
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((this.into(), packet_for_next_state))
    }

    async fn heartbeat_loop(self: Arc<Self>) -> Result<()> {
        let heartbeat_interval = self.config.heartbeat_interval;
        let mut ticker = tokio::time::interval(heartbeat_interval);
        loop {
            ticker.tick().await;
            for peer in self.config.peers.iter() {
                let peer_request = Packet {
                    message_type: PacketType::AppendEntries,
                    peer: peer.to_owned(),
                    term: self.term.load(Ordering::Acquire),
                };
                self.packets_out.send(peer_request).await
                    .map_err(ConnectionError::from)?;
            }
        }
    }
}

impl From<Server<Candidate>> for Server<Leader> {
    fn from(candidate: Server<Candidate>) -> Self {
        Self {
            connection_h: candidate.connection_h,
            packets_in: candidate.packets_in,
            packets_out: candidate.packets_out,
            config: candidate.config,
            term: candidate.term,
            state: Leader {},
        }
    }
}