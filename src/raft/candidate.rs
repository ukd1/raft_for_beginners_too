use std::sync::{atomic::Ordering, Arc};

use tracing::{warn, info};
use crate::{raft::state::Follower, connection::{Packet, PacketType, ConnectionError}};
use super::{Result, Server, state::{Candidate, ElectionResult}, HandlePacketAction};

impl Server<Candidate> {
    pub(super) async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => Ok(HandlePacketAction::Reply(Packet {
                message_type: crate::connection::PacketType::VoteResponse { is_granted: false },
                peer: packet.peer,
                term: packet.term,
            })),
            VoteResponse { .. } => self.handle_voteresponse(&packet).await,
            // Candidates ignore these packets
            AppendEntries | AppendEntriesAck { .. } => Ok(HandlePacketAction::NoReply),
        }
    }

    async fn handle_voteresponse(&self, packet: &Packet) -> Result<HandlePacketAction> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term != current_term {
            warn!(peer = ?packet.peer, term = ?packet.term, "got a vote response for the wrong term");
            return Ok(HandlePacketAction::NoReply);
        }

        // let is_granted = if let Packet { message_type: VoteResponse { is_granted }, .. } = packet { *is_granted } else { false };
        let Packet { message_type: PacketType::VoteResponse { is_granted }, .. } = *packet;
        info!(peer = ?packet.peer, term = ?packet.term, is_granted, "got a vote response");
        self.state.votes.record_vote(&packet.peer, is_granted);

        if self.has_won_election() {
            info!(term = ?self.term.load(Ordering::Acquire), "won election; becoming Leader");
            Ok(HandlePacketAction::StateTransition)
        } else {
            Ok(HandlePacketAction::NoReply)
        }
    }

    fn has_won_election(&self) -> bool {
        // count votes and nodes
        // add 1 to each so we count ourselves
        let vote_cnt = self.state.votes.vote_count();
        let node_cnt = self.config.peers.len() + 1;
        info!(?vote_cnt, ?node_cnt, "vote count, node count");

        // did we get more than half the votes, including our own?
        vote_cnt > (node_cnt / 2)
    }

    async fn send_voterequest(self: Arc<Self>) -> Result<()> {
        let current_term = self.term.load(Ordering::Acquire);

        for peer in self.config.peers.iter() {
            let current_term = self.term.load(Ordering::Acquire);
            let peer_request = Packet {
                message_type: PacketType::VoteRequest { last_log_index: 0, last_log_term: current_term - 1 }, // TODO: THIS IS THE WRONG TERM, it should come from the log and doesn't need the -1
                term: current_term,
                peer: peer.to_owned(),
            };
            self.packets_out.send(peer_request).await
                .map_err(ConnectionError::from)?;
        }

        Ok(())
    }

    pub(super) async fn start_election(self) -> Result<ElectionResult> {
        self.term.fetch_add(1, Ordering::Release);
        println!("Candidate started");
        let this = Arc::new(self);
        {
            let mut tasks = this.tasks.lock().await;
            // Loop on incoming packets until a successful exit, and...
            tasks.spawn(Arc::clone(&this).incoming_loop());
            // ...send a voterequest packet to all peers, then...
            tasks.spawn(Arc::clone(&this).send_voterequest());
            // ...await task results. TODO: add timeout
            while tasks.len() > 1 {
                tasks
                    .join_next()
                    .await
                    .expect("tasks should not be empty")??;
            }
            assert_eq!(tasks.len(), 1, "Only connect task should remain");
        }
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        let next_state = if this.has_won_election() {
            ElectionResult::Leader(this.into())
        } else {
            ElectionResult::Follower(this.into())
        };
        Ok(next_state)
    }
}

impl From<Server<Follower>> for Server<Candidate> {
    fn from(follower: Server<Follower>) -> Self {
        Self {
            tasks: follower.tasks,
            packets_in: follower.packets_in,
            packets_out: follower.packets_out,
            config: follower.config,
            term: follower.term,
            state: Candidate::from(follower.state),
        }
    }
}
