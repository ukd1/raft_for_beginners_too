use std::sync::{atomic::Ordering, Arc};

use tracing::{warn, info, debug};
use crate::{raft::state::Follower, connection::{Packet, PacketType, ConnectionError, Connection}};
use super::{Result, Server, state::{Candidate, ElectionResult}, HandlePacketAction, StateResult};

impl<C: Connection> Server<Candidate, C> {
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction> {
        info!("Candidate timeout");
        // Restart election and maintain state on timeout
        self.start_election().await?;
        Ok(HandlePacketAction::MaintainState(None))
    }

    pub(super) async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => Ok(HandlePacketAction::MaintainState(Some(Packet {
                message_type: PacketType::VoteResponse { is_granted: false },
                peer: packet.peer,
                term: packet.term,
            }))),
            VoteResponse { .. } => self.handle_voteresponse(&packet).await,
            // Candidates ignore these packets
            AppendEntries | AppendEntriesAck { .. } => Ok(HandlePacketAction::MaintainState(None)),
        }
    }

    async fn handle_voteresponse(&self, packet: &Packet) -> Result<HandlePacketAction> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term != current_term {
            warn!(peer = ?packet.peer, term = ?packet.term, "got a vote response for the wrong term");
            return Ok(HandlePacketAction::MaintainState(None));
        }

        let is_granted = if let Packet { message_type: PacketType::VoteResponse { is_granted }, .. } = packet {
            *is_granted
        } else {
            unreachable!("handle_voteresponse called with a non-VoteResponse packet");
        };
        info!(peer = ?packet.peer, term = ?packet.term, is_granted, "got a vote response");
        self.state.votes.record_vote(&packet.peer, is_granted);

        if self.has_won_election() {
            info!(
                term = ?self.term.load(Ordering::Acquire),
                vote_cnt = %(self.state.votes.vote_count() + 1),
                node_cnt = %(self.config.peers.len() + 1),
                "won election; becoming Leader"
            );
            Ok(HandlePacketAction::ChangeState(None))
        } else {
            Ok(HandlePacketAction::MaintainState(None))
        }
    }

    fn has_won_election(&self) -> bool {
        // count votes and nodes
        // add 1 to each so we count ourselves
        let vote_cnt = self.state.votes.vote_count() + 1;
        let node_cnt = self.config.peers.len() + 1;
        debug!(?vote_cnt, ?node_cnt, "checking vote count");

        // did we get more than half the votes, including our own?
        vote_cnt > (node_cnt / 2)
    }

    async fn start_election(&self) -> Result<()> {
        let current_term = self.term.fetch_add(1, Ordering::Release) + 1;
        self.reset_term_timeout().await;
        info!(term = %current_term, "Candidate started");

        for peer in self.config.peers.iter() {
            let peer_request = Packet {
                message_type: PacketType::VoteRequest { last_log_index: 0, last_log_term: current_term - 1 }, // TODO: THIS IS THE WRONG TERM, it should come from the log and doesn't need the -1
                term: current_term,
                peer: peer.to_owned(),
            };
            self.connection.send(peer_request).await
                .map_err(ConnectionError::from)?;
        }

        Ok(())
    }

    pub(super) async fn run(self, next_packet: Option<Packet>) -> StateResult<ElectionResult<C>> {
        let this = Arc::new(self);
        let packet_for_next_state = {
            // Loop on incoming packets until a successful exit, and...
            let loop_h = tokio::spawn(Arc::clone(&this).incoming_loop(next_packet));
            // ...send a voterequest packet to all peers, then...
            let this_election = Arc::clone(&this);
            tokio::spawn(async move { this_election.start_election().await }).await??; // TODO: add timeout
            // ...await task results.
            loop_h.await??
        };
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        let next_state = if this.has_won_election() {
            ElectionResult::Leader(this.into())
        } else {
            ElectionResult::Follower(this.into())
        };
        Ok((next_state, packet_for_next_state))
    }
}

impl<C: Connection> From<Server<Follower, C>> for Server<Candidate, C> {
    fn from(follower: Server<Follower, C>) -> Self {
        Self {
            connection: follower.connection,
            config: follower.config,
            term: follower.term,
            state: Candidate::from(follower.state),
        }
    }
}

