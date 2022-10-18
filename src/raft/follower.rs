use std::sync::{Arc, atomic::Ordering};

use tracing::{info, warn};

use crate::connection::{Connection, Packet, PacketType};

use super::{Result, Server, state::{Follower, ElectionResult, Candidate, Leader}, HandlePacketAction, StateResult, ServerHandle};

impl<C: Connection> Server<Follower, C> {
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction> {
        warn!(term = %self.term.load(Ordering::Relaxed), "Follower timeout");
        // Advance to Candidate state on timeout
        Ok(HandlePacketAction::ChangeState(None))
    }

    pub(super) async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => self.handle_voterequest(&packet).await,
            AppendEntries => self.handle_appendentries(&packet).await,
            // Followers ignore these packets
            AppendEntriesAck { .. } | VoteResponse { .. } => Ok(HandlePacketAction::MaintainState(None)),
        }
    }

    async fn handle_voterequest(&self, packet: &Packet) -> Result<HandlePacketAction> {
        let current_term = self.term.load(Ordering::Acquire);
        let vote_granted = if packet.term == current_term {
            self.reset_term_timeout().await;
            let mut last_ballot = self.state.voted_for.lock().expect("voted_for Mutex poisoned");
            last_ballot.cast_vote(packet.term, &packet.peer)
        } else {
            // Packet term is too old
            false
        };
        
        let reply = Packet {
            message_type: PacketType::VoteResponse { is_granted: vote_granted },
            term: current_term,
            peer: packet.peer.clone(),
        };
        info!(candidate = ?reply.peer, term = ?reply.term, ?vote_granted, "casting vote");
        Ok(HandlePacketAction::MaintainState(Some(reply)))
    }

    async fn handle_appendentries(&self, packet: &Packet) -> Result<HandlePacketAction> {
        let current_term = self.term.load(Ordering::Acquire);
        let ack = packet.term == current_term;

        if ack {
            self.reset_term_timeout().await;
        }

        if ack {
            // TODO: append to log
        }

        let reply = Packet {
            message_type: PacketType::AppendEntriesAck { did_append: ack },
            term: current_term,
            peer: packet.peer.clone(),
        };
        Ok(HandlePacketAction::MaintainState(Some(reply)))
    }

    async fn run(self, handoff_packet: Option<Packet>) -> StateResult<Server<Candidate, C>> {
        let this = Arc::new(self);
        // Loop on incoming packets until a successful exit,
        // propagating any errors
        let packet_for_candidate = tokio::spawn(Arc::clone(&this).main(handoff_packet)).await??;
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((Server::<Candidate, C>::from(this), packet_for_candidate))
    }

    pub fn start(connection: C, config: crate::config::Config) -> ServerHandle {
        let timeout = Self::generate_random_timeout(config.election_timeout_min, config.election_timeout_max);
        tokio::spawn(async move {
            let mut follower = Self {
                connection,
                config,
                term: 0.into(),
                span: tracing::Span::none().into(),
                state: Follower::new(timeout),
            };
            let mut packet = None;
            let mut candidate;
            loop {
                (candidate, packet) = follower.run(packet).await?;
                (follower, packet) = match candidate.run(packet).await? {
                    (ElectionResult::Leader(leader), packet) => leader.run(packet).await?,
                    (ElectionResult::Follower(follower), packet) => (follower, packet),
                };
            }
        })
    }
}

impl<C: Connection> From<Server<Candidate, C>> for Server<Follower, C> {
    fn from(candidate: Server<Candidate, C>) -> Self {
        let timeout = Self::generate_random_timeout(candidate.config.election_timeout_min, candidate.config.election_timeout_max);
        Self {
            connection: candidate.connection,
            config: candidate.config,
            term: candidate.term,
            span: tracing::Span::none().into(),
            state: Follower::new(timeout),
        }
    }
}

impl<C: Connection> From<Server<Leader, C>> for Server<Follower, C> {
    fn from(leader: Server<Leader, C>) -> Self {
        let timeout = Self::generate_random_timeout(leader.config.election_timeout_min, leader.config.election_timeout_max);
        Self {
            connection: leader.connection,
            config: leader.config,
            term: leader.term,
            span: tracing::Span::none().into(),
            state: Follower::new(timeout),
        }
    }
}