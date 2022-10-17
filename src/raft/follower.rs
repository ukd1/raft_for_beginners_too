use std::sync::{Arc, atomic::Ordering};

use tracing::info;

use crate::connection::{Connection, Packet, PacketType};

use super::{Result, Server, state::{Follower, ElectionResult, Candidate, Leader}, ServerHandle, HandlePacketAction, StateResult};

impl Server<Follower> {
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction> {
        info!("Follower timeout");
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

    async fn run(self, handoff_packet: Option<Packet>) -> StateResult<Server<Candidate>> {
        let current_term = self.term.load(Ordering::Acquire);
        info!(term = %current_term, "Follower started");
        let this = Arc::new(self);
        // Loop on incoming packets until a successful exit,
        // propagating any errors
        let packet_for_candidate = tokio::spawn(Arc::clone(&this).incoming_loop(handoff_packet)).await??;
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((Server::<Candidate>::from(this), packet_for_candidate))
    }

    pub fn start(connection: impl Connection, config: crate::config::Config) -> ServerHandle {
        let (packets_receive_tx, packets_receive_rx) = tokio::sync::mpsc::channel(32);
        let (packets_send_tx, packets_send_rx) = tokio::sync::mpsc::channel(32);

        let connection_h = tokio::spawn(Self::connection_loop(connection, packets_receive_tx, packets_send_rx));
        let timeout = Self::generate_random_timeout(config.election_timeout_min, config.election_timeout_max);
        tokio::spawn(async move {
            let mut follower = Self {
                connection_h,
                packets_in: packets_receive_rx.into(),
                packets_out: packets_send_tx,
                config,
                term: 0.into(),
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

impl From<Server<Candidate>> for Server<Follower> {
    fn from(candidate: Server<Candidate>) -> Self {
        let timeout = Self::generate_random_timeout(candidate.config.election_timeout_min, candidate.config.election_timeout_max);
        Self {
            connection_h: candidate.connection_h,
            packets_in: candidate.packets_in,
            packets_out: candidate.packets_out,
            config: candidate.config,
            term: candidate.term,
            state: Follower::new(timeout),
        }
    }
}

impl From<Server<Leader>> for Server<Follower> {
    fn from(leader: Server<Leader>) -> Self {
        let timeout = Self::generate_random_timeout(leader.config.election_timeout_min, leader.config.election_timeout_max);
        Self {
            connection_h: leader.connection_h,
            packets_in: leader.packets_in,
            packets_out: leader.packets_out,
            config: leader.config,
            term: leader.term,
            state: Follower::new(timeout),
        }
    }
}