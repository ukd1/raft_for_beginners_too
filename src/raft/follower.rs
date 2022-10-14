use std::sync::{Arc, atomic::Ordering};

use tokio::{sync::Mutex, task::JoinSet};

use tracing::info;

use crate::connection::{Connection, Packet, PacketType};

use super::{Result, Server, state::{Follower, ElectionResult, Candidate, Leader}, ServerHandle, HandlePacketAction};

impl Server<Follower> {
    pub(super) async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => self.handle_voterequest(&packet).await,
            AppendEntries => self.handle_appendentries(&packet).await,
            // Followers ignore these packets
            AppendEntriesAck { .. } | VoteResponse { .. } => Ok(HandlePacketAction::NoReply),
        }
    }

    async fn handle_voterequest(&self, packet: &Packet) -> Result<HandlePacketAction> {
        let current_term = self.term.load(Ordering::Acquire);
        let vote_granted = if packet.term == current_term {
            self.reset_term_timeout().await;
            let mut last_vote = self.state.voted_for.lock().expect("voted_for Mutex poisoned");
            match &*last_vote {
                None => { 
                    // We didn't vote... --> true vote
                    *last_vote = Some(packet.peer.clone());
                    true
                },
                Some(p) if p == &packet.peer => {
                    // or we voted for this peer already --> true vote
                    // TODO: Is packet last_log_index last_log_term as up to date as our log?
                    true
                },
                Some(_) => {
                    // We already voted, and it wasn't for this peer --> False vote
                    false
                }
            }
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
        Ok(HandlePacketAction::Reply(reply))
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
        Ok(HandlePacketAction::Reply(reply))
    }

    async fn follow(self) -> Result<Server<Candidate>> {
        let current_term = self.term.load(Ordering::Acquire);
        println!("[Term {}] Follower started", current_term);
        let this = Arc::new(self);
        // Loop on incoming packets until a successful exit,
        // propagating any errors
        tokio::spawn(Arc::clone(&this).incoming_loop()).await??;
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok(Server::<Candidate>::from(this))
    }

    pub fn run(connection: impl Connection, config: crate::config::Config) -> ServerHandle {
        let (packets_receive_tx, packets_receive_rx) = tokio::sync::mpsc::channel(32);
        let (packets_send_tx, packets_send_rx) = tokio::sync::mpsc::channel(32);

        let mut tasks = JoinSet::new();
        tasks.spawn(
            Self::connection_loop(connection, packets_receive_tx, packets_send_rx)
        );
        let timeout = Self::generate_random_timeout(config.election_timeout_min, config.election_timeout_max);
        tokio::spawn(async move {
            let mut follower = Self {
                tasks: tasks.into(),
                packets_in: packets_receive_rx.into(),
                packets_out: packets_send_tx,
                config,
                term: 0.into(),
                state: Follower::new(timeout),
            };
            loop {
                let candidate = follower.follow().await?;
                follower = match candidate.start_election().await? {
                    ElectionResult::Leader(leader) => leader.lead().await?,
                    ElectionResult::Follower(follower) => follower,
                };
            }
        })
    }
}

impl From<Server<Candidate>> for Server<Follower> {
    fn from(candidate: Server<Candidate>) -> Self {
        let timeout = Self::generate_random_timeout(candidate.config.election_timeout_min, candidate.config.election_timeout_max);
        Self {
            tasks: candidate.tasks,
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
            tasks: leader.tasks,
            packets_in: leader.packets_in,
            packets_out: leader.packets_out,
            config: leader.config,
            term: leader.term,
            state: Follower::new(timeout),
        }
    }
}