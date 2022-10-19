use std::sync::{Arc, atomic::Ordering};

use tracing::{info, warn, debug};

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
            AppendEntries { .. } => self.handle_appendentries(&packet).await,
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

        let (prev_log_index, prev_log_term, entries, leader_commit) = match &packet.message_type {
            PacketType::AppendEntries { prev_log_index, prev_log_term, entries, leader_commit} => (
                *prev_log_index, prev_log_term, entries, leader_commit
            ),
            _ => unreachable!("handle_appendentries called with non-PacketType::AppendEntries"),
        };
        let ack = if packet.term != current_term {
            // Packet term is too old
            false
        } else if prev_log_index == 0 {
            true
        } else if let Some(entry) = self.journal.get(prev_log_index) {
            entry.term == *prev_log_term
        } else {
            false
        };

        let match_index = if ack {
            for (i, entry) in entries.iter().enumerate() {
                let entry_index = prev_log_index + (i as u64);
                if let Some(existing_entry) = self.journal.get(entry_index) {
                    // 3. If an existing entry conflicts with a new one (same index but different terms)
                    if existing_entry.term != entry.term {
                        // delete all delete the existing entry and all that follow it (§5.3)
                        self.journal.truncate(entry_index);
                        warn!(last_index = %entry_index, "truncated journal");
                        break;
                    }
                } else {
                    // 4. Append any new entries not already in the logs
                    self.journal.append_entry(entry.to_owned());
                    debug!(index = %entry_index, "appended entry to journal");
                }
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            let commit_index = self.journal.commit_index.load(Ordering::Acquire).try_into()?;
            let last_entry_index = self.journal.last_index();
            if *leader_commit > commit_index {
                let commit_index = std::cmp::min(*leader_commit, last_entry_index);
                self.journal.commit_index.store(commit_index.try_into()?, Ordering::Release);
            }

            self.reset_term_timeout().await;
            Some(last_entry_index)
        } else {
            None
        };

        let reply = Packet {
            message_type: PacketType::AppendEntriesAck {
                did_append: ack,
                match_index,
            },
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
                journal: Default::default(),
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
            journal: candidate.journal,
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
            journal: leader.journal,
            state: Follower::new(timeout),
        }
    }
}