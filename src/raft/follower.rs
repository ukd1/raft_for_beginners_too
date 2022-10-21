use std::sync::{Arc, atomic::Ordering};

use tracing::{info, warn, debug};

use crate::{connection::{Connection, Packet, PacketType}, journal::JournalEntry};

use super::{Result, Server, state::{Follower, ElectionResult, Candidate, Leader}, HandlePacketAction, StateResult, ServerHandle};

impl<C: Connection> Server<Follower, C> {
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction> {
        warn!("Follower timeout");
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
        let (candidate_last_log_index, candidate_last_log_term) = match &packet.message_type {
            PacketType::VoteRequest { last_log_index, last_log_term } => (*last_log_index, *last_log_term),
            _ => unreachable!("handle_voterequest called with non-PacketType::VoteRequest"),
        };


        let vote_granted = if packet.term == current_term {
            let our_last_log_index = self.journal.last_index();
            let candidate_log_valid = if candidate_last_log_index == our_last_log_index {
                // Candidate log is equally up-to-date as ours, so verify terms match;
                // if both terms are None, then they match without checking the journal,
                // because both Candidate and Follower journals are empty
                candidate_last_log_index.map_or(true, |i| {
                    self.journal.get(i)
                        .filter(|e| e.term == candidate_last_log_term)
                        .is_some()
                })
            } else if candidate_last_log_index > our_last_log_index {
                // Candidate log is more up-to-date than ours
                true
            } else {
                // Candidate log is not as up-to-date as ours
                false
            };

            if candidate_log_valid {
                let mut last_ballot = self.state.voted_for.lock().expect("voted_for Mutex poisoned");
                // Check if we have already voted for someone else in this term
                last_ballot.cast_vote(packet.term, &packet.peer)
            } else {
                // Candidate log is not valid
                false
            }
        } else {
            // Packet term is too old
            false
        };

        if vote_granted {
            self.reset_term_timeout().await;
        }
        
        let reply = Packet {
            message_type: PacketType::VoteResponse { is_granted: vote_granted },
            term: current_term,
            peer: packet.peer.clone(),
        };
        info!(candidate = ?reply.peer, ?vote_granted, "casting vote");
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

        let term_matches = packet.term == current_term;

        if term_matches {
            self.reset_term_timeout().await;
        }

        let prev_log_matches = match prev_log_index {
            None => true,
            Some(index) => self.journal.get(index)
                .filter(|e| e.term == *prev_log_term)
                .is_some()
        };

        let ack = term_matches && prev_log_matches;

        let match_index = if ack {
            for (packet_entry_idx, packet_entry) in entries.iter().enumerate() {
                let maybe_existing: Option<(u64, JournalEntry)> = prev_log_index
                    .map(|prev_log_index| prev_log_index + (packet_entry_idx as u64) + 1)
                    .and_then(|i| Some(i).zip(self.journal.get(i)));

                if let Some((existing_entry_idx, existing_entry)) = maybe_existing {
                    // 3. If an existing entry conflicts with a new one (same index but different terms)
                    if existing_entry.term != packet_entry.term {
                        // delete all delete the existing entry and all that follow it (§5.3)
                        self.journal.truncate(existing_entry_idx);
                        warn!(last_index = %existing_entry_idx, "truncated journal");
                        break;
                    }
                } else {
                    // 4. Append any new entries not already in the logs
                    let new_entry_idx = self.journal.append_entry(packet_entry.to_owned());
                    debug!(index = %new_entry_idx, ?packet_entry, "appended entry to journal");
                }
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            let last_entry_index = self.journal.last_index();
            if let Some(last_entry_index) = last_entry_index {
                let commit_index = self.journal.commit_index.load(Ordering::Acquire).try_into()?;
                if *leader_commit > commit_index {
                    let commit_index = std::cmp::min(*leader_commit, last_entry_index);
                    debug!(%commit_index, "updating commit index");
                    self.journal.commit_index.store(commit_index.try_into()?, Ordering::Release);
                }
            }

            last_entry_index
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
                state: Follower::new(timeout),
            };
            let mut packet = None;
            let mut candidate;
            info!("starting node");
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
            journal: leader.journal,
            state: Follower::new(timeout),
        }
    }
}