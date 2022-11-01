use std::{
    cmp,
    fmt::Display,
    sync::{atomic::Ordering, Arc, Mutex, RwLock},
};

use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};
use tracing::{debug, error, info, info_span, warn, Instrument};

use super::{
    candidate::ElectionResult,
    state::{Candidate, CurrentState, ServerState},
    HandlePacketAction, Result, Server, ServerHandle, StateResult, Term,
};
use crate::{
    connection::{Connection, Packet, PacketType, ServerAddress},
    journal::Journal,
};

#[derive(Debug)]
pub struct Follower {
    pub timeout: RwLock<Instant>,
    pub voted_for: Mutex<Ballot>,
    pub leader: Mutex<Option<ServerAddress>>,
}

impl ServerState for Follower {
    fn get_timeout(&self) -> Option<Instant> {
        Some(*self.timeout.read().expect("RwLock poisoned"))
    }
    fn set_timeout(&self, timeout: Instant) {
        *self.timeout.write().expect("RwLock poisoned") = timeout;
    }
}

impl Display for Follower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Follower")
    }
}

impl Follower {
    pub fn new(timeout: Instant) -> Self {
        Self {
            timeout: RwLock::new(timeout),
            voted_for: Default::default(),
            leader: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Ballot {
    term: Term,
    choice: Option<ServerAddress>,
}

impl Ballot {
    pub fn cast_vote(&mut self, vote_term: Term, vote_choice: &ServerAddress) -> bool {
        // If the term has changed or we haven't voted in this term
        if self.term != vote_term || self.choice.is_none() {
            self.term = vote_term;
            self.choice = Some(vote_choice.clone());
            return true;
        }

        self.choice.as_ref() == Some(vote_choice)
    }
}

impl<C, J> Server<Follower, C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    pub(super) async fn handle_timeout(
        &self,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        warn!("Follower timeout");
        // Advance to Candidate state on timeout
        Ok(HandlePacketAction::ChangeState(None))
    }

    pub(super) async fn handle_packet(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => self.handle_voterequest(packet).await,
            AppendEntries { .. } => self.handle_appendentries(packet).await,
            // Followers ignore these packets
            AppendEntriesAck { .. } | VoteResponse { .. } => {
                Ok(HandlePacketAction::MaintainState(None))
            }
        }
    }

    async fn handle_voterequest(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        let current_term = self.term.load(Ordering::Acquire);
        let (candidate_last_log_index, candidate_last_log_term) = match packet.message_type {
            PacketType::VoteRequest {
                last_log_index,
                last_log_term,
            } => (last_log_index, last_log_term),
            _ => unreachable!("handle_voterequest called with non-PacketType::VoteRequest"),
        };

        let vote_granted = if packet.term == current_term {
            let our_last_log_index = self.journal.last_index().await;
            let candidate_log_valid = match candidate_last_log_index.cmp(&our_last_log_index) {
                // Candidate log is more up-to-date than ours
                cmp::Ordering::Greater => true,
                // Candidate log is not as up-to-date as ours
                cmp::Ordering::Less => false,
                // Candidate log is equally up-to-date as ours, so verify terms match;
                // if both terms are None, then they match without checking the journal,
                // because both Candidate and Follower journals are empty
                cmp::Ordering::Equal => match candidate_last_log_index {
                    None => true,
                    Some(i) => self
                        .journal
                        .get(i)
                        .await
                        .filter(|e| e.term == candidate_last_log_term)
                        .is_some(),
                },
            };

            if candidate_log_valid {
                let mut last_ballot = self
                    .state
                    .voted_for
                    .lock()
                    .expect("voted_for Mutex poisoned");
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
            message_type: PacketType::VoteResponse {
                is_granted: vote_granted,
            },
            term: current_term,
            peer: packet.peer.clone(),
        };
        info!(candidate = ?reply.peer, ?vote_granted, "casting vote");
        Ok(HandlePacketAction::MaintainState(Some(reply)))
    }

    async fn handle_appendentries(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        let current_term = self.term.load(Ordering::Acquire);

        let (prev_log_index, prev_log_term, entries, leader_commit) = match packet.message_type {
            PacketType::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => (prev_log_index, prev_log_term, entries, leader_commit),
            _ => unreachable!("handle_appendentries called with non-PacketType::AppendEntries"),
        };

        let term_matches = packet.term == current_term;

        if term_matches {
            self.reset_term_timeout().await;
        }

        let prev_log_matches = match prev_log_index {
            None => true,
            Some(index) => self
                .journal
                .get(index)
                .await
                .filter(|e| e.term == prev_log_term)
                .is_some(),
        };

        let ack = term_matches && prev_log_matches;

        let match_index = if ack {
            //
            // Remove any Snapshotting in-progress entries that came over from the leader.
            // It's less intensive to remove them here than on the leader, because the follower
            // is already iterating over each entry. The Snapshotting entry is just a bookmark
            // for the results of an in-progress snapshot operation on the leader, and all the
            // un-compacted entries the follower needs to catch up will have preceded it.
            //
            let appendable_entries = entries
                .into_iter()
                .filter(|e| !matches!(e.value, crate::journal::JournalEntryType::Snapshotting));
            for packet_entry in appendable_entries {
                let maybe_existing = self.journal.get(packet_entry.index).await;

                if let Some(existing_entry) = maybe_existing {
                    // 3. If an existing entry conflicts with a new one (same index but different terms)
                    if existing_entry.term != packet_entry.term {
                        // delete all delete the existing entry and all that follow it (§5.3)
                        warn!(last_index = %existing_entry.index, "truncated journal");
                        self.journal.truncate(existing_entry.index).await;
                        break;
                    }
                } else {
                    // 4. Append any new entries not already in the logs
                    let new_entry_idx = self.journal.append_entry(packet_entry).await;
                    debug!(index = %new_entry_idx, "appended entry to journal");
                }
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            let last_entry_index = self.journal.last_index().await;
            if let Some(last_entry_index) = last_entry_index {
                let commit_index = self.journal.commit_index().await;
                if leader_commit > commit_index {
                    let start_commit_range = commit_index.map_or(0, |i| i + 1);
                    let commit_index = std::cmp::min(leader_commit.unwrap(), last_entry_index);
                    debug!(%commit_index, "updating commit index");
                    let (results_tx, results): (Vec<_>, Vec<_>) = self
                        .journal
                        .indices_in_range(start_commit_range, commit_index)
                        .await
                        .into_iter()
                        .map(|i| {
                            let (tx, rx) = oneshot::channel();
                            ((i, tx), (i, rx))
                        })
                        .unzip();
                    self.journal
                        .commit_and_apply(commit_index, results_tx)
                        .await;
                    let request_timeout = self.config.request_timeout;
                    tokio::spawn(
                        async move {
                            use futures_util::future::join_all;
                            use tokio::time::timeout;

                            let (indices, receivers): (Vec<_>, Vec<_>) =
                                results.into_iter().unzip();
                            // Wrap each result receiver in a timeout
                            let receivers =
                                receivers.into_iter().map(|r| timeout(request_timeout, r));
                            // Await all results or timeouts
                            let results = join_all(receivers).await;
                            // Re-attach journal indices to results for logging
                            let results = indices.into_iter().zip(results);
                            for (index, result) in results {
                                match result {
                                    Err(_) => error!(%index, "applying journal entry timed out"),
                                    Ok(Err(error)) => {
                                        error!(%index, %error, "error applying journal entry")
                                    }
                                    Ok(Ok(result)) => {
                                        debug!(%index, ?result, "applied journal entry")
                                    }
                                }
                            }
                        }
                        .instrument(info_span!("apply_committed_entries", %commit_index)),
                    );
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

    async fn run(
        self,
        handoff_packet: Option<Packet<J::Snapshot, J::Value>>,
    ) -> StateResult<Server<Candidate, C, J>, J::Snapshot, J::Value> {
        let this = Arc::new(self);
        // Loop on incoming packets until a successful exit,
        // propagating any errors
        let packet_for_candidate = tokio::spawn(Arc::clone(&this).main(handoff_packet)).await??;
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((this.into_candidate(), packet_for_candidate))
    }

    pub fn start(
        connection: C,
        journal: J,
        config: crate::config::Config,
    ) -> ServerHandle<J::Value, J::Applied> {
        let timeout =
            Self::generate_random_timeout(config.election_timeout_min, config.election_timeout_max);
        let (requests_tx, requests_rx) = mpsc::channel(64);
        let (state_tx, state_rx) = watch::channel(CurrentState::Follower);
        let handle_timeout = config.request_timeout;
        let join_h = tokio::spawn(async move {
            let mut follower = Self {
                connection,
                requests: requests_rx.into(),
                config,
                journal,
                term: 0.into(),
                state: Follower::new(timeout),
                state_tx,
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
        });
        ServerHandle::new(join_h, requests_tx, state_rx, handle_timeout)
    }

    fn into_candidate(self) -> Server<Candidate, C, J> {
        let timeout = Self::generate_random_timeout(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
        Server {
            connection: self.connection,
            requests: self.requests,
            config: self.config,
            term: self.term,
            journal: self.journal,
            state: Candidate::new(timeout),
            state_tx: self.state_tx,
        }
    }
}
