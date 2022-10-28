use std::{
    cmp,
    fmt::Display,
    sync::{atomic::Ordering, Arc, Mutex, RwLock},
};

use tokio::{
    sync::{mpsc, watch, oneshot},
    time::Instant,
};
use tracing::{debug, info, warn, error, info_span, Instrument};

use super::{
    candidate::ElectionResult,
    state::{Candidate, CurrentState, Leader, ServerState},
    HandlePacketAction, Result, Server, ServerHandle, StateResult,
};
use crate::{
    connection::{Connection, Packet, PacketType, ServerAddress},
    journal::{Journal, JournalEntry, Journalable},
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
    term: u64,
    choice: Option<ServerAddress>,
}

impl Ballot {
    pub fn cast_vote(&mut self, vote_term: u64, vote_choice: &ServerAddress) -> bool {
        // If the term has changed or we haven't voted in this term
        if self.term != vote_term || self.choice.is_none() {
            self.term = vote_term;
            self.choice = Some(vote_choice.clone());
            return true;
        }

        self.choice.as_ref() == Some(vote_choice)
    }
}

impl<C, J, D, V> Server<Follower, C, J, D, V>
where
    C: Connection<D, V>,
    J: Journal<D, V>,
    D: Journalable,
    V: Journalable,
{
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction<D, V>, V> {
        warn!("Follower timeout");
        // Advance to Candidate state on timeout
        Ok(HandlePacketAction::ChangeState(None))
    }

    pub(super) async fn handle_packet(
        &self,
        packet: Packet<D, V>,
    ) -> Result<HandlePacketAction<D, V>, V> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => self.handle_voterequest(&packet).await,
            AppendEntries { .. } => self.handle_appendentries(&packet).await,
            // Followers ignore these packets
            AppendEntriesAck { .. } | VoteResponse { .. } => {
                Ok(HandlePacketAction::MaintainState(None))
            }
        }
    }

    async fn handle_voterequest(
        &self,
        packet: &Packet<D, V>,
    ) -> Result<HandlePacketAction<D, V>, V> {
        let current_term = self.term.load(Ordering::Acquire);
        let (candidate_last_log_index, candidate_last_log_term) = match &packet.message_type {
            PacketType::VoteRequest {
                last_log_index,
                last_log_term,
            } => (*last_log_index, *last_log_term),
            _ => unreachable!("handle_voterequest called with non-PacketType::VoteRequest"),
        };

        let vote_granted = if packet.term == current_term {
            let our_last_log_index = self.journal.last_index();
            let candidate_log_valid = match candidate_last_log_index.cmp(&our_last_log_index) {
                // Candidate log is more up-to-date than ours
                cmp::Ordering::Greater => true,
                // Candidate log is not as up-to-date as ours
                cmp::Ordering::Less => false,
                // Candidate log is equally up-to-date as ours, so verify terms match;
                // if both terms are None, then they match without checking the journal,
                // because both Candidate and Follower journals are empty
                cmp::Ordering::Equal => candidate_last_log_index.map_or(true, |i| {
                    self.journal
                        .get(i)
                        .filter(|e| e.term == candidate_last_log_term)
                        .is_some()
                }),
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
        packet: &Packet<D, V>,
    ) -> Result<HandlePacketAction<D, V>, V> {
        let current_term = self.term.load(Ordering::Acquire);

        let (prev_log_index, prev_log_term, entries, leader_commit) = match &packet.message_type {
            PacketType::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => (*prev_log_index, prev_log_term, entries, leader_commit),
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
                .filter(|e| e.term == *prev_log_term)
                .is_some(),
        };

        let ack = term_matches && prev_log_matches;

        let match_index = if ack {
            for (packet_entry_idx, packet_entry) in entries.iter().enumerate() {
                let maybe_existing: Option<(u64, JournalEntry<D, V>)> = prev_log_index
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
                    let new_entry_idx = self.journal.append_entry(packet_entry.clone());
                    debug!(index = %new_entry_idx, ?packet_entry, "appended entry to journal");
                }
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            let last_entry_index = self.journal.last_index();
            if let Some(last_entry_index) = last_entry_index {
                let commit_index = self.journal.commit_index();
                if *leader_commit > commit_index {
                    let prev_commit = commit_index.unwrap_or(0);
                    let commit_index = std::cmp::min(leader_commit.unwrap(), last_entry_index);
                    debug!(%commit_index, "updating commit index");
                    let (results_tx, results): (Vec<_>, Vec<_>) = (prev_commit..=commit_index).into_iter()
                        .map(|i| {
                            let (tx, rx) = oneshot::channel();
                            ((i, tx), (i, rx))
                        })
                        .unzip();
                    self.journal.commit_and_apply(commit_index, results_tx);
                    let request_timeout = self.config.request_timeout;
                    tokio::spawn(async move {
                        use futures::future::join_all;
                        use tokio::time::timeout;

                        let (indices, receivers): (Vec<_>, Vec<_>) = results.into_iter().unzip();
                        // Wrap each result receiver in a timeout
                        let receivers = receivers.into_iter()
                            .map(|r| timeout(request_timeout, r));
                        // Await all results or timeouts
                        let results = join_all(receivers).await;
                        // Re-attach journal indices to results for logging
                        let results = indices.into_iter().zip(results);
                        for (index, result) in results {
                            match result {
                                Err(_) => error!(%index, "applying journal entry timed out"),
                                Ok(Err(error)) => error!(%index, %error, "error applying journal entry"),
                                Ok(Ok(result)) => debug!(?result, "successfully applied journal entry"),
                            }
                        }
                    }.instrument(info_span!("apply_committed_entries", %commit_index)));
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
        handoff_packet: Option<Packet<D, V>>,
    ) -> StateResult<Server<Candidate, C, J, D, V>, D, V> {
        let this = Arc::new(self);
        // Loop on incoming packets until a successful exit,
        // propagating any errors
        let packet_for_candidate = tokio::spawn(Arc::clone(&this).main(handoff_packet)).await??;
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((
            Server::<Candidate, C, J, D, V>::from(this),
            packet_for_candidate,
        ))
    }

    pub fn start(connection: C, journal: J, config: crate::config::Config) -> ServerHandle<V> {
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
                _snapshot: Default::default(),
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
}

impl<C, J, D, V> From<Server<Candidate, C, J, D, V>> for Server<Follower, C, J, D, V>
where
    C: Connection<D, V>,
    J: Journal<D, V>,
    D: Journalable,
    V: Journalable,
{
    fn from(candidate: Server<Candidate, C, J, D, V>) -> Self {
        let timeout = Self::generate_random_timeout(
            candidate.config.election_timeout_min,
            candidate.config.election_timeout_max,
        );
        Self {
            connection: candidate.connection,
            requests: candidate.requests,
            config: candidate.config,
            term: candidate.term,
            journal: candidate.journal,
            state: Follower::new(timeout),
            state_tx: candidate.state_tx,
            _snapshot: candidate._snapshot,
        }
    }
}

impl<C, J, D, V> From<Server<Leader<V>, C, J, D, V>> for Server<Follower, C, J, D, V>
where
    C: Connection<D, V>,
    J: Journal<D, V>,
    D: Journalable,
    V: Journalable,
{
    fn from(leader: Server<Leader<V>, C, J, D, V>) -> Self {
        let timeout = Self::generate_random_timeout(
            leader.config.election_timeout_min,
            leader.config.election_timeout_max,
        );
        Self {
            connection: leader.connection,
            requests: leader.requests,
            config: leader.config,
            term: leader.term,
            journal: leader.journal,
            state: Follower::new(timeout),
            state_tx: leader.state_tx,
            _snapshot: leader._snapshot,
        }
    }
}
