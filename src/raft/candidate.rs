use std::{
    collections::HashMap,
    fmt::Display,
    sync::{atomic::Ordering, Arc, Mutex, RwLock},
};

use tokio::time::Instant;
use tracing::{debug, field, info, trace, warn, Instrument, Span};

use super::{
    state::{Follower, Leader, ServerState},
    HandlePacketAction, Result, Server, StateResult,
};
use crate::{
    connection::{Connection, Packet, PacketType, ServerAddress},
    journal::Journal,
};

#[derive(Debug)]
pub struct Candidate {
    pub timeout: RwLock<Instant>,
    pub votes: ElectionTally,
}

impl Candidate {
    pub fn new(timeout: Instant) -> Self {
        Self {
            timeout: timeout.into(),
            votes: ElectionTally::default(),
        }
    }
}

impl ServerState for Candidate {
    fn get_timeout(&self) -> Option<Instant> {
        Some(*self.timeout.read().expect("RwLock poisoned"))
    }
    fn set_timeout(&self, timeout: Instant) {
        *self.timeout.write().expect("RwLock poisoned") = timeout;
    }
}

impl Display for Candidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Candidate ({} votes)", self.votes.vote_count())
    }
}

#[derive(Debug)]
pub struct ElectionTally {
    votes: Mutex<HashMap<ServerAddress, bool>>,
}

impl ElectionTally {
    pub fn record_vote(&self, peer: &ServerAddress, is_granted: bool) {
        let mut election_results = self.votes.lock().expect("votes Mutex poisoned");

        // store the result from the vote
        debug!(peer = ?peer, ?is_granted, "vote recorded");
        election_results.insert(peer.to_owned(), is_granted);
    }

    pub fn vote_count(&self) -> usize {
        let election_results = self.votes.lock().expect("votes Mutex poisoned");
        trace!(votes = ?*election_results, "vote count");
        election_results.values().filter(|v| **v).count()
    }
}

impl Default for ElectionTally {
    fn default() -> Self {
        Self {
            votes: Mutex::new(HashMap::new()),
        }
    }
}

pub enum ElectionResult<C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    Follower(Server<Follower, C, J>),
    Leader(Server<Leader<J::Value, J::Applied>, C, J>),
}

impl<C, J> Server<Candidate, C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        warn!("Candidate timeout");
        // Restart election and maintain state on timeout
        self.start_election().await?;
        Ok(HandlePacketAction::MaintainState(None))
    }

    pub(super) async fn handle_packet(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => Ok(HandlePacketAction::MaintainState(Some(Packet {
                message_type: PacketType::VoteResponse { is_granted: false },
                peer: packet.peer,
                term: packet.term,
            }))),
            VoteResponse { .. } => self.handle_voteresponse(&packet).await,
            // Candidates ignore these packets
            AppendEntries { .. } => {
                let current_term = self.term.load(Ordering::Acquire);
                let action = if packet.term >= current_term {
                    HandlePacketAction::ChangeState(Some(packet))
                } else {
                    warn!(?packet.peer, ?packet.term, "got AppendEntries from a previous term");
                    HandlePacketAction::MaintainState(None)
                };
                Ok(action)
            }
            AppendEntriesAck { .. } => Ok(HandlePacketAction::MaintainState(None)),
        }
    }

    async fn handle_voteresponse(
        &self,
        packet: &Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term != current_term {
            warn!(?packet.peer, ?packet.term, "got a vote response for the wrong term");
            return Ok(HandlePacketAction::MaintainState(None));
        }

        let is_granted = if let Packet {
            message_type: PacketType::VoteResponse { is_granted },
            ..
        } = packet
        {
            *is_granted
        } else {
            unreachable!("handle_voteresponse called with a non-VoteResponse packet");
        };
        debug!(?packet.peer, ?packet.term, is_granted, "got a vote response");
        self.state.votes.record_vote(&packet.peer, is_granted);

        if self.has_won_election() {
            info!(
                vote_cnt = %(self.state.votes.vote_count() + 1),
                node_cnt = %(self.config.peers.len() + 1),
                "won election"
            );
            Ok(HandlePacketAction::ChangeState(None))
        } else {
            Ok(HandlePacketAction::MaintainState(None))
        }
    }

    fn has_won_election(&self) -> bool {
        // count votes
        // add 1 so we count ourselves
        let vote_cnt = self.state.votes.vote_count() + 1;
        let quorum = self.quorum();
        debug!(%vote_cnt, %quorum, "checking vote count");

        // did we get quorum, including our own vote?
        vote_cnt >= quorum
    }

    async fn start_election(&self) -> Result<(), J::Value> {
        let current_term = self.term.fetch_add(1, Ordering::Release) + 1;
        // There is a bug in tracing_subscriber that causes the
        // term field to be logged twice when an election timeout
        // occurs in Candidate state. The `tracing` crate correctly
        // updates the original `term` field on the span to the new
        // value, but `tracing_subscriber` only appends new values
        // to the log line for... reasons. See:
        // https://github.com/tokio-rs/tracing/issues/2334
        Span::current().record("term", current_term);
        let last_log_index = self.journal.last_index();
        let last_log_term = last_log_index
            .and_then(|i| self.journal.get(i))
            .map(|e| e.term)
            .unwrap_or(0);

        self.reset_term_timeout().await;
        info!("starting new election");

        for peer in &self.config.peers {
            let peer_request = Packet {
                message_type: PacketType::VoteRequest {
                    last_log_index,
                    last_log_term,
                },
                term: current_term,
                peer: peer.to_owned(),
            };
            self.connection.send(peer_request).await?;
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "election",
        skip_all,
        fields(
            term = field::Empty, // Set in self.start_election()
            state = %self.state,
        ),
    )]
    pub(super) async fn run(
        self,
        next_packet: Option<Packet<J::Snapshot, J::Value>>,
    ) -> StateResult<ElectionResult<C, J>, J::Snapshot, J::Value> {
        let this = Arc::new(self);
        let packet_for_next_state = {
            // Loop on incoming packets until a successful exit, and...
            let loop_h = tokio::spawn(Arc::clone(&this).main(next_packet));
            // ...send a voterequest packet to all peers, then...
            let this_election = Arc::clone(&this);
            let current_span = Span::current();
            tokio::spawn(async move {
                this_election
                    .start_election()
                    .instrument(current_span)
                    .await
            })
            .await??; // TODO: add timeout
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

impl<C, J> From<Server<Follower, C, J>> for Server<Candidate, C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    fn from(follower: Server<Follower, C, J>) -> Self {
        let timeout = Self::generate_random_timeout(
            follower.config.election_timeout_min,
            follower.config.election_timeout_max,
        );
        Self {
            connection: follower.connection,
            requests: follower.requests,
            config: follower.config,
            term: follower.term,
            journal: follower.journal,
            state: Candidate::new(timeout),
            state_tx: follower.state_tx,
        }
    }
}
