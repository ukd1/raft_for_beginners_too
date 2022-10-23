use std::sync::{atomic::Ordering, Arc};

use tracing::{warn, info, debug, Instrument, Span, field};
use crate::{raft::state::Follower, connection::{Packet, PacketType, Connection}, journal::JournalValue};
use super::{Result, Server, state::{Candidate, ElectionResult}, HandlePacketAction, StateResult};

impl<C, V> Server<Candidate, C, V>
where
    C: Connection<V>,
    V: JournalValue,
{
    pub(super) async fn handle_timeout(&self) -> Result<HandlePacketAction<V>> {
        warn!("Candidate timeout");
        // Restart election and maintain state on timeout
        self.start_election().await?;
        Ok(HandlePacketAction::MaintainState(None))
    }

    pub(super) async fn handle_packet(&self, packet: Packet<V>) -> Result<HandlePacketAction<V>> {
        use PacketType::*;

        match packet.message_type {
            VoteRequest { .. } => Ok(HandlePacketAction::MaintainState(Some(Packet {
                message_type: PacketType::VoteResponse { is_granted: false },
                peer: packet.peer,
                term: packet.term,
            }))),
            VoteResponse { .. } => self.handle_voteresponse(&packet).await,
            // Candidates ignore these packets
            AppendEntries { .. } | AppendEntriesAck { .. } => Ok(HandlePacketAction::MaintainState(None)),
        }
    }

    async fn handle_voteresponse(&self, packet: &Packet<V>) -> Result<HandlePacketAction<V>> {
        let current_term = self.term.load(Ordering::Acquire);
        if packet.term != current_term {
            warn!(?packet.peer, ?packet.term, "got a vote response for the wrong term");
            return Ok(HandlePacketAction::MaintainState(None));
        }

        let is_granted = if let Packet { message_type: PacketType::VoteResponse { is_granted }, .. } = packet {
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

    async fn start_election(&self) -> Result<()> {
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
        let last_log_term = last_log_index.and_then(|i| self.journal.get(i)).map(|e| e.term).unwrap_or(0);

        self.reset_term_timeout().await;
        info!("starting new election");

        for peer in &self.config.peers {
            let peer_request = Packet {
                message_type: PacketType::VoteRequest { last_log_index, last_log_term },
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
    pub(super) async fn run(self, next_packet: Option<Packet<V>>) -> StateResult<ElectionResult<C, V>, V> {
        let this = Arc::new(self);
        let packet_for_next_state = {
            // Loop on incoming packets until a successful exit, and...
            let loop_h = tokio::spawn(Arc::clone(&this).main(next_packet));
            // ...send a voterequest packet to all peers, then...
            let this_election = Arc::clone(&this);
            let current_span = Span::current();
            tokio::spawn(async move { this_election.start_election().instrument(current_span).await }).await??; // TODO: add timeout
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

impl<C, V> From<Server<Follower, C, V>> for Server<Candidate, C, V>
where
    C: Connection<V>,
    V: JournalValue,
{
    fn from(follower: Server<Follower, C, V>) -> Self {
        let timeout = Self::generate_random_timeout(follower.config.election_timeout_min, follower.config.election_timeout_max);
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

