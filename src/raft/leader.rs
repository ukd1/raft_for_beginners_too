use std::{sync::{atomic::Ordering, Arc}, collections::HashMap};

use tokio::time::timeout;
use tracing::{debug, trace};

use crate::{connection::{PacketType, Packet, Connection}, raft::HandlePacketAction};

use super::{Result, Server, state::{Leader, Follower, Candidate, PeerIndices}, StateResult};

impl<C: Connection> Server<Leader, C> {
    pub(super) async fn handle_packet(&self, packet: Packet) -> Result<HandlePacketAction> {
        use PacketType::*;

        match packet.message_type {
            AppendEntriesAck { .. } => self.handle_appendentriesack(&packet).await,
            // Leaders ignore these packets
            AppendEntries { .. } | VoteRequest { .. } | VoteResponse { .. } => Ok(HandlePacketAction::MaintainState(None)),
        }
    }

    // TODO: this should take a Packet with PacketType::ClientRequest
    async fn handle_clientrequest(&self, cmd: String) {
        let current_term = self.term.load(Ordering::SeqCst);
        let index = self.journal.append(current_term, cmd);
        // Setting match_index for the leader so that quorum
        // is counted correctly; next_index just to be correct
        let leader_addr = self.connection.address();
        self.state.next_index.set(&leader_addr, Some(index + 1));
        self.state.match_index.set(&leader_addr, Some(index));
    }

    async fn handle_appendentriesack(&self, packet: &Packet) -> Result<HandlePacketAction> {
        // TODO: this is messy, and could be simplified into an Ack/Nack enum or by
        // removing did_append and using Some/None as the boolean
        match packet.message_type {
            PacketType::AppendEntriesAck { did_append, .. } if !did_append => {
                self.state.next_index.decrement(&packet.peer);
            },
            PacketType::AppendEntriesAck { did_append, match_index: Some(match_index) } if did_append => {
                self.state.next_index.set(&packet.peer, Some(match_index + 1));
                self.state.match_index.set(&packet.peer, Some(match_index));
                let commit_index = self.journal.commit_index();
                if match_index > commit_index {
                    let quorum = self.quorum();
                    let quorum_index = self.state.match_index.greatest_quorum_index(quorum);
                    let current_term = self.term.load(Ordering::Acquire);
                    let match_index_term = self.journal.get(match_index).map_or(0, |e| e.term);
                    trace!(%quorum, %quorum_index, %current_term, %match_index_term, "checking commit index quorum");
                    if match_index <= quorum_index && match_index_term == current_term {
                        self.journal.set_commit_index(match_index);
                        debug!(commit_index = %match_index, "updated commit index");
                    }
                }
            },
            PacketType::AppendEntriesAck { match_index: None, .. } => {
                self.state.next_index.set(&packet.peer, Some(0));
                self.state.match_index.set(&packet.peer, None);
            },
            _ => unreachable!("handle_appendentriesack called with non-AppendEntriesAck packet"),
        }

        Ok(HandlePacketAction::MaintainState(None))
    }

    pub(super) async fn run(self, next_packet: Option<Packet>) -> StateResult<Server<Follower, C>> {
        let this = Arc::new(self);

        //
        // DEBUG: test client events generator
        //
        let test_request_handle = {
            let this = Arc::clone(&this);
            tokio::spawn(async move {
                let mut i = 0;
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    i += 1;
                    ticker.tick().await;
                    this.handle_clientrequest(format!("{}", i)).await;
                    tracing::info!(%i, last_index = ?this.journal.last_index(), "test request added to journal");
                }
            })
        };
        //
        // END DEBUG
        //
        let heartbeat_handle = tokio::spawn(Arc::clone(&this).heartbeat_loop());
        let incoming_loop_result = tokio::spawn(Arc::clone(&this).main(next_packet)).await;
        // 1. Shut down heartbeat_loop as soon as incoming_loop is done
        heartbeat_handle.abort();
        test_request_handle.abort(); // DEBUG: shutdown test client events generator
        // 2. Raise any error from the incoming loop
        let packet_for_next_state = incoming_loop_result??;
        // 3. incoming_loop exited without error, so wait for
        //    heartbeat_loop to exit and check for errors
        match heartbeat_handle.await {
            Err(join_err) if !join_err.is_cancelled() => Err(join_err)?,
            Ok(Err(heartbeat_err)) => Err(heartbeat_err)?,
            _ => {}, // Either exited normally or was cancelled
        }
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((this.into(), packet_for_next_state))
    }

    async fn heartbeat_loop(self: Arc<Self>) -> Result<()> {
        let heartbeat_interval = self.config.heartbeat_interval;
        let mut journal_changes = self.journal.subscribe();

        // Updates go out every heartbeat_interval (default 15ms),
        // so cache the results of Journal.get_update(INDEX) for the period
        // of a single heartbeat so that it doesn't have to be called repeatedly
        // for peers on the same next_index; pre-allocate to the maximum possible
        // size (i.e. each peer has a different next_index value)
        let mut update_cache = HashMap::with_capacity(self.config.peers.len());
        loop {
            update_cache.clear();
            if let Ok(Err(_)) = timeout(heartbeat_interval, journal_changes.changed()).await {
                panic!("journal_changes sender dropped");
            }
            for peer in &self.config.peers {
                let peer_next_index = self.state.next_index.get(peer);
                // let peer_update = self.journal.get_update(peer_next_index);
                let peer_update = update_cache.entry(peer_next_index)
                    .or_insert_with(|| self.journal.get_update(peer_next_index));
                let heartbeat = PacketType::AppendEntries {
                    prev_log_index: peer_update.prev_index,
                    prev_log_term: peer_update.prev_term,
                    entries: peer_update.entries.clone(),
                    leader_commit: peer_update.commit_index,
                };
                let peer_request = Packet {
                    message_type: heartbeat,
                    peer: peer.to_owned(),
                    term: self.term.load(Ordering::Acquire),
                };
                self.connection.send(peer_request).await?;
            }
        }
    }
}

impl<C: Connection> From<Server<Candidate, C>> for Server<Leader, C> {
    fn from(candidate: Server<Candidate, C>) -> Self {

        // figure out match index
        let journal_next_index = candidate.journal.last_index().map(|i| i + 1).unwrap_or(0);
        let next_index: PeerIndices = candidate.config.peers.iter().map(|p| (p.to_owned(), Some(journal_next_index))).collect();
        let match_index: PeerIndices = candidate.config.peers.iter().map(|p| (p.to_owned(), None)).collect();


        Self {
            connection: candidate.connection,
            config: candidate.config,
            term: candidate.term,
            journal: candidate.journal,
            state: Leader {
                next_index: next_index.into(),
                match_index: match_index.into(),
            },
        }
    }
}