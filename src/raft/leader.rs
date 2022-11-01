use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::{Debug, Display},
    sync::{atomic::Ordering, Arc, RwLock},
};

use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, trace};

use super::{
    state::{Follower, ServerState},
    ClientResultSender, Result, Server, StateResult,
};
use crate::{
    connection::{Connection, Packet, PacketType, ServerAddress},
    journal::{ApplyResult, Journal, JournalIndex, Journalable},
    raft::HandlePacketAction,
};

#[derive(Debug)]
pub struct Leader<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    pub next_index: PeerIndices,
    pub match_index: PeerIndices,
    pub requests: Mutex<VecDeque<(JournalIndex, ClientResultSender<V, R>)>>,
}

impl<V, R> ServerState for Leader<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    fn get_timeout(&self) -> Option<Instant> {
        None
    }
    fn set_timeout(&self, _: Instant) {
        unimplemented!("Leader doesn't have timeout");
    }
}

impl<V, R> Display for Leader<V, R>
where
    V: Journalable,
    R: ApplyResult,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Leader")
    }
}

#[derive(Debug)]
pub struct PeerIndices(RwLock<HashMap<ServerAddress, Option<JournalIndex>>>);

impl PeerIndices {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ServerAddress, Option<JournalIndex>>> {
        self.0.read().expect("PeerIndices lock was posioned")
    }

    fn write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<ServerAddress, Option<JournalIndex>>> {
        self.0.write().expect("PeerIndices lock was posioned")
    }

    pub fn get(&self, peer: &ServerAddress) -> Option<JournalIndex> {
        self.read().get(peer).copied().flatten()
    }

    pub fn set(&self, peer: &ServerAddress, index: Option<JournalIndex>) {
        self.write().insert(peer.clone(), index);
    }

    pub fn decrement(&self, peer: &ServerAddress) {
        let mut map = self.write();
        let new_index = map
            .get(peer)
            .copied()
            .flatten()
            .and_then(|i| i.checked_sub(1));
        map.insert(peer.clone(), new_index);
    }

    /// Get the highest index for which a quorum of nodes
    /// exists
    ///
    /// Parameters:
    /// quorum - the number of nodes required to establish a majority
    pub fn greatest_quorum_index(&self, quorum: usize) -> JournalIndex {
        let map = self.read();

        let mut index_counts: BTreeMap<JournalIndex, usize> = BTreeMap::new();
        for index in map.iter().filter_map(|(_, opt_i)| opt_i.as_ref()) {
            let count = index_counts
                .entry(*index)
                .and_modify(|cnt| *cnt += 1)
                .or_insert(1);

            // Early return if any one index value
            // has quorum
            if *count >= quorum {
                return *index;
            }
        }

        // In highest to lowest index order (rev), sum the count
        // of nodes whose index is the current value or higher
        let mut greatest_quorum_index_count = 0;
        for (index, count) in index_counts.into_iter().rev() {
            greatest_quorum_index_count += count;
            if greatest_quorum_index_count >= quorum {
                return index;
            }
        }

        // If there was no quorum, return index 0
        0
    }
}

impl FromIterator<(ServerAddress, Option<JournalIndex>)> for PeerIndices {
    fn from_iter<T: IntoIterator<Item = (ServerAddress, Option<JournalIndex>)>>(iter: T) -> Self {
        let inner: HashMap<ServerAddress, Option<JournalIndex>> = iter.into_iter().collect();
        Self(inner.into())
    }
}

impl<C, J> Server<Leader<J::Value, J::Applied>, C, J>
where
    C: Connection<J::Snapshot, J::Value>,
    J: Journal,
{
    pub(super) async fn handle_packet(
        &self,
        packet: Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        use PacketType::*;

        match packet.message_type {
            AppendEntriesAck { .. } => self.handle_appendentriesack(&packet).await,
            // Leaders ignore these packets
            AppendEntries { .. } | VoteRequest { .. } | VoteResponse { .. } => {
                Ok(HandlePacketAction::MaintainState(None))
            }
        }
    }

    pub async fn handle_clientrequest(
        &self,
        value: J::Value,
        result_tx: ClientResultSender<J::Value, J::Applied>,
    ) {
        let current_term = self.term.load(Ordering::SeqCst);
        let index = self.journal.append(current_term, value).await;
        // Setting match_index for the leader so that quorum
        // is counted correctly; next_index just to be correct
        let leader_addr = self.connection.address();
        self.state.next_index.set(&leader_addr, Some(index + 1));
        self.state.match_index.set(&leader_addr, Some(index));
        {
            let mut requests = self.state.requests.lock().await;
            requests.push_back((index, result_tx));
            trace!(%index, "added client response sender to requests queue");
        }
    }

    async fn handle_appendentriesack(
        &self,
        packet: &Packet<J::Snapshot, J::Value>,
    ) -> Result<HandlePacketAction<J::Snapshot, J::Value>, J::Value> {
        // TODO: this is messy, and could be simplified into an Ack/Nack enum or by
        // removing did_append and using Some/None as the boolean
        match packet.message_type {
            PacketType::AppendEntriesAck { did_append, .. } if !did_append => {
                trace!(peer = ?packet.peer, "AppendEntriesAck !did_append"); // DEBUG
                self.state.next_index.decrement(&packet.peer);
            }
            PacketType::AppendEntriesAck {
                did_append,
                match_index,
            } if did_append => {
                let next_index = match_index.map_or(0, |i| i + 1);
                self.state.next_index.set(&packet.peer, Some(next_index));
                self.state.match_index.set(&packet.peer, match_index);
                let commit_index = self.journal.commit_index().await;
                // Using the following match_index == commit_index == 0 logic
                // instead of using an Option<u64> for the commit_index, because
                // it allows us to use atomics inside journal instead of locking
                if match_index > commit_index {
                    let match_index = match_index.unwrap();
                    let quorum = self.quorum();
                    let quorum_index = self.state.match_index.greatest_quorum_index(quorum);
                    let current_term = self.term.load(Ordering::Acquire);
                    let match_index_term =
                        self.journal.get(match_index).await.map_or(0, |e| e.term);
                    trace!(%quorum, %quorum_index, %current_term, %match_index_term, "checking commit index quorum");
                    if match_index <= quorum_index && match_index_term == current_term {
                        let requests_to_commit: Vec<_> = {
                            let mut requests = self.state.requests.lock().await;
                            let last_committed_request_index =
                                requests.partition_point(|&(i, _)| i <= match_index);
                            requests.drain(0..last_committed_request_index).collect()
                        };

                        self.journal
                            .commit_and_apply(match_index, requests_to_commit)
                            .await;
                        debug!(commit_index = %match_index, "updated commit index");
                    }
                }
            }
            PacketType::AppendEntriesAck {
                match_index: None, ..
            } => {
                self.state.next_index.set(&packet.peer, Some(0));
                self.state.match_index.set(&packet.peer, None);
            }
            _ => unreachable!("handle_appendentriesack called with non-AppendEntriesAck packet"),
        }

        Ok(HandlePacketAction::MaintainState(None))
    }

    pub(super) async fn run(
        self,
        next_packet: Option<Packet<J::Snapshot, J::Value>>,
    ) -> StateResult<Server<Follower, C, J>, J::Snapshot, J::Value> {
        let this = Arc::new(self);

        let heartbeat_handle = tokio::spawn(Arc::clone(&this).heartbeat_loop());
        let incoming_loop_result = tokio::spawn(Arc::clone(&this).main(next_packet)).await;
        // 1. Shut down heartbeat_loop as soon as incoming_loop is done
        heartbeat_handle.abort();
        // 2. Raise any error from the incoming loop
        let packet_for_next_state = incoming_loop_result??;
        // 3. incoming_loop exited without error, so wait for
        //    heartbeat_loop to exit and check for errors
        match heartbeat_handle.await {
            Err(join_err) if !join_err.is_cancelled() => Err(join_err)?,
            Ok(Err(heartbeat_err)) => Err(heartbeat_err)?,
            _ => {} // Either exited normally or was cancelled
        }
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok((this.into_follower(), packet_for_next_state))
    }

    async fn heartbeat_loop(self: Arc<Self>) -> Result<(), J::Value> {
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
                trace!(?peer, next_index = ?peer_next_index, "getting update for peer"); // DEBUG
                let peer_update = match update_cache.get(&peer_next_index) {
                    Some(u) => u,
                    None => {
                        let update = self.journal.get_update(peer_next_index).await;
                        update_cache.insert(peer_next_index, update);
                        update_cache.get(&peer_next_index).unwrap()
                    }
                };
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

    fn into_follower(self) -> Server<Follower, C, J> {
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
            state: Follower::new(timeout),
            state_tx: self.state_tx,
        }
    }
}
