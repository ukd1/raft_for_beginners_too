use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Display,
    sync::{atomic::Ordering, Arc, Mutex, RwLock},
};

use tokio::time::{timeout, Instant};
use tracing::{debug, trace, warn};

use crate::{
    connection::{Connection, Packet, PacketType, ServerAddress},
    journal::Journalable,
    raft::HandlePacketAction,
};

use super::{
    state::{Candidate, Follower, ServerState},
    ClientResultSender, Result, Server, StateResult,
};

#[derive(Debug)]
pub struct Leader<V: Journalable> {
    pub next_index: PeerIndices,
    pub match_index: PeerIndices,
    pub requests: Mutex<VecDeque<(u64, ClientResultSender<V>)>>,
}

impl<V: Journalable> ServerState for Leader<V> {
    fn get_timeout(&self) -> Option<Instant> {
        None
    }
    fn set_timeout(&self, _: Instant) {
        unimplemented!("Leader doesn't have timeout");
    }
}

impl<V: Journalable> Display for Leader<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Leader")
    }
}

#[derive(Debug)]
pub struct PeerIndices(RwLock<HashMap<ServerAddress, Option<u64>>>);

impl PeerIndices {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ServerAddress, Option<u64>>> {
        self.0.read().expect("PeerIndices lock was posioned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<ServerAddress, Option<u64>>> {
        self.0.write().expect("PeerIndices lock was posioned")
    }

    pub fn get(&self, peer: &ServerAddress) -> Option<u64> {
        self.read().get(peer).copied().flatten()
    }

    pub fn set(&self, peer: &ServerAddress, index: Option<u64>) {
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
    pub fn greatest_quorum_index(&self, quorum: usize) -> u64 {
        let map = self.read();

        let mut index_counts: BTreeMap<u64, usize> = BTreeMap::new();
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

impl FromIterator<(ServerAddress, Option<u64>)> for PeerIndices {
    fn from_iter<T: IntoIterator<Item = (ServerAddress, Option<u64>)>>(iter: T) -> Self {
        let inner: HashMap<ServerAddress, Option<u64>> = iter.into_iter().collect();
        Self(inner.into())
    }
}

impl<C, D, V> Server<Leader<V>, C, D, V>
where
    C: Connection<D, V>,
    D: Journalable,
    V: Journalable,
{
    pub(super) async fn handle_packet(
        &self,
        packet: Packet<D, V>,
    ) -> Result<HandlePacketAction<D, V>, V> {
        use PacketType::*;

        match packet.message_type {
            AppendEntriesAck { .. } => self.handle_appendentriesack(&packet).await,
            // Leaders ignore these packets
            AppendEntries { .. } | VoteRequest { .. } | VoteResponse { .. } => {
                Ok(HandlePacketAction::MaintainState(None))
            }
        }
    }

    pub async fn handle_clientrequest(&self, value: V, result_tx: ClientResultSender<V>) {
        let current_term = self.term.load(Ordering::SeqCst);
        let index = self.journal.append(current_term, value);
        // Setting match_index for the leader so that quorum
        // is counted correctly; next_index just to be correct
        let leader_addr = self.connection.address();
        self.state.next_index.set(&leader_addr, Some(index + 1));
        self.state.match_index.set(&leader_addr, Some(index));
        {
            let mut requests = self.state.requests.lock().expect("requests lock poisoned");
            requests.push_back((index, result_tx));
            trace!(%index, "added client response sender to requests queue");
        }
    }

    async fn handle_appendentriesack(
        &self,
        packet: &Packet<D, V>,
    ) -> Result<HandlePacketAction<D, V>, V> {
        // TODO: this is messy, and could be simplified into an Ack/Nack enum or by
        // removing did_append and using Some/None as the boolean
        match packet.message_type {
            PacketType::AppendEntriesAck { did_append, .. } if !did_append => {
                self.state.next_index.decrement(&packet.peer);
            }
            PacketType::AppendEntriesAck {
                did_append,
                match_index,
            } if did_append => {
                let next_index = match_index.map_or(0, |i| i + 1);
                self.state.next_index.set(&packet.peer, Some(next_index));
                self.state.match_index.set(&packet.peer, match_index);
                let commit_index = self.journal.commit_index();
                // Using the following match_index == commit_index == 0 logic
                // instead of using an Option<u64> for the commit_index, because
                // it allows us to use atomics inside journal instead of locking
                if match_index > commit_index {
                    let match_index = match_index.unwrap();
                    let quorum = self.quorum();
                    let quorum_index = self.state.match_index.greatest_quorum_index(quorum);
                    let current_term = self.term.load(Ordering::Acquire);
                    let match_index_term = self.journal.get(match_index).map_or(0, |e| e.term);
                    trace!(%quorum, %quorum_index, %current_term, %match_index_term, "checking commit index quorum");
                    if match_index <= quorum_index && match_index_term == current_term {
                        self.journal.set_commit_index(match_index);
                        debug!(commit_index = %match_index, "updated commit index");

                        {
                            let mut requests =
                                self.state.requests.lock().expect("requests lock poisoned");
                            loop {
                                match requests.pop_front() {
                                    Some((index, result_tx)) if index <= match_index => {
                                        trace!(%index, "found response sender for committed value");
                                        let result = result_tx.send(Ok(()));
                                        if result.is_err() {
                                            warn!(%index, "client request dropped");
                                        }
                                    }
                                    Some(r) => {
                                        requests.push_front(r);
                                        break;
                                    }
                                    None => break,
                                };
                            }
                        }
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
        next_packet: Option<Packet<D, V>>,
    ) -> StateResult<Server<Follower, C, D, V>, D, V> {
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
        Ok((this.into(), packet_for_next_state))
    }

    async fn heartbeat_loop(self: Arc<Self>) -> Result<(), V> {
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
                let peer_update = update_cache
                    .entry(peer_next_index)
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

impl<C, D, V> From<Server<Candidate, C, D, V>> for Server<Leader<V>, C, D, V>
where
    C: Connection<D, V>,
    D: Journalable,
    V: Journalable,
{
    fn from(candidate: Server<Candidate, C, D, V>) -> Self {
        // figure out match index
        let journal_next_index = candidate.journal.last_index().map(|i| i + 1).unwrap_or(0);
        let next_index: PeerIndices = candidate
            .config
            .peers
            .iter()
            .map(|p| (p.to_owned(), Some(journal_next_index)))
            .collect();
        let match_index: PeerIndices = candidate
            .config
            .peers
            .iter()
            .map(|p| (p.to_owned(), None))
            .collect();

        Self {
            connection: candidate.connection,
            requests: candidate.requests,
            config: candidate.config,
            term: candidate.term,
            journal: candidate.journal,
            state: Leader {
                next_index,
                match_index,
                requests: Default::default(),
            },
            state_tx: candidate.state_tx,
        }
    }
}
