use std::{collections::HashMap, sync::{RwLock, Mutex}, any::Any, fmt::{Debug, Display}};
use tokio::time::Instant;
use tracing::debug;

use crate::connection::{ServerAddress, Connection};

use super::Server;

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
#[derive(Debug)]
pub struct Follower {
    pub timeout: RwLock<Instant>,
    pub voted_for: Mutex<Ballot>,
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
        }
    }
}

#[derive(Debug)]
pub struct Candidate {
    pub timeout: RwLock<Instant>,
    pub votes: ElectionTally,
}

impl Candidate {
    pub fn new(timeout: Instant) -> Self {
        Self {
            timeout: timeout.into(),
            votes: ElectionTally::new(),
        }
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
    pub fn new() -> Self {
        Self {
            votes: Mutex::new(HashMap::new()),
        }
    }
    pub fn record_vote(&self, peer: &ServerAddress, is_granted: bool) {
        let mut election_results = self.votes.lock().expect("votes Mutex poisoned");

        // store the result from the vote
        debug!(peer = ?peer, ?is_granted, "vote recorded");
        election_results.insert(peer.to_owned(), is_granted);
    }

    pub fn vote_count(&self) -> usize {
        let election_results = self.votes.lock().expect("votes Mutex poisoned");
        debug!(votes = ?*election_results, "vote count");
        election_results.values().filter(|v| **v).count()
    }
}

#[derive(Debug)]
pub struct PeerIndices(RwLock<HashMap<ServerAddress, u64>>);

impl PeerIndices {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ServerAddress, u64>> {
        self.0.read().expect("PeerIndices lock was posioned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<ServerAddress, u64>> {
        self.0.write().expect("PeerIndices lock was posioned")
    }

    pub fn get(&self, peer: &ServerAddress) -> u64 {
        self.read().get(peer)
            .copied()
            .unwrap_or(0)
    }

    pub fn set(&self, peer: &ServerAddress, index: u64) {
        self.write().insert(peer.clone(), index);
    }

    pub fn decrement(&self, peer: &ServerAddress) {
        let mut map = self.write();
        let index = map.get(peer).unwrap_or(&1);
        let new_index = std::cmp::max(index - 1, 1);
        map.insert(peer.clone(), new_index);
    }

    pub fn greatest_quorum_index(&self) -> u64 {
        let map = self.read();
        let quorum = map.len() / 2;
        
        let mut common_indexes: HashMap<u64, usize> = HashMap::new();
        for (_, index) in map.iter() {
            let count = common_indexes.entry(*index)
                .and_modify(|cnt| *cnt += 1)
                .or_default();

            if *count > quorum {
                return *index;
            } 
        }

        0
    }
}

impl FromIterator<(ServerAddress, u64)> for PeerIndices {
    fn from_iter<T: IntoIterator<Item = (ServerAddress, u64)>>(iter: T) -> Self {
        let inner: HashMap<ServerAddress, u64> = iter.into_iter().collect();
        Self(inner.into())
    }
}

#[derive(Debug)]
pub struct Leader {
    pub next_index: PeerIndices,
    pub match_index: PeerIndices,
}

impl Display for Leader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Leader")
    }
}

pub enum ElectionResult<C: Connection> {
    Follower(Server<Follower, C>),
    Leader(Server<Leader, C>),
}

pub trait ServerState: Debug + Display + Any + Send + Sync {
    fn get_timeout(&self) -> Option<Instant>;
    fn set_timeout(&self, timeout: Instant);
}
impl ServerState for Follower {
    fn get_timeout(&self) -> Option<Instant> {
        Some(*self.timeout.read().expect("RwLock poisoned"))
    }
    fn set_timeout(&self, timeout: Instant) {
        *self.timeout.write().expect("RwLock poisoned") = timeout;
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
impl ServerState for Leader {
    fn get_timeout(&self) -> Option<Instant> {
        None
    }
    fn set_timeout(&self, _: Instant) {
        unimplemented!("Leader doesn't have timeout");
    }
}
