use std::{collections::HashMap, sync::{RwLock, Mutex}};
use tokio::time::Instant;

use crate::connection::ServerAddress;

use super::Server;

#[derive(Debug)]
pub struct Follower {
    pub timeout: RwLock<Instant>,
    pub voted_for: Mutex<Option<ServerAddress>>,
}

impl Follower {
    pub fn new(timeout: Instant) -> Self {
        Self {
            timeout: RwLock::new(timeout),
            voted_for: Mutex::new(None),
        }
    }
}

#[derive(Debug)]
pub struct Candidate {
    pub timeout: RwLock<Instant>,
    pub votes: ElectionTally,
}

#[derive(Debug)]
struct ElectionTally {
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
        election_results.insert(peer.to_owned(), is_granted);
    }

    pub fn vote_count(&self) -> usize {
        let election_results = self.votes.lock().expect("votes Mutex poisoned");
        election_results.values().filter(|v| **v).count() + 1
    }
}

impl From<Follower> for Candidate {
    fn from(follower: Follower) -> Self {
        Self {
            timeout: follower.timeout,
            votes: ElectionTally::new(),
        }
    }
}

#[derive(Debug)]
pub struct Leader;

pub enum ElectionResult {
    Follower(Server<Follower>),
    Leader(Server<Leader>),
}

pub trait ServerState: Send + Sync {
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
