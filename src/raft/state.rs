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
