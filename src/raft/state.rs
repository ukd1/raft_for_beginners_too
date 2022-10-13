use std::collections::HashMap;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Follower {
    pub timeout: Instant,
    pub voted_for: Option<String>,
}

#[derive(Debug)]
pub struct Candidate {
    pub timeout: Instant,
    pub votes: HashMap<String, bool>,
}

#[derive(Debug)]
pub struct Leader;

pub enum ElectionResult {
    Follower(super::Server<Follower>),
    Leader(super::Server<Leader>),
}

pub trait ServerState {
    fn get_timeout(&self) -> Option<Instant>;
    fn set_timeout(&mut self, timeout: Instant);
}
impl ServerState for Follower {
    fn get_timeout(&self) -> Option<Instant> {
        Some(self.timeout)
    }
    fn set_timeout(&mut self, timeout: Instant) {
        self.timeout = timeout;
    }
}
impl ServerState for Candidate {
    fn get_timeout(&self) -> Option<Instant> {
        Some(self.timeout)
    }
    fn set_timeout(&mut self, timeout: Instant) {
        self.timeout = timeout;
    }
}
impl ServerState for Leader {
    fn get_timeout(&self) -> Option<Instant> {
        None
    }
    fn set_timeout(&mut self, _: Instant) {
        unimplemented!("Leader doesn't have timeout");
    }
}
