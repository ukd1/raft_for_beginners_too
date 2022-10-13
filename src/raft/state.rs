use std::{collections::HashMap, time::Instant};

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

pub trait ServerState {}
impl ServerState for Follower {}
impl ServerState for Candidate {}
impl ServerState for Leader {}
