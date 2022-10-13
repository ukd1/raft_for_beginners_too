use std::{time::{Instant, Duration}, collections::HashMap};

use typestate::typestate;

#[typestate]
mod raft {
    use std::{collections::HashMap, time::Instant};

    #[automaton]
    pub struct Server {
        pub term: u64,
    }

    #[state]
    pub struct Follower {
        pub timeout: Instant,
        pub voted_for: Option<String>,
    }
    pub trait Follower {
        fn new() -> Follower;
        fn follow(self) -> Candidate;
        fn shutdown(self);
    }

    #[state]
    pub struct Candidate {
        pub timeout: Instant,
        pub votes: HashMap<String, bool>,
    }
    pub trait Candidate {
        fn poll_electors(self) -> ElectionResult;
        fn shutdown(self);
    }

    #[state]
    pub struct Leader;
    pub trait Leader {
        fn lead(self) -> Follower;
        fn shutdown(self);
    }

    pub enum ElectionResult {
        Follower,
        Leader,
    }
}

use raft::*;

impl FollowerState for Server<Follower> {
    fn new() -> Self {
        let timeout = Instant::now() + Duration::from_secs(5);
        Self {
            term: 0,
            state: Follower {
                timeout,
                voted_for: None,
            },
        }
    }

    fn follow(self) -> Server<Candidate>  {
        println!("Follower started");
        let Follower { timeout, .. } = self.state;
        let sleep_time = timeout - Instant::now();
        std::thread::sleep(sleep_time);
        println!("Timeout elapsed");
        let election_timeout = Instant::now() + Duration::from_secs(5);
        Server {
            term: self.term,
            state: Candidate {
                votes: HashMap::new(),
                timeout: election_timeout,
            },
        }
    }

    fn shutdown(self) {
        todo!()
    }
}

impl CandidateState for Server<Candidate> {
    fn poll_electors(mut self) -> ElectionResult {
        self.term += 1;
        println!("Candidate started");
        println!("Won (mock) election");
        ElectionResult::Leader(Server {
            term: self.term,
            state: Leader,
        })
    }

    fn shutdown(self) {
        todo!()
    }
}

impl LeaderState for Server<Leader> {
    fn lead(mut self) -> Server<Follower>  {
        println!("Leader started");
        std::thread::sleep(Duration::from_secs(5));
        println!("Got (mock) other leader packet; Leader going back to follower");
        self.term += 1;
        let follower_timeout = Instant::now() + Duration::from_secs(5);
        Server { term: self.term, state: Follower { timeout: follower_timeout, voted_for: None } }
    }

    fn shutdown(self) {
        todo!()
    }
}

fn main() {
    let mut follower = Server::new();
    loop {
        let candidate = follower.follow();
        follower = match candidate.poll_electors() {
            ElectionResult::Leader(leader) => {
                leader.lead()
            },
            ElectionResult::Follower(follower) => follower,
        };
    }
}
