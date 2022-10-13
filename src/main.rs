use std::{time::{Instant, Duration}, collections::HashMap};

use async_trait::async_trait;
use typestate::typestate;

#[typestate]
mod raft {
    use async_trait::async_trait;
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
    #[async_trait]
    pub trait Follower {
        // typestate macro needs this to return a named state, not Self
        #[allow(clippy::new_ret_no_self)]
        fn new() -> Follower;
        async fn follow(self) -> Candidate;
        fn shutdown(self);
    }

    #[state]
    pub struct Candidate {
        pub timeout: Instant,
        pub votes: HashMap<String, bool>,
    }
    #[async_trait]
    pub trait Candidate {
        async fn poll_electors(self) -> ElectionResult;
        fn shutdown(self);
    }

    #[state]
    pub struct Leader;
    #[async_trait]
    pub trait Leader {
        async fn lead(self) -> Follower;
        fn shutdown(self);
    }

    pub enum ElectionResult {
        Follower,
        Leader,
    }
}

use raft::*;

#[async_trait]
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

    async fn follow(self) -> Server<Candidate>  {
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

#[async_trait]
impl CandidateState for Server<Candidate> {
    async fn poll_electors(mut self) -> ElectionResult {
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

#[async_trait]
impl LeaderState for Server<Leader> {
    async fn lead(mut self) -> Server<Follower>  {
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


#[tokio::main]
async fn main() {
    let mut follower = Server::new();
    loop {
        let candidate = follower.follow().await;
        follower = match candidate.poll_electors().await {
            ElectionResult::Leader(leader) => {
                leader.lead().await
            },
            ElectionResult::Follower(follower) => follower,
        };
    }
}
