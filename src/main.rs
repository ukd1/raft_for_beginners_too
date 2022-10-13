use std::{sync::{Arc, atomic::Ordering}, time::{Instant, Duration}, collections::HashMap};

use async_trait::async_trait;
use typestate::typestate;
use tokio::{time::sleep, task::JoinSet};

#[derive(Debug)]
pub struct ServerError;

#[typestate]
mod raft {
    use std::{collections::HashMap, sync::{Arc, atomic::AtomicU64}, time::Instant};

    use async_trait::async_trait;

    use crate::ServerError;

    #[automaton]
    #[derive(Debug)]
    pub struct Server {
        pub term: AtomicU64,
    }

    #[state]
    #[derive(Debug)]
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
        async fn incoming_loop(self: Arc<Self>) -> Result<(), ServerError>;
        fn shutdown(self);
    }

    #[state]
    #[derive(Debug)]
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
    #[derive(Debug)]
    pub struct Leader;
    #[async_trait]
    pub trait Leader {
        async fn lead(self) -> Follower;
        async fn heartbeat_loop(self: Arc<Self>) -> Result<(), ServerError>;
        async fn incoming_loop(self: Arc<Self>) -> Result<(), ServerError>;
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
            term: 0.into(),
            state: Follower {
                timeout,
                voted_for: None,
            },
        }
    }

    async fn incoming_loop(self: Arc<Self>) -> Result<(), ServerError> {
        let Follower { timeout, .. } = self.state;
        let sleep_time = timeout - Instant::now();
        #[allow(clippy::never_loop)] // For testing
        loop {
            sleep(sleep_time).await;
            println!("Timeout elapsed");
            break Ok(())
        }
    }

    async fn follow(self) -> Server<Candidate>  {
        let current_term = self.term.load(Ordering::Acquire);
        println!("[Term {}] Follower started", current_term);
        let self = Arc::new(self);
        let incoming_handle = tokio::spawn(Arc::clone(&self).incoming_loop());
        incoming_handle.await.expect("TODO: handle JoinError").expect("TODO: handle error");
        let self = Arc::try_unwrap(self).expect("should have exclusive ownership here");
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
        self.term.fetch_add(1, Ordering::Release);
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
    async fn lead(mut self) -> Server<Follower> {
        let current_term = self.term.load(Ordering::Acquire);
        let self = Arc::new(self);
        println!("[Term {}] Leader started", current_term);
        let mut tasks = JoinSet::new();
        tasks.spawn(Arc::clone(&self).heartbeat_loop());
        tasks.spawn(Arc::clone(&self).incoming_loop());
        tasks.join_next().await.unwrap().expect("JoinError: task panicked").expect("TODO: handle task error");
        // A task exited without error, must be incoming_loop relinquishing Leader state, so...
        // Shut down heartbeat_loop
        tasks.shutdown().await;

        let self = Arc::try_unwrap(self).expect("should have exclusive ownership here");
        let follower_timeout = Instant::now() + Duration::from_secs(5);
        Server { term: self.term, state: Follower { timeout: follower_timeout, voted_for: None } }
    }

    async fn heartbeat_loop(self: Arc<Self>) -> Result<(), ServerError> {
        let heartbeat_interval = Duration::from_secs(1);
        let mut ticker = tokio::time::interval(heartbeat_interval);
        loop {
            println!("Leader heartbeat");
            ticker.tick().await;
        }
    }

    async fn incoming_loop(self: Arc<Self>) -> Result<(), ServerError> {
        #[allow(clippy::never_loop)] // For testing
        loop {
            sleep(Duration::from_secs(5)).await;
            self.term.fetch_add(1, Ordering::Release);
            println!("Got (mock) other leader packet; Leader going back to follower");
            break Ok(())
        }
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
