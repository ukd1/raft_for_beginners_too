use std::{sync::{Arc, atomic::{Ordering, AtomicU64}}, time::{Instant, Duration}, collections::HashMap};

use tokio::{time::sleep, task::JoinSet};

#[derive(Debug)]
pub struct ServerError;

#[derive(Debug)]
pub struct Server<S: ServerState> {
    pub state: S,
    pub term: AtomicU64,
}

mod state {
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
}

use state::*;

impl Server<Follower> {
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
        let this = Arc::new(self);
        let incoming_handle = tokio::spawn(Arc::clone(&this).incoming_loop());
        incoming_handle.await.expect("TODO: handle JoinError").expect("TODO: handle error");
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        let election_timeout = Instant::now() + Duration::from_secs(5);
        Server {
            term: this.term,
            state: Candidate {
                votes: HashMap::new(),
                timeout: election_timeout,
            },
        }
    }
}

impl Server<Candidate> {
    async fn poll_electors(self) -> ElectionResult {
        self.term.fetch_add(1, Ordering::Release);
        println!("Candidate started");
        let won_election = rand::random();
        if won_election {
            println!("Won (mock) election");
            ElectionResult::Leader(Server {
                term: self.term,
                state: Leader,
            })
        } else {
            println!("Lost (mock) election");
            let follower_timeout = Instant::now() + Duration::from_secs(5);
            ElectionResult::Follower(Server {
                term: self.term,
                state: Follower {
                    timeout: follower_timeout,
                    voted_for: None,
                },
            })
        }
    }
}

impl Server<Leader> {
    async fn lead(self) -> Server<Follower> {
        let current_term = self.term.load(Ordering::Acquire);
        let this = Arc::new(self);
        println!("[Term {}] Leader started", current_term);
        let mut tasks = JoinSet::new();
        tasks.spawn(Arc::clone(&this).heartbeat_loop());
        tasks.spawn(Arc::clone(&this).incoming_loop());
        tasks.join_next().await.unwrap().expect("JoinError: task panicked").expect("TODO: handle task error");
        // A task exited without error, must be incoming_loop relinquishing Leader state, so...
        // Shut down heartbeat_loop
        tasks.shutdown().await;

        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        let follower_timeout = Instant::now() + Duration::from_secs(5);
        Server { term: this.term, state: Follower { timeout: follower_timeout, voted_for: None } }
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
