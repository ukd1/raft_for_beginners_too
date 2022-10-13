use std::{time::{Instant, Duration}, sync::{Arc, atomic::Ordering}, collections::HashMap};

use tokio::time::sleep;

use super::{Result, Server, state::{Follower, ElectionResult, Candidate}, ServerHandle};


impl Server<Follower> {
    pub fn run() -> ServerHandle {
        let timeout = Instant::now() + Duration::from_secs(5);
        tokio::spawn(async move {
            let mut follower = Self {
                term: 0.into(),
                state: Follower {
                    timeout,
                    voted_for: None,
                },
            };
            loop {
                let candidate = follower.follow().await?;
                follower = match candidate.poll_electors().await? {
                    ElectionResult::Leader(leader) => leader.lead().await?,
                    ElectionResult::Follower(follower) => follower,
                };
            }
        })
    }

    async fn incoming_loop(self: Arc<Self>) -> Result<()> {
        let Follower { timeout, .. } = self.state;
        let sleep_time = timeout - Instant::now();
        #[allow(clippy::never_loop)] // For testing
        loop {
            sleep(sleep_time).await;
            println!("Timeout elapsed");
            break Ok(());
        }
    }

    async fn follow(self) -> Result<Server<Candidate>> {
        let current_term = self.term.load(Ordering::Acquire);
        println!("[Term {}] Follower started", current_term);
        let this = Arc::new(self);
        let incoming_handle = tokio::spawn(Arc::clone(&this).incoming_loop());
        incoming_handle.await??;
        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        let election_timeout = Instant::now() + Duration::from_secs(5);
        let candidate = Server {
            term: this.term,
            state: Candidate {
                votes: HashMap::new(),
                timeout: election_timeout,
            },
        };
        Ok(candidate)
    }
}

