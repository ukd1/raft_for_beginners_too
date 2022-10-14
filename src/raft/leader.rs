use std::sync::{atomic::Ordering, Arc};

use tokio::time::Duration;

use super::{Result, Server, state::{Leader, Follower, Candidate}};

impl Server<Leader> {
    pub(super) async fn lead(self) -> Result<Server<Follower>> {
        let current_term = self.term.load(Ordering::Acquire);
        let this = Arc::new(self);
        println!("[Term {}] Leader started", current_term);
        {
            let mut tasks = this.tasks.lock().await;
            let heartbeat_handle = tasks.spawn(Arc::clone(&this).heartbeat_loop());
            tasks.spawn(Arc::clone(&this).incoming_loop());
            tasks
                .join_next()
                .await
                .expect("tasks should not be empty")??;
            // A task exited without error, must be incoming_loop relinquishing Leader state, so...
            // Shut down heartbeat_loop
            heartbeat_handle.abort();
            match tasks.join_next().await.expect("tasks should not be empty") {
                Err(e) if !e.is_cancelled() => Err(e)?,
                _ => {}, // Heartbeat exited normally or with an expected cancelled error
            }
            assert_eq!(tasks.len(), 1, "Only connect task should remain");
        }

        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        Ok(this.into())
    }

    async fn heartbeat_loop(self: Arc<Self>) -> Result<()> {
        let heartbeat_interval = Duration::from_secs(1);
        let mut ticker = tokio::time::interval(heartbeat_interval);
        loop {
            println!("Leader heartbeat");
            ticker.tick().await;
        }
    }
}

impl From<Server<Candidate>> for Server<Leader> {
    fn from(candidate: Server<Candidate>) -> Self {
        Self {
            tasks: candidate.tasks,
            packets_in: candidate.packets_in,
            packets_out: candidate.packets_out,
            config: candidate.config,
            term: candidate.term,
            state: Leader {},
        }
    }
}