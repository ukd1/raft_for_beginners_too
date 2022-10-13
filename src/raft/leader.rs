use std::sync::{atomic::Ordering, Arc};

use tokio::{task::JoinSet, time::{Instant, Duration}};

use super::{Result, Server, state::{Leader, Follower}};

impl Server<Leader> {
    pub(super) async fn lead(self) -> Result<Server<Follower>> {
        let current_term = self.term.load(Ordering::Acquire);
        let this = Arc::new(self);
        println!("[Term {}] Leader started", current_term);
        let mut tasks = JoinSet::new();
        tasks.spawn(Arc::clone(&this).heartbeat_loop());
        tasks.spawn(Arc::clone(&this).incoming_loop());
        tasks
            .join_next()
            .await
            .expect("tasks should not be empty")??;
        // A task exited without error, must be incoming_loop relinquishing Leader state, so...
        // Shut down heartbeat_loop
        tasks.shutdown().await;

        let this = Arc::try_unwrap(this).expect("should have exclusive ownership here");
        let follower_timeout = Instant::now() + Duration::from_secs(5);
        let follower = Server {
            connection_h: this.connection_h,
            packets_in: this.packets_in,
            packets_out: this.packets_out,
            config: this.config,
            term: this.term,
            state: Follower {
                timeout: follower_timeout,
                voted_for: None,
            },
        };
        Ok(follower)
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

