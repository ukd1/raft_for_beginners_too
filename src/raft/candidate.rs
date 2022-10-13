use std::sync::atomic::Ordering;

use tokio::time::{Instant, Duration};
use crate::raft::state::{Leader, Follower};
use super::{Result, Server, state::{Candidate, ElectionResult}};

impl Server<Candidate> {
    pub(super) async fn poll_electors(self) -> Result<ElectionResult> {
        self.term.fetch_add(1, Ordering::Release);
        println!("Candidate started");
        let won_election = rand::random();
        let next_state = if won_election {
            println!("Won (mock) election");
            ElectionResult::Leader(Server {
                connection_h: self.connection_h,
                packets_in: self.packets_in,
                packets_out: self.packets_out,
                config: self.config,
                term: self.term,
                state: Leader,
            })
        } else {
            println!("Lost (mock) election");
            let follower_timeout = Instant::now() + Duration::from_secs(5);
            ElectionResult::Follower(Server {
                connection_h: self.connection_h,
                packets_in: self.packets_in,
                packets_out: self.packets_out,
                config: self.config,
                term: self.term,
                state: Follower {
                    timeout: follower_timeout,
                    voted_for: None,
                },
            })
        };
        Ok(next_state)
    }
}

