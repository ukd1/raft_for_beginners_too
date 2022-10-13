use std::{time::{Instant, Duration}, sync::{Arc, atomic::Ordering}, collections::HashMap};

use tokio::time::sleep;
use tokio::sync::mpsc::{Sender, Receiver};
use tracing::trace;

use crate::connection::{Connection, Packet};

use super::{Result, Server, state::{Follower, ElectionResult, Candidate}, ServerHandle};


impl Server<Follower> {
    pub fn run(connection: impl Connection, config: crate::config::Config) -> ServerHandle {
        let timeout = Instant::now() + Duration::from_secs(5);

        let (packets_receive_tx, packets_receive_rx) = tokio::sync::mpsc::channel(32);
        let (packets_send_tx, packets_send_rx) = tokio::sync::mpsc::channel(32);

        let connection_h = tokio::spawn(
            Self::connection_loop(connection, packets_receive_tx, packets_send_rx)
        );
        tokio::spawn(async move {
            let mut follower = Self {
                connection_h,
                config,
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

    async fn connection_loop(connection: impl Connection, incoming: Sender<Packet>, mut outgoing: Receiver<Packet>) -> Result<()> {
        loop {
            tokio::select! {
                packet = connection.receive() => {
                    let packet = packet?;
                    trace!(?packet, "receive");
                    //incoming.try_send(packet).expect("TODO: ConnectionError");
                    let _ = incoming.send(packet).await; // DEBUG: try ignoring send errors
                },
                Some(packet) = outgoing.recv() => {
                    connection.send(packet).await?;
                },
            }
        }
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
            connection_h: this.connection_h,
            config: this.config,
            term: this.term,
            state: Candidate {
                votes: HashMap::new(),
                timeout: election_timeout,
            },
        };
        Ok(candidate)
    }
}

