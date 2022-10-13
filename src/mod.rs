use std::{time::Duration, num::ParseIntError};

use clap::Parser;

use crate::connection::ServerAddress;

#[derive(Parser, Debug)]
pub struct Config {
    /// The address to listen on
    #[clap(short, long, default_value = "[::]:8000")]
    pub listen_socket: ServerAddress,

    /// A peer to connect to (use multiple times for multiple peers)
    #[clap(short, long = "peer")]
    pub peers: Vec<ServerAddress>,

    /// Heartbeat interval in milliseconds
    #[clap(long = "heartbeat", parse(try_from_str = parse_millis), default_value = "15")]
    pub heartbeat_interval: Duration,

    // Raft uses randomized election timeouts to ensure that
    // split votes are rare and that they are resolved quickly. To
    // prevent split votes in the first place, election timeouts are
    // chosen randomly from a fixed interval (e.g., 150–300ms).
    // This spreads out the servers so that in most cases only a
    // single server will time out; it wins the election and sends
    // heartbeats before any other servers time out. The same
    // mechanism is used to handle split votes. Each candidate
    // restarts its randomized election timeout at the start of an
    // election, and it waits for that timeout to elapse before
    // starting the next election; this reduces the likelihood of
    // another split vote in the new election. 

    /// Election min timeout in ms
    #[clap(long, parse(try_from_str = parse_millis), default_value = "150")]
    pub election_timeout_min: Duration,

    /// Election max timeout in ms
    #[clap(long, parse(try_from_str = parse_millis), default_value = "300")]
    pub election_timeout_max: Duration,
}

fn parse_millis(s: &str) -> Result<Duration, ParseIntError> {
    let millis: u64 = s.parse()?;
    Ok(Duration::from_millis(millis))
}