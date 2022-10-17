mod config;
mod connection;
mod raft;

use clap::Parser;

use crate::config::Config;
use crate::connection::Connection;
use crate::connection::udp::UdpConnection;
use crate::raft::{Result, Server};

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::{
        EnvFilter,
        prelude::*,
        filter::LevelFilter,
    };

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_forest::ForestLayer::default())
        .init();

    let opts = Config::parse();

    // make sure the opts are ok
    // assert_greater_than!(opts.heartbeat_interval, Duration::new(0, 0));
    // assert_lesser_than!(opts.heartbeat_interval, opts.election_timeout_min);
    // assert_lesser_than!(opts.election_timeout_min, opts.election_timeout_max);

    let connection = UdpConnection::bind(opts.listen_socket.clone()).await.unwrap();
    let server_handle = Server::start(connection, opts);
    server_handle.await?
}
