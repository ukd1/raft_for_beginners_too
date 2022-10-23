mod config;
mod connection;
mod raft;
mod journal;

use clap::Parser;

use crate::config::Config;
use crate::connection::udp::UdpConnection;
use crate::raft::{Result, Server, ServerError};

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::{
        EnvFilter,
        prelude::*,
        filter::LevelFilter,
        fmt,
    };

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let fmt_layer = fmt::layer()
        .with_ansi(true)
        .with_timer(fmt::time::Uptime::default());
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    let opts = Config::parse();

    // make sure the opts are ok
    // assert_greater_than!(opts.heartbeat_interval, Duration::new(0, 0));
    // assert_lesser_than!(opts.heartbeat_interval, opts.election_timeout_min);
    // assert_lesser_than!(opts.election_timeout_min, opts.election_timeout_max);

    // TODO: find out how to make the types here less nasty
    let connection = UdpConnection::bind::<String>(opts.listen_socket.clone()).await?;
    let server_handle = Server::start(connection, opts);
    //
    // DEBUG: test client events generator
    // TODO: only do this when state == Leader
    //
    let test_request_handle = {
        let this = server_handle.clone();
        tokio::spawn(async move {
            let mut i = 0;
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                i += 1;
                ticker.tick().await;
                this.send(i.to_string()).await?;
                tracing::info!(%i, "test request added to journal");
            }
            #[allow(unreachable_code)]
            Ok::<_, ServerError>(())
        })
    };
    //
    // END DEBUG
    //
    tokio::select! {
        res = server_handle => res?,
        res = test_request_handle => res?,
    }
}
