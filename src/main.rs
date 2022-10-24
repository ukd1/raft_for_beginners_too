mod config;
mod connection;
mod journal;
mod raft;

use clap::Parser;

use crate::config::Config;
use crate::connection::{udp::UdpConnection, Connection};
use crate::raft::{Result, Server, ServerError};

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, EnvFilter};

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

    let connection = UdpConnection::bind(opts.listen_socket.clone()).await?;
    let server_handle = Server::start(connection, opts);
    //
    // DEBUG: test client events generator
    //
    let test_request_handle = {
        let this = server_handle.clone();
        tokio::spawn(async move {
            let mut i = 1;
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                match this.send(i.to_string()).await {
                    Err(ServerError::NotLeader) => {
                        this.state_change().await;
                        continue;
                    }
                    Err(e) => Err(e)?,
                    Ok(_) => {}
                }
                tracing::info!(%i, "test request added to journal");
                i += 1;
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
