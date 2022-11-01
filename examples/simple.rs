use std::error::Error;

use clap::Parser;
use raft_for_beginners_too::{
    connection::{Connection, UdpConnection},
    journal::mem::VecJournal,
    Config, Server, ServerError,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
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
    let journal = VecJournal::default();
    let server_handle = Server::start(connection, journal, opts);
    //
    // DEBUG: test client events generator
    //
    let test_request_handle = {
        let this = server_handle.clone();
        tokio::spawn(async move {
            let mut i = 0;
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                match this.send(i.to_string()).await {
                    // On a real client, the following two errors should result
                    // in retrying requests to the leader address, but because
                    // we are running a simulated client inside of each node,
                    // we just pause until the state changes.
                    Err(ServerError::Unavailable(_)) => {
                        this.state_change().await;
                        continue;
                    }
                    Err(ServerError::NotLeader { leader, .. }) => {
                        tracing::error!(?leader, "request sent to follower, OOPS");
                        this.state_change().await;
                        continue;
                    }
                    Err(e) => tracing::error!(error = %e, "request error"),
                    Ok(_) => {}
                }
                tracing::info!(%i, "test request added to journal");
                i += 1;
            }
            #[allow(unreachable_code)]
            Ok::<_, ServerError<_>>(())
        })
    };
    //
    // END DEBUG
    //
    tokio::select! {
        res = server_handle => res?,
        res = test_request_handle => res?,
    }?;
    Ok(())
}
