mod follower;
mod candidate;
mod leader;
mod state;

use std::sync::atomic::AtomicU64;

use tokio::task::JoinHandle;

use state::ServerState;

use crate::connection::ConnectionError;

pub type Result<T> = std::result::Result<T, ServerError>;
pub type ServerHandle = JoinHandle<Result<()>>;

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error(transparent)]
    ConnectionFailed(#[from] ConnectionError),
    #[error(transparent)]
    TaskPanicked(#[from] tokio::task::JoinError),
}

#[derive(Debug)]
pub struct Server<S: ServerState> {
    connection_h: JoinHandle<Result<()>>,
    config: crate::config::Config,
    pub state: S,
    pub term: AtomicU64,
}

