use std::{
    any::Any,
    fmt::{Debug, Display},
};

use tokio::time::Instant;

use super::ServerImpl;
pub use super::{candidate::Candidate, follower::Follower, leader::Leader};
use crate::{
    connection::Connection,
    journal::{Journal, Journalable, ApplyResult},
};

pub trait ServerState: Debug + Display + Any + Send + Sync {
    fn get_timeout(&self) -> Option<Instant>;
    fn set_timeout(&self, timeout: Instant);
}

#[derive(Clone, Copy, Debug)]
pub enum CurrentState {
    Follower,
    Candidate,
    Leader,
}

impl<'s, C, J, D, V, R> From<ServerImpl<'s, C, J, D, V, R>> for CurrentState
where
    C: Connection<D, V>,
    J: Journal<D, V, R>,
    D: Journalable,
    V: Journalable,
    R: ApplyResult
{
    fn from(server: ServerImpl<'s, C, J, D, V, R>) -> Self {
        use ServerImpl::*;

        match server {
            Follower(_) => Self::Follower,
            Candidate(_) => Self::Candidate,
            Leader(_) => Self::Leader,
        }
    }
}
