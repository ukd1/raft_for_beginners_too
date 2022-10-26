use std::{
    any::Any,
    fmt::{Debug, Display},
};

use tokio::time::Instant;

use crate::{connection::Connection, journal::JournalValue};

use super::ServerImpl;
pub use super::{candidate::Candidate, follower::Follower, leader::Leader};

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

impl<'s, C, D, V> From<ServerImpl<'s, C, D, V>> for CurrentState
where
    C: Connection<D, V>,
    D: JournalValue,
    V: JournalValue,
{
    fn from(server: ServerImpl<'s, C, D, V>) -> Self {
        use ServerImpl::*;

        match server {
            Follower(_) => Self::Follower,
            Candidate(_) => Self::Candidate,
            Leader(_) => Self::Leader,
        }
    }
}
