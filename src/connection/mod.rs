pub mod udp;

use std::{net::SocketAddr, error::Error};

use async_trait::async_trait;
use derive_more::{From, FromStr};
use serde::{Serialize, Deserialize};

use crate::journal::JournalEntry;
#[derive(Serialize, Deserialize, Debug)]
pub struct Packet {
    pub message_type: PacketType,
    pub peer: ServerAddress,
    pub term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum PacketType {
    VoteRequest {
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteResponse {
        is_granted: bool,
    },
    AppendEntries {
        prev_log_index: Option<u64>,
        prev_log_term: u64,
        entries: Vec<JournalEntry>,
        leader_commit: u64,
    },
    AppendEntriesAck {
        did_append: bool,
        match_index: Option<u64>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, From, FromStr, Hash, PartialEq, Eq)]
pub struct ServerAddress(pub SocketAddr); // TODO: make more generic?

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Error decoding packet")]
    DecodingError(Box<dyn Error + Send + Sync + 'static>),
    #[error("Error encoding packet")]
    EncodingError(Box<dyn Error + Send + Sync + 'static>),
}

#[async_trait]
pub trait Connection: std::fmt::Debug + Sized + Send + Sync + 'static {
    async fn bind(bind_socket: ServerAddress) -> Result<Self, ConnectionError>;
    async fn send(&self, packet: Packet) -> Result<(), ConnectionError>;
    async fn receive(&self) -> Result<Packet, ConnectionError>;
    fn address(&self) -> ServerAddress;
}
