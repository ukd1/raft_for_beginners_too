pub mod udp;

use std::{net::SocketAddr, error::Error};

use async_trait::async_trait;
use derive_more::{From, FromStr};
use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::journal::{JournalEntry, JournalValue};
#[derive(Serialize, Deserialize, Debug)]
pub struct Packet<V: JournalValue> {
    #[serde(bound = "V: DeserializeOwned")]
    pub message_type: PacketType<V>,
    pub peer: ServerAddress,
    pub term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum PacketType<V: JournalValue> {
    VoteRequest {
        last_log_index: Option<u64>,
        last_log_term: u64,
    },
    VoteResponse {
        is_granted: bool,
    },
    AppendEntries {
        prev_log_index: Option<u64>,
        prev_log_term: u64,
        #[serde(bound = "V: DeserializeOwned")]
        entries: Vec<JournalEntry<V>>,
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
    Io(#[from] std::io::Error),
    #[error("Error decoding packet")]
    Decoding(Box<dyn Error + Send + Sync + 'static>),
    #[error("Error encoding packet")]
    Encoding(Box<dyn Error + Send + Sync + 'static>),
}

#[async_trait]
pub trait Connection<V: JournalValue>: std::fmt::Debug + Sized + Send + Sync + 'static {
    async fn bind(bind_socket: ServerAddress) -> Result<Self, ConnectionError>;
    async fn send(&self, packet: Packet<V>) -> Result<(), ConnectionError>;
    async fn receive(&self) -> Result<Packet<V>, ConnectionError>;
    fn address(&self) -> ServerAddress;
}
