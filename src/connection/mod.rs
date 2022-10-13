pub mod udp;

use std::net::SocketAddr;

use async_trait::async_trait;
use derive_more::{From, FromStr};
use serde::{Serialize, Deserialize};

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
    AppendEntries,
    AppendEntriesAck {
        did_append: bool,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, From, FromStr, Hash, PartialEq, Eq)]
pub struct ServerAddress(pub SocketAddr); // TODO: make more generic?

#[derive(Debug)]
pub struct ConnectionError; // TODO

#[async_trait]
pub trait Connection: Sized + Send + Sync + 'static {
    async fn bind(bind_socket: ServerAddress) -> Result<Self, ConnectionError>;
    async fn send(&self, packet: Packet) -> Result<(), ConnectionError>;
    async fn receive(&self) -> Result<Packet, ConnectionError>;
    fn address(&self) -> ServerAddress;
}
