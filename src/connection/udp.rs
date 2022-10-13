use async_trait::async_trait;
use tracing::trace;
use tokio::net::UdpSocket;

use super::*;

pub struct UdpConnection {
    socket: UdpSocket,
}

#[async_trait]
impl Connection for UdpConnection {
    async fn bind(bind_socket: ServerAddress) -> Result<Self, ConnectionError> {
        trace!(?bind_socket);
        let socket = UdpSocket::bind(bind_socket.0).await
            .expect("TODO: handle error");
        Ok(Self {
            socket,
        }) // TODO
    }

    async fn send(&self, packet: Packet) -> Result<(), ConnectionError> {
        trace!(?packet, "send");
        let data = rmp_serde::to_vec(&packet).expect("serialization failed");
        self.socket.send_to(&data, packet.peer.0).await
            .expect("TODO: handle error");
        Ok(()) // TODO
    }

    async fn receive(&self) -> Result<Packet, ConnectionError> {
        let mut buf = vec![0; 65536];
        let (bytes_received, peer_addr) = self.socket.recv_from(&mut buf).await
            .expect("TODO: handle error");
        buf.truncate(bytes_received);
        trace!(?peer_addr, bytes_received, "receive"); // DEBUG

        let mut packet: Packet = rmp_serde::from_slice(&buf).expect("deserialization failed");
        // Change the peer field to be the received peer's, not the one it sent
        // TODO: this is wrong and bad to tamper with wire data
        packet.peer = peer_addr.into();
        Ok(packet)
    }

    fn address(&self) -> ServerAddress {
        self.socket.local_addr().expect("should always be bound").into()
    }
}
