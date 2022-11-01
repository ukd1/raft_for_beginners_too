use std::marker::PhantomData;

use async_trait::async_trait;
use tokio::net::UdpSocket;
use tracing::trace;

use super::*;

#[derive(Debug)]
pub struct UdpConnection<D, V>
where
    D: Journalable,
    V: Journalable,
{
    socket: UdpSocket,
    _send: PhantomData<fn(Packet<D, V>)>,
    _receive: PhantomData<fn() -> Packet<D, V>>,
}

#[async_trait]
impl<D, V> Connection<D, V> for UdpConnection<D, V>
where
    D: Journalable,
    V: Journalable,
{
    async fn bind(bind_socket: ServerAddress) -> Result<Self, ConnectionError> {
        trace!(?bind_socket);
        let socket = UdpSocket::bind(bind_socket.0).await?;
        Ok(Self {
            socket,
            _send: Default::default(),
            _receive: Default::default(),
        })
    }

    async fn send(&self, packet: Packet<D, V>) -> Result<(), ConnectionError> {
        trace!(?packet, "send");
        let data = rmp_serde::to_vec(&packet)
            .map_err(Box::from)
            .map_err(ConnectionError::Encoding)?;
        self.socket.send_to(&data, packet.peer.0).await?;
        Ok(())
    }

    async fn receive(&self) -> Result<Packet<D, V>, ConnectionError> {
        let mut buf = vec![0; 65536];
        let (bytes_received, peer_addr) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(bytes_received);

        let mut packet: Packet<D, V> = rmp_serde::from_slice(&buf)
            .map_err(Box::from)
            .map_err(ConnectionError::Decoding)?;
        trace!(?peer_addr, ?packet, "receive");

        // Change the peer field to be the received peer's, not the one it sent
        packet.peer = peer_addr.into();
        Ok(packet)
    }

    fn address(&self) -> ServerAddress {
        self.socket
            .local_addr()
            .expect("should always be bound")
            .into()
    }
}
