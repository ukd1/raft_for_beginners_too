use tracing::trace;

pub struct DummyConnection;

#[async_trait]
impl Connection for DummyConnection {
    async fn bind(bind_socket: ServerAddress) -> Result<Self, ConnectionError> {
        trace!(?bind_socket);
        Ok(Self) // TODO
    }
    async fn send(&self, packet: Packet) -> Result<(), ConnectionError> {
        trace!(?packet);
        Ok(()) // TODO
    }

    async fn receive(&self) -> Result<Packet, ConnectionError> {
        trace!("receive");
        Ok(Packet {
            data: Vec::new(),
            peer: ServerAddress("0.0.0.0:0".parse().unwrap()),
        }) // TODO
    }
}
