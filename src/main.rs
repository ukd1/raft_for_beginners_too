mod connection;
mod raft;

use raft::{Result, Server};

#[tokio::main]
async fn main() -> Result<()> {
    let server_handle = Server::run();
    server_handle.await?
}
