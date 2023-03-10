use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::stream::StreamExt;

use crate::paxos::proposal::{Datagram, Incoming, Outgoing};
use crate::paxos::*;

#[derive(Debug)]
pub struct Proxy {
    local_id: usize,
    id2addr: HashMap<usize, SocketAddr>,
}

impl Proxy {
    pub fn new(local_id: usize, id2addr: HashMap<usize, SocketAddr>) -> Arc<Self> {
        let proxy = Self { local_id, id2addr };
        Arc::new(proxy)
    }

    pub async fn run(
        self: Arc<Self>,
        tx: Tx<Incoming>,
        rx: Rx<Outgoing>,
    ) -> Result<(), tokio::io::Error> {
        let mut listener = TcpListener::bind(self.id2addr[&self.local_id]).await?;
        tokio::spawn(self.clone().serve_outflow(rx));
        while let Some(socket) = listener.incoming().next().await {
            tokio::spawn(Self::serve_inflow(socket?, tx.clone()));
        }
        Ok(())
    }

    pub async fn read_incoming(
        socket: &mut TcpStream,
    ) -> Result<(usize, Datagram), tokio::io::Error> {
        let mut buf = vec![0u8; 512];
        let src = socket.read_u64().await?;
        let src = src as usize;
        let len = socket.read_u64().await? as usize;
        socket.read_exact(&mut buf[..len]).await?;
        let decoded: Datagram = bincode::deserialize(&buf[..len]).unwrap();
        Ok((src, decoded))
    }

    async fn serve_inflow(mut socket: TcpStream, tx: Tx<Incoming>) {
        while let Ok((src, dgram)) = Self::read_incoming(&mut socket).await {
            tx.unbounded_send(Incoming { src, dgram }).unwrap();
        }
    }

    async fn serve_outflow(self: Arc<Self>, mut rx: Rx<Outgoing>) {
        while let Some(Outgoing { dst, dgram }) = rx.next().await {
            dst.iter().for_each(|id| {
                let addr = self.id2addr[id];
                let dgram = dgram.clone();
                let local_id = self.local_id;
                let send_task = async move {
                    let mut stream = TcpStream::connect(addr).await.unwrap();
                    let buf = dgram.encode_with_src(local_id);
                    stream.write_all(&buf).await.unwrap();
                };
                tokio::spawn(send_task);
            });
        }
    }
}
