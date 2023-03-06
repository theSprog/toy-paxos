use futures::channel::mpsc;
use std::collections::HashMap;
use std::io::BufRead;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;

use crate::net_proxy::Proxy;
use crate::paxos::node::Node;
use crate::paxos::proposal::{Datagram, Request};
use crate::paxos::ValueType;

macro_rules! print_flushed {
    ($($tokens: tt)*) => {
        {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            write!(handle, $($tokens)*).unwrap();
            handle.flush().unwrap();
        }
    }
}

macro_rules! println_flushed {
    ($($tokens: tt)*) => {
        {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            writeln!(handle, $($tokens)*).unwrap();
            handle.flush().unwrap();
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Start(usize),
    Propose(usize, ValueType),
    Query(usize),
    Exit,
}

#[derive(Debug, PartialEq)]
pub struct ParseCommandError;

impl FromStr for Command {
    type Err = ParseCommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        let tokens: Vec<&str> = lower.split_whitespace().collect();
        Ok(match tokens[..] {
            ["s" | "start", num] => Self::Start(num.parse().unwrap()),
            ["p" | "propose", id, val] => Self::Propose(id.parse().unwrap(), val.parse().unwrap()),
            ["q" | "query", id] => Self::Query(id.parse().unwrap()),
            ["x" | "exit"] => Self::Exit,

            _ => return Err(ParseCommandError),
        })
    }
}

pub struct Console {
    rt: tokio::runtime::Runtime,
    addr_table: Option<Arc<HashMap<usize, SocketAddr>>>,
}

impl Console {
    pub fn new() -> Self {
        Self {
            rt: tokio::runtime::Runtime::new().unwrap(),
            addr_table: None,
        }
    }

    pub fn run(mut self) {
        let stdin = std::io::stdin();
        let handle = stdin.lock();
        print_flushed!("Paxos> ");
        for line in handle.lines() {
            // 解析命令
            if let Ok(line) = line {
                if let Ok(cmd) = line.parse() {
                    match cmd {
                        // 启动 num 个服务器
                        Command::Start(num) => self.start_servers(num, 9527),
                        // server_id 号服务器提交值 val
                        Command::Propose(server_id, val) => self.propose(server_id, val),
                        // 查询 server_id 号服务器
                        Command::Query(server_id) => self.query(server_id),
                        Command::Exit => break,
                    }
                } else {
                    println_flushed!("Unknown command.");
                }
            }
            // A slight pause waiting for servers' output.
            // Otherwise the prompt will mess up with them.
            std::thread::sleep(std::time::Duration::from_millis(200));
            print_flushed!("Paxos> ");
        }
    }

    // 从端口 base_port 启动 server_num 个服务器
    pub fn start_servers(&mut self, server_num: usize, base_port: usize) {
        let server_num = server_num + 1; // #0 转为客户端 client.

        // 将 ID 和 addr 绑定起来，建立一种映射关系
        let addr_table: Arc<HashMap<usize, SocketAddr>> = Arc::new(
            ((base_port)..(base_port + server_num))
                .enumerate()
                .map(|(id, port)| (id, format!("127.0.0.1:{}", port).parse().unwrap()))
                .collect(),
        );

        // 为每一个 ID 都建立一条到其他 id 的连接，包括自己到自己
        let start_server = |id: usize| {
            let (itx, irx) = mpsc::unbounded();
            let (otx, orx) = mpsc::unbounded();
            // 跳过客户端 #0
            let node = Node::new(id, (1..server_num).collect(), otx, irx);
            let proxy = Proxy::new(id, (*addr_table).clone());
            self.rt.spawn(proxy.run(itx, orx));
            self.rt.spawn(node.run());
        };
        (0..server_num).for_each(|id| {
            start_server(id);
        });
        self.addr_table = Some(addr_table.clone());
    }

    pub fn propose(&mut self, server_id: usize, val: ValueType) {
        if let Some(addr_table) = &self.addr_table {
            if let Some(addr) = addr_table.get(&server_id) {
                let addr = addr.clone();
                let task = async move {
                    if let Ok(mut stream) = TcpStream::connect(addr).await {
                        let dgram = Datagram::Request(Request::Propose { value: val });
                        stream.write_all(&dgram.encode_with_src(0)).await.unwrap();
                    }
                };
                self.rt.block_on(task);
            } else {
                println_flushed!("error: server id dosen't exist.");
            }
        } else {
            println_flushed!("error: servers haven't started.");
        }
    }

    pub fn query(&mut self, server_id: usize) {
        if let Some(peers_addr) = &self.addr_table {
            if let Some(addr) = peers_addr.get(&server_id) {
                let addr = addr.clone();
                let task = async move {
                    if let Ok(mut stream) = TcpStream::connect(addr).await {
                        let dgram = Datagram::Request(Request::Query);
                        stream.write_all(&dgram.encode_with_src(0)).await.unwrap();
                    }
                };
                self.rt.block_on(task);
            } else {
                println_flushed!("error: server id dosen't exist.");
            }
        } else {
            println_flushed!("error: servers haven't started.");
        }
    }

    pub fn exit(self) {}
}
