use shell::Console;

mod net_proxy;
mod paxos;
pub mod shell;

fn main() {
    let console = Console::new();
    console.run();
}
