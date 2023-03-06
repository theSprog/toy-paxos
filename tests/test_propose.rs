use std::{thread, time::Duration};

use paxos::shell::Console;
use rand::{seq::SliceRandom, thread_rng};

#[test]
fn test() {
    loop {
        let mut console = Console::new();
        let mut vec: Vec<i32> = (1..21).collect();
        vec.shuffle(&mut thread_rng());
        console.start_servers(20, 9527);
        for i in vec {
            console.propose(i as usize, i as u32);
        }
        thread::sleep(Duration::from_millis(100));
        for i in 0..21 {
            console.query(i);
        }
        console.exit()
    }
}
