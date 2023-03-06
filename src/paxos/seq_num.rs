use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceNumber {
    time_stamp: u128,
    server_id: usize,
}

impl PartialOrd for SequenceNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.time_stamp == other.time_stamp {
            Some(self.server_id.cmp(&other.server_id))
        } else {
            Some(self.time_stamp.cmp(&other.time_stamp))
        }
    }
}

impl Ord for SequenceNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.time_stamp == other.time_stamp {
            self.server_id.cmp(&other.server_id)
        } else {
            self.time_stamp.cmp(&other.time_stamp)
        }
    }
}

impl SequenceNumber {
    pub(crate) fn new(server_id: usize, time_stamp: u128) -> Self {
        Self {
            time_stamp,
            server_id,
        }
    }
}
