use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::stream::StreamExt;

use super::proposal::*;
use super::seq_num::SequenceNumber;
use super::ValueType;
use super::{Rx, Tx};

macro_rules! log {
    ($($tokens: tt)*) => {
        {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            writeln!(handle, $($tokens)*).unwrap();
            handle.flush().unwrap();
            // println!($($tokens)*);
        }
    }
}

#[derive(Debug)]
pub struct Node {
    self_id: usize,
    peers_id: HashSet<usize>,
    proposal: Option<Proposal>,

    last_promised: Option<SequenceNumber>,
    last_accepted_proposal: Option<AcceptedProposal>,

    chosen: Option<ValueType>,
    tx: Tx<Outgoing>,
    rx: Rx<Incoming>,
}

impl Node {
    pub fn new(
        self_id: usize,
        peers_id: HashSet<usize>,
        tx: Tx<Outgoing>,
        rx: Rx<Incoming>,
    ) -> Self {
        // log!("Paxos start with peers_num: {:?}", peers_id);
        Self {
            self_id,
            last_promised: None,
            chosen: None,
            last_accepted_proposal: None,
            peers_id,
            proposal: None,
            tx,
            rx,
        }
    }

    pub async fn run(mut self) {
        while let Some(incoming) = self.rx.next().await {
            self.handle_incoming(incoming);
        }
    }

    fn next_seq(&mut self) -> SequenceNumber {
        SequenceNumber::new(
            self.self_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
    }

    fn handle_incoming(&mut self, incoming: Incoming) {
        let Incoming { src, dgram } = incoming;
        match dgram {
            Datagram::Request(req) => self.handle_request(src, req),
            Datagram::Response(resp) => self.handle_response(src, resp),
        }
    }

    fn handle_request(&mut self, src: usize, req: Request) {
        log!(
            "Server #{} handle req  from #{}: {:?}",
            self.self_id,
            src,
            req
        );
        match req {
            Request::Prepare { seq } => {
                // ??????????????????????????????????????? prepare ?????? ID ??????
                if self.last_promised.is_none() || self.last_promised.unwrap() <= seq {
                    self.last_promised = Some(seq);
                    // ????????????????????????????????????
                    let resp = Response::Prepare(self.last_accepted_proposal);
                    self.unicast(src, Datagram::Response(resp));
                } else {
                    // ????????????????????????
                    log!(
                        "Server#{} ignore low-seq req `{:?}` from #{}",
                        self.self_id,
                        req,
                        src
                    );
                }
            }
            Request::Accept { seq, value } => {
                if self.last_promised.is_none() || self.last_promised.unwrap() <= seq {
                    self.last_accepted_proposal = Some(AcceptedProposal::new(seq, value));
                    // ??????????????????accepted???
                    let resp = Response::Accepted { seq };
                    self.unicast(src, Datagram::Response(resp));
                } else {
                    log!(
                        "Server#{} ignore req `{:?}` from #{}",
                        self.self_id,
                        req,
                        src
                    );
                }
            }
            // ????????????????????? value
            Request::Learn { value } => {
                // ????????????????????????????????????????????????
                if let Some(chosen_value) = self.chosen {
                    assert!(chosen_value == value);
                } else {
                    // ??????????????????
                    self.chosen = Some(value);
                }
                log!("Server #{} learned {}", self.self_id, self.chosen.unwrap());
            }
            Request::Propose { value } => {
                let seq = self.next_seq();
                if self.chosen.is_none() {
                    // ??????????????????
                    self.proposal = Some(Proposal {
                        seq,
                        value: None,
                        want_value: value,
                        prepared: HashSet::new(),
                        accepted: HashSet::new(),
                    });

                    // ????????? prepare ?????????????????????
                    let req = Request::Prepare { seq };
                    self.boardcast(Datagram::Request(req));
                } else {
                    // ???????????????????????????????????? Propose ???
                    if Some(value) != self.chosen {
                        log!(
                            "proposal value `{}` fail, `{}` is chosen.",
                            value,
                            self.chosen.unwrap()
                        );
                    } else {
                        log!("proposal value `{}` is existed", value);
                    }
                }
            }
            Request::Query => {
                let resp = Response::Query { val: self.chosen };
                self.unicast(src, Datagram::Response(resp));
            }
        }
    }

    fn handle_response(&mut self, src: usize, resp: Response) {
        log!(
            "Server #{} handle resp from #{}: {:?}",
            self.self_id,
            src,
            resp
        );
        match resp {
            Response::Prepare(accepted_proposal) => {
                // ????????????????????????
                if let Some(ref mut my_proposal) = self.proposal {
                    // ?????????????????????????????????
                    if let Some(AcceptedProposal { seq, val }) = accepted_proposal {
                        assert!(my_proposal.seq >= seq);

                        // ??????????????????????????????
                        let req = Request::Accept {
                            seq: my_proposal.seq,
                            value: val,
                        };
                        // ?????????????????????????????????????????? node ???????????? accept
                        self.boardcast(Datagram::Request(req));
                    } else {
                        // ????????????????????????????????????
                        my_proposal.prepared.insert(src);

                        // Prepare ??????????????????
                        if my_proposal.prepared.len() >= self.peers_id.len() / 2 + 1 {
                            // ?????????????????? Accept ??????
                            let req = Request::Accept {
                                seq: my_proposal.seq,
                                value: *my_proposal.value.get_or_insert(my_proposal.want_value),
                            };
                            self.boardcast(Datagram::Request(req));
                        }
                    }
                } else {
                    // ?????????????????????????????????
                    panic!("Why there is no proposal for me?");
                }
            }
            Response::Accepted { seq } => {
                // ????????????????????????????????????????????????????????????????????????
                if let Some(ref mut my_proposal) = self.proposal {
                    assert!(seq == my_proposal.seq);

                    // ??????????????????
                    my_proposal.accepted.insert(src);

                    // ?????????????????????
                    if my_proposal.accepted.len() == self.peers_id.len() / 2 + 1 {
                        my_proposal.value = Some(my_proposal.want_value);
                        let value = my_proposal.value.unwrap();
                        log!("value accepted by majority: {}", value);

                        let req = Request::Learn { value };
                        self.boardcast(Datagram::Request(req));
                    }
                } else {
                    panic!("recv an accepted response, but not my proposal !!!");
                }
            }
            Response::Query { val } => {
                if let Some(val) = val {
                    log!("Server #{} Answer: {}.", src, val);
                } else {
                    log!("Server #{} Answer: not value learned yet.", src);
                }
            }
        }
    }

    pub(crate) fn boardcast(&self, msg: Datagram) {
        self.tx
            .unbounded_send(Outgoing {
                dst: self.peers_id.clone(),
                dgram: msg,
            })
            .unwrap();
    }

    pub(crate) fn unicast(&self, src: usize, msg: Datagram) {
        self.tx
            .unbounded_send(Outgoing {
                dst: (src..src + 1).collect(),
                dgram: msg,
            })
            .unwrap();
    }
}
