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
                // 如果是没有给过承诺，或者新 prepare 请求 ID 更大
                if self.last_promised.is_none() || self.last_promised.unwrap() <= seq {
                    self.last_promised = Some(seq);
                    // 将最后接受的值返回给它。
                    let resp = Response::Prepare(self.last_accepted_proposal);
                    self.tx
                        .unbounded_send(Outgoing {
                            // 只返回给它一个结点
                            dst: (src..src + 1).collect(),
                            dgram: Datagram::Response(resp),
                        })
                        .unwrap();
                } else {
                    // 否则的话忽略请求
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
                    // 回应已接受
                    let resp = Response::Accepted { seq };
                    self.tx
                        .unbounded_send(Outgoing {
                            dst: (src..src + 1).collect(),
                            dgram: Datagram::Response(resp),
                        })
                        .unwrap();
                } else {
                    log!(
                        "Server#{} ignore req `{:?}` from #{}",
                        self.self_id,
                        req,
                        src
                    );
                }
            }
            Request::Learn { value } => {
                if let Some(chosen_value) = self.chosen {
                    assert!(chosen_value == value);
                }
                self.chosen = Some(value);
                log!("Server#{} learned {}", self.self_id, self.chosen.unwrap());
            }
            Request::Propose { value } => {
                if let Some(proposal) = &self.proposal {
                    if proposal.wanted_value == value {
                        log!("Retry to propose `{}`", value);
                    } else {
                        log!(
                            "Override a existed proposal value `{}`.",
                            proposal.wanted_value
                        );
                    }
                }
                let seq = self.next_seq();
                self.proposal = Some(Proposal {
                    seq,
                    value: None,
                    wanted_value: value,
                    highest_seq: None,
                    prepared: HashSet::new(),
                    accepted: HashSet::new(),
                });
                let req = Request::Prepare { seq };
                self.tx
                    .unbounded_send(Outgoing {
                        dst: self.peers_id.clone(),
                        dgram: Datagram::Request(req),
                    })
                    .unwrap();
            }
            Request::Query => {
                let resp = Response::Query { val: self.chosen };
                self.tx
                    .unbounded_send(Outgoing {
                        dst: (src..src + 1).collect(),
                        dgram: Datagram::Response(resp),
                    })
                    .unwrap();
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
                if let Some(ref mut proposal) = self.proposal {
                    proposal.prepared.insert(src);
                    if let Some(AcceptedProposal { seq, val }) = accepted_proposal {
                        if proposal.prepared.len() <= self.peers_id.len() / 2 + 1
                            && seq >= *proposal.highest_seq.get_or_insert(seq)
                        {
                            proposal.value = Some(val);
                        }
                    }
                    if proposal.prepared.len() == self.peers_id.len() / 2 + 1 {
                        let req = Request::Accept {
                            seq: proposal.seq,
                            value: *proposal.value.get_or_insert(proposal.wanted_value),
                        };
                        self.tx
                            .unbounded_send(Outgoing {
                                dst: proposal.prepared.clone(),
                                dgram: Datagram::Request(req),
                            })
                            .unwrap();
                    }
                } else {
                    panic!("recv a resp for prepare, but no proposal presented");
                }
            }
            Response::Accepted { seq } => {
                // log!("handle accept resp seq: {}", seq);
                if let Some(ref mut proposal) = self.proposal {
                    if seq == proposal.seq {
                        proposal.accepted.insert(src);
                        if proposal.accepted.len() == 1 + self.peers_id.len() / 2 {
                            assert!(proposal.value.is_some());
                            let value = proposal.value.unwrap();
                            if value == proposal.wanted_value {
                                log!("proposal value `{}` success.", value);
                            } else {
                                log!(
                                    "proposal value `{}` fail, `{}` is chosen.",
                                    proposal.wanted_value,
                                    value
                                );
                            }
                            let req = Request::Learn {
                                value: proposal.value.unwrap(),
                            };
                            log!("value accepted by majority: {}", value);
                            self.tx
                                .unbounded_send(Outgoing {
                                    dst: self.peers_id.clone(),
                                    dgram: Datagram::Request(req),
                                })
                                .unwrap();
                        }
                    }
                } else {
                    panic!("recv an accepted response, but no proposal presented")
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
}
