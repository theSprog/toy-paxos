use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::{seq_num::SequenceNumber, ValueType};

#[derive(Debug)]
pub struct Proposal {
    pub(crate) seq: SequenceNumber,
    pub(crate) value: Option<ValueType>, // 我已经 accept 过的值
    pub(crate) want_value: ValueType,    // 我想要设定的值
    // pub(crate) highest_seq: Option<SequenceNumber>,
    pub(crate) prepared: HashSet<usize>,
    pub(crate) accepted: HashSet<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct AcceptedProposal {
    pub(crate) seq: SequenceNumber,
    pub(crate) val: ValueType,
}

impl AcceptedProposal {
    pub fn new(seq: SequenceNumber, val: ValueType) -> Self {
        Self { seq, val }
    }
}

#[derive(Debug)]
pub struct Incoming {
    pub src: usize,      // 来源
    pub dgram: Datagram, // 报文数据
}

#[derive(Debug)]
pub struct Outgoing {
    pub dst: HashSet<usize>, // 目的地
    pub dgram: Datagram,     // 报文数据
}

// 报文数据分为两类
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Datagram {
    Request(Request),   // 请求类
    Response(Response), // 响应类
}

impl Datagram {
    pub fn encode_with_src(&self, src: usize) -> Bytes {
        const N: usize = std::mem::size_of::<usize>();

        let data = bincode::serialize(&self).unwrap();
        let mut buf = BytesMut::with_capacity(2 * N + data.len());

        buf.put_uint_be(src as u64, N);
        buf.put_uint_be(data.len() as u64, N);
        buf.put(data);
        buf.freeze()
    }
}

/*
请求有四种请求：
    1. propose: 提出设定值
    2. prepare: 询问众人，查询是否已被设定值
    3. accept: 请求众人将值设定为 value
    4. learn: 请求学习设定好的值

*/

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Propose {
        value: ValueType,
    },
    Prepare {
        seq: SequenceNumber,
    },
    Accept {
        seq: SequenceNumber,
        value: ValueType,
    },
    Learn {
        value: ValueType,
    },
    Query,
}

/*
响应有三种：
    1. prepare: 没有设定值，或者已经有设定值
    2. accept: 接受值成功
    3. query: 查询响应，要么没有值，要么有设定值
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Prepare(Option<AcceptedProposal>),
    Accepted { seq: SequenceNumber },
    Query { val: Option<ValueType> },
}
