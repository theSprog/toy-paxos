use futures::channel::mpsc;

pub mod node;
pub mod proposal;
pub mod seq_num;

pub type ValueType = u32;
pub type Tx<T> = mpsc::UnboundedSender<T>;
pub type Rx<T> = mpsc::UnboundedReceiver<T>;