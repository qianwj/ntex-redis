
mod xinfo;

use std::convert::TryFrom;
pub use xinfo::{XInfoType, XInfo};

pub struct Record<T>
  where T: TryFrom<T> {

  msg_id: String,
  body: T,
}



