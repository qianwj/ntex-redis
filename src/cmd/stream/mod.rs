
mod xinfo;
mod xgroup;

use std::convert::TryFrom;
pub use xinfo::{XInfoType, XInfo};
pub use xgroup::XGroup;

pub struct Record<T>
  where T: TryFrom<T> {

  msg_id: String,
  body: T,
}



