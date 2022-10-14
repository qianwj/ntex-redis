use std::convert::TryFrom;
use std::collections::HashMap;
use ntex::util::ByteString;
use crate::cmd::Command;
use crate::codec::{Request, Response};
use crate::errors::CommandError;

pub fn XInfo(key: String, info_type: XInfoType) -> XInfoCommand {
  XInfoCommand::new(key, info_type)
}

pub struct XInfoCommand {
  stream: String,
  full: bool,
  info_type: XInfoType,
  requests: Vec<Request>
}

impl XInfoCommand {
  fn new(stream: String, info_type: XInfoType) -> Self {
    XInfoCommand {
      stream,
      full: false,
      info_type,
      requests: vec![Request::from_static("XINFO")]
    }
  }

  pub fn full(mut self) -> Self {
    self.full = true;
    self
  }

  fn parse_response(items: Vec<Response>) -> Result<XInfoItem, CommandError> {
    let first: &Response = items.first().unwrap();
    match first {
      Response::Bytes(_) => Self::parse_stream_response(items),
      Response::Array(_) => Self::parse_groups_response(Response::Array(items)),
      _ => Err(CommandError::Output("Unknown Stream Info Key", first.clone()))
    }
  }

  fn parse_stream_response(items: Vec<Response>) -> Result<XInfoItem, CommandError> {
    let data = parse_response_to_map(items)?;
    let length = i64::try_from(data.get("length").unwrap().clone())?;
    let last_generated_id = ByteString::try_from(data.get("last-generated-id").unwrap().clone())?.to_string();
    let max_deleted_entry_id = data.get("max-deleted-entry-id")
      .map(|id| ByteString::try_from(id.clone()).ok().map(|v| v.to_string()))
      .map_or(None, |v| v);
    let recorded_first_entry_id = data.get("recorded-first-entry-id")
      .map(|id| ByteString::try_from(id.clone()).ok().map(|v| v.to_string()))
      .map_or(None, |v| v);
    let entries_added = i64::try_from(data.get("entries-added").unwrap().clone())?;
    let groups = StreamGroupsInfo::try_from(data.get("groups").unwrap().clone())?;
    let entries_info = match data.get("entries") {
      Some(v) => StreamEntriesInfo::Full(Vec::try_from(v.clone())?),
      None => StreamEntriesInfo::Simple {
        first: RawRecord::try_from(data.get("first-entry").unwrap().clone())?,
        last: RawRecord::try_from(data.get("last-entry").unwrap().clone())?
      }
    };
    let radix_tree = StreamRadixTreeInfo::try_from(data)?;
    Ok(XInfoItem::Stream(XInfoStream {
      length,
      radix_tree,
      last_generated_id,
      max_deleted_entry_id,
      groups,
      entries_added,
      recorded_first_entry_id,
      entries_info,
    }))
  }

  fn parse_groups_response(resp: Response) -> Result<XInfoItem, CommandError> {
    let data = Vec::try_from(resp)?;
    Ok(XInfoItem::Groups(data))
  }
}

impl Command for XInfoCommand {
  type Output = XInfoItem;

  fn to_request(mut self) -> Request {
    match self.info_type {
      XInfoType::Stream(count) => {
        self.requests.push(Request::from_static("STREAM"));
        self.requests.push(Request::from(self.stream));
        if self.full {
          self.requests.push(Request::from_static("FULL"));
        }
        if let Some(entries_count) = count {
          self.requests.push(Request::from(entries_count));
        }
      }
      XInfoType::Groups => {
        self.requests.push(Request::from_static("GROUPS"));
        self.requests.push(Request::from(self.stream));
      }
      XInfoType::Consumers(group) => {
        self.requests.push(Request::from_static("CONSUMERS"));
        self.requests.push(Request::from(self.stream));
        self.requests.push(Request::from(group));
      }
    };
    Request::Array(self.requests)
  }

  fn to_output(val: Response) -> Result<Self::Output, CommandError> {
    match val {
      Response::Array(items) => XInfoCommand::parse_response(items),
      _ => Err(CommandError::Output("Unexpected Value", val))
    }
  }
}

pub enum XInfoType {
  Stream(Option<usize>), Groups, Consumers(String)
}

#[derive(Debug)]
pub enum XInfoItem {
  Stream(XInfoStream),
  Groups(Vec<XInfoGroup>),
  Consumers(Vec<XInfoConsumer>),
}

#[derive(Debug)]
pub struct XInfoStream {
  length: i64,
  radix_tree: StreamRadixTreeInfo,
  groups: StreamGroupsInfo,
  last_generated_id: String,
  max_deleted_entry_id: Option<String>,
  entries_added: i64,
  recorded_first_entry_id: Option<String>,
  entries_info: StreamEntriesInfo,
}

#[derive(Debug)]
pub struct StreamRadixTreeInfo {
  keys: i64,
  nodes: i64,
}

impl StreamRadixTreeInfo {
  fn try_from(src: HashMap<String, Response>) -> Result<Self, CommandError> {
    let keys = i64::try_from(src.get("radix-tree-keys").unwrap().clone())?;
    let nodes = i64::try_from(src.get("radix-tree-nodes").unwrap().clone())?;
    Ok(StreamRadixTreeInfo { keys, nodes })
  }
}

#[derive(Debug)]
pub enum StreamGroupsInfo {
  Simple(i64),
  Full(Vec<StreamGroupInfo>)
}

impl TryFrom<Response> for StreamGroupsInfo {
  type Error = (&'static str, Response);

  fn try_from(resp: Response) -> Result<Self, Self::Error> {
    match resp {
      Response::Integer(count) => Ok(Self::Simple(count)),
      Response::Array(_) => {
        let data = Vec::try_from(resp.clone())?;
        Ok(Self::Full(data))
      }
      _ => Err(("Unexpected Value Type, Want i64 or array", resp))
    }
  }
}

#[derive(Debug)]
pub struct StreamGroupInfo {
  name: String,
  last_delivered_id: String,
  entries_read: Option<i64>,
  lag: i64,
  pel_count: i64,
  consumers: Vec<StreamGroupConsumerInfo>
}

impl TryFrom<Response> for StreamGroupInfo {
  type Error = (&'static str, Response);

  fn try_from(resp: Response) -> Result<StreamGroupInfo, Self::Error> {
    match resp {
      Response::Array(items) => {
        let data = parse_response_to_map(items)?;
        let name = ByteString::try_from(data.get("name").unwrap().clone())?.to_string();
        let last_delivered_id = ByteString::try_from(data.get("last-delivered-id").unwrap().clone())?.to_string();
        let entries_read = data.get("entries-read").map(|v| {
          match v.clone() {
            Response::Integer(v) => Some(v),
            _ => None,
          }
        }).map_or(None, |v| v);
        let lag = i64::try_from(data.get("lag").unwrap().clone())?;
        let pel_count = i64::try_from(data.get("pel-count").unwrap().clone())?;
        let consumers: Vec<StreamGroupConsumerInfo> = Vec::try_from(data.get("consumers").unwrap().clone())?;
        Ok(StreamGroupInfo {
          name,
          last_delivered_id,
          entries_read,
          lag,
          pel_count,
          consumers
        })
      }
      _ => Err(("Unexpected Value Type, Want array", resp.clone()))
    }
  }
}

#[derive(Debug)]
pub struct StreamGroupConsumerInfo {
  name: String,
  seen_time: i64,
  pel_count: i64,
}

impl TryFrom<Response> for StreamGroupConsumerInfo {
  type Error = (&'static str, Response);

  fn try_from(resp: Response) -> Result<Self, Self::Error> {
    if let Response::Array(items) = resp {
      let data = parse_response_to_map(items)?;
      Ok(StreamGroupConsumerInfo {
        name: ByteString::try_from(data.get("name").unwrap().clone())?.to_string(),
        seen_time: i64::try_from(data.get("seen-time").unwrap().clone())?,
        pel_count: i64::try_from(data.get("pel-count").unwrap().clone())?,
      })
    } else {
      Err(("Unexpected Value Type, Want array", resp.clone()))
    }
  }
}

#[derive(Debug)]
pub enum StreamEntriesInfo {
  Simple {
    first: RawRecord,
    last: RawRecord,
  },
  Full(Vec<RawRecord>)
}

#[derive(Debug)]
pub struct XInfoGroup {
  name: String,
  consumers: i64,
  pending: i64,
  last_delivered_id: String,
  lag: i64,
  entries_read: Option<i64>,
}

impl TryFrom<Response> for XInfoGroup {
  type Error = (&'static str, Response);

  fn try_from(resp: Response) -> Result<Self, Self::Error> {
    match resp {
      Response::Array(items) => {
        let data = parse_response_to_map(items)?;
        let name = ByteString::try_from(data.get("name").unwrap().clone())?.to_string();
        let consumers = i64::try_from(data.get("consumers").unwrap().clone())?;
        let pending = i64::try_from(data.get("pending").unwrap().clone())?;
        let last_delivered_id = ByteString::try_from(data.get("last-delivered-id").unwrap().clone())?.to_string();
        let lag = i64::try_from(data.get("lag").unwrap().clone())?;
        let entries_read = data.get("entries-read").map(|v| {
          match v {
            Response::Integer(v) => Some(v.clone()),
            _ => None,
          }
        }).map_or(None, |v| v);
        Ok(XInfoGroup { name, consumers, pending, last_delivered_id, lag, entries_read })
      }
      _ => Err(("Unexpected Value Type, Want array", resp.clone()))
    }
  }
}

#[derive(Debug)]
pub struct XInfoConsumer {
  name: String,
  pending: usize,
  idle: usize,
}

#[derive(Debug)]
pub struct RawRecord {
  msg_id: String,
  body: HashMap<ByteString, ByteString>
}

impl TryFrom<Response> for RawRecord {
  type Error = (&'static str, Response);

  fn try_from(resp: Response) -> Result<Self, Self::Error> {
    if let Response::Array(data) = resp {
      let msg_id = ByteString::try_from(data.first().unwrap().clone())?.to_string();
      let body = HashMap::try_from(data.get(1).unwrap().clone())?;
      return Ok(RawRecord { msg_id, body })
    }
    Err(("Unexpected Value Type, Want array", resp.clone()))
  }
}

fn parse_response_to_map(items: Vec<Response>) -> Result<HashMap<String, Response>, (&'static str, Response)> {
  let mut result = HashMap::new();
  let mut key = String::new();
  for (i, resp) in items.iter().enumerate() {
    if (i % 2) == 0 {
      key = ByteString::try_from(resp.clone())?.to_string();
    } else {
      result.insert(key.clone(), resp.clone());
    }
  };
  Ok(result)
}
