use std::convert::TryFrom;
use ntex::util::ByteString;
use crate::cmd::Command;
use crate::codec::{Request, Response};
use crate::errors::CommandError;

pub fn XGroup(key: &str, group_name: &str) -> XGroupCommandBuilder {
    XGroupCommand::builder(key.to_string(), group_name.to_string())
}

#[derive(Debug)]
pub struct XGroupCommand(Vec<Request>);

impl XGroupCommand {
    fn new(stream: String, group_name: String, sub_command: XGroupSubCommand) -> Self {
        let mut req = vec![
            Request::from_static("XGROUP"),
            Request::from(stream),
            Request::from(group_name),
        ];
        match sub_command {
            XGroupSubCommand::Create(cmd) => {
                req.insert(1, Request::from_static("CREATE"));
                req.push(Request::from(cmd.id.map_or(String::from("$"), |id| id)));
                if cmd.mkstream {
                    req.push(Request::from_static("MKSTREAM"));
                }
                if cmd.entries_read {
                    req.push(Request::from_static("ENTRIESREAD"));
                }
            }
            XGroupSubCommand::SetId(cmd) => {
                req.insert(1, Request::from_static("SETID"));
                req.push(Request::from(cmd.id.map_or(String::from("$"), |id| id)));
                if cmd.entries_read {
                    req.push(Request::from_static("ENTRIESREAD"));
                }
            }
            XGroupSubCommand::Destroy => req.insert(1, Request::from_static("DESTROY")),
            XGroupSubCommand::CreateConsumer(consumer) => {
                req.insert(1, Request::from_static("CREATECONSUMER"));
                req.push(Request::from(consumer));
            }
            XGroupSubCommand::DeleteConsumer(consumer) => {
                req.insert(1, Request::from_static("DELCONSUMER"));
                req.push(Request::from(consumer));
            }
            _ => {}
        };
        XGroupCommand(req)
    }

    fn builder(stream: String, group_name: String) -> XGroupCommandBuilder {
        XGroupCommandBuilder {
            stream,
            group_name,
            sub_command: XGroupSubCommand::NotSet
        }
    }
}

impl Command for XGroupCommand {
    type Output = ();

    fn to_request(self) -> Request {
        Request::Array(self.0)
    }

    fn to_output(resp: Response) -> Result<Self::Output, CommandError> {
        match resp.clone() {
            Response::Integer(_) => Ok(()),
            Response::String(reply) => {
                if reply.as_str() == "OK" {
                    Ok(())
                } else {
                    Err(CommandError::Error(reply))
                }
            }
            _ => Err(CommandError::Output("Unexpected value type, Want string or integer", resp.clone()))
        }
    }
}

#[derive(Debug)]
pub struct XGroupCommandBuilder {
    stream: String,
    group_name: String,
    sub_command: XGroupSubCommand,
}

impl XGroupCommandBuilder {

    pub fn create(mut self) -> Self {
        self.sub_command = XGroupSubCommand::Create(XGroupCreateCommand::default());
        self
    }

    pub fn set_id(mut self) -> Self {
        self.sub_command = XGroupSubCommand::SetId(XGroupSetIdCommand::default());
        self
    }

    pub fn destroy(mut self) -> Self {
        self.sub_command = XGroupSubCommand::Destroy;
        self
    }

    pub fn create_consumer(mut self, consumer_name: &str) -> Self {
        self.sub_command = XGroupSubCommand::CreateConsumer(consumer_name.to_string());
        self
    }

    pub fn delete_consumer(mut self, consumer_name: &str) -> Self {
        self.sub_command = XGroupSubCommand::DeleteConsumer(consumer_name.to_string());
        self
    }

    pub fn id(mut self, id: String) -> Self {
        self.sub_command = match self.sub_command {
            XGroupSubCommand::Create(sub) => {
                let mut new_sub_command = sub.clone();
                new_sub_command.id = Some(id);
                XGroupSubCommand::Create(new_sub_command)
            }
            XGroupSubCommand::SetId(sub) => {
                let mut new_sub_command = sub.clone();
                new_sub_command.id = Some(id);
                XGroupSubCommand::SetId(new_sub_command)
            }
            _ => XGroupSubCommand::Invalid("id only used in create or setid sub command".to_string())
        };
        self
    }

    pub fn entries_read(mut self) -> Self {
        self.sub_command = match self.sub_command {
            XGroupSubCommand::Create(sub) => {
                let mut new_sub_command = sub.clone();
                new_sub_command.entries_read = true;
                XGroupSubCommand::Create(new_sub_command)
            }
            XGroupSubCommand::SetId(sub) => {
                let mut new_sub_command = sub.clone();
                new_sub_command.entries_read = true;
                XGroupSubCommand::SetId(new_sub_command)
            }
            _ => XGroupSubCommand::Invalid("entriesread only used in create or setid sub command".to_string())
        };
        self
    }

    pub fn make_stream(mut self) -> Self {
        self.sub_command = match self.sub_command {
            XGroupSubCommand::Create(sub) => {
                let mut new_sub_command = sub.clone();
                new_sub_command.mkstream = true;
                XGroupSubCommand::Create(new_sub_command)
            }
            _ => XGroupSubCommand::Invalid("mkstream only used in create sub command".to_string())
        };
        self
    }

    pub fn build(self) -> Result<XGroupCommand, CommandError> {
        match self.sub_command {
            XGroupSubCommand::NotSet => Err(CommandError::Error(ByteString::from_static("XGroup Sub Command not set"))),
            XGroupSubCommand::Invalid(msg) => Err(CommandError::Error(ByteString::from(msg))),
            _ => Ok(XGroupCommand::new(self.stream, self.group_name, self.sub_command))
        }
    }
}

#[derive(Debug)]
enum XGroupSubCommand {
    NotSet,
    Create(XGroupCreateCommand),
    SetId(XGroupSetIdCommand),
    Destroy,
    CreateConsumer(String),
    DeleteConsumer(String),
    Invalid(String),
}

#[derive(Debug, Clone)]
struct XGroupCreateCommand {
    id: Option<String>,
    mkstream: bool,
    entries_read: bool,
}

impl Default for XGroupCreateCommand {
    fn default() -> Self {
        XGroupCreateCommand { id: None, mkstream: false, entries_read: false }
    }
}

#[derive(Debug, Clone)]
struct XGroupSetIdCommand {
    id: Option<String>,
    entries_read: bool,
}

impl Default for XGroupSetIdCommand {
    fn default() -> Self {
        XGroupSetIdCommand { id: None, entries_read: false }
    }
}