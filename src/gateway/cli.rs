use std::collections::BTreeMap;
use std::io::Write;
use std::io::stdin;
use std::io::stdout;

use futures_util::TryStreamExt;
use nu_ansi_term::Color;
use nu_ansi_term::Style;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use time::OffsetDateTime;
use tower::Service;
use tower::ServiceExt;
use tower::util::BoxService;
use tracing::error;
use tracing::warn;

use crate::provider::ProviderError;
use crate::request::Request;
use crate::session::Branch;
use crate::session::BranchEntry;
use crate::session::BranchId;
use crate::session::Role;
use crate::session::SessionStore;
use crate::stream::MessageEvent;
use crate::stream::ResponseEvent;
use crate::stream::ResponseStream;
use crate::stream::ToolEvent;
use crate::stream::ToolResultEvent;

pub struct Cli {
    pub sessions: SessionStore,
    pub service: BoxService<Request, ResponseStream, ProviderError>,
    pub system_prompt: String,
}

impl Cli {
    pub async fn run(self) -> rootcause::Result<()> {
        let mut chat = self.service;

        let mut stdin = stdin().lines();
        let mut stdout = stdout();
        loop {
            _ = write!(stdout, "{} ", Style::new().bold().paint(">"));
            _ = stdout.flush();

            let Some(Ok(line)) = stdin.next() else {
                return Ok(());
            };

            let now = OffsetDateTime::now_utc();
            let new_session_entry = BranchEntry::Message {
                id: BranchId::new_from_time(now),
                user: None,
                role: Role::User,
                timestamp: now.to_utc(),
                content: line,
            };

            let system_prompt = self.system_prompt.clone();

            let session_identifier = "cli".to_string();
            let mut branch = self
                .sessions
                .by_session_id(&session_identifier)
                .await
                .context("failed to fetch session")?
                .unwrap_or_else(|| {
                    Branch::new([BranchEntry::System {
                        id: BranchId::new_from_current_time(),
                        message: system_prompt.clone(),
                    }])
                });
            branch.push(new_session_entry);

            // TODO: better way to do this
            if branch.len() >= 100 {
                branch = branch.sliced(50..);
            }

            // Allow for multiple turns, for example for tool calls.
            let mut should_continue = true;
            while should_continue {
                should_continue = false;

                let service = chat.ready().await?;
                let mut response_stream = service
                    .call(Request::from_branch(branch.clone()))
                    .await
                    .context("failed to get LLM response")?;

                let mut channels = BTreeMap::new();
                let mut tools = BTreeMap::new();
                let mut tool_results = BTreeMap::new();

                struct ToolState {
                    name: String,
                    id: String,
                    args: String,
                }

                struct ToolResultState {
                    name: String,
                    id: String,
                    result: String,
                }

                while let Some(response_event) = response_stream
                    .try_next()
                    .await
                    .context("failed to get next event")?
                {
                    match response_event {
                        ResponseEvent::Message(MessageEvent::Start { index }) => {
                            channels.insert(index, String::new());
                        }
                        ResponseEvent::Message(MessageEvent::Chunk { index, delta }) => {
                            let buf = channels.get_mut(&index).context("unknown stream channel")?;
                            buf.push_str(&delta);

                            _ = write!(stdout, "{}", Color::White.dimmed().paint(&delta));
                            _ = stdout.flush();
                        }
                        ResponseEvent::Message(MessageEvent::Complete { index }) => {
                            let buf = channels.remove(&index).context("unknown stream channel")?;

                            _ = writeln!(stdout);

                            let now = OffsetDateTime::now_utc();
                            let message_id = BranchId::new_from_time(now);

                            let entry = BranchEntry::Message {
                                id: message_id,
                                user: None,
                                role: Role::Assistant,
                                timestamp: now.to_utc(),
                                content: buf,
                            };

                            branch.push(entry);
                        }
                        ResponseEvent::Tool(ToolEvent::Start {
                            name,
                            index,
                            tool_call_id,
                            handled,
                        }) => {
                            if !handled {
                                warn!(%index, %name, "tool call was not handled");
                            }
                            should_continue = true;

                            tools.insert(
                                index,
                                ToolState {
                                    name,
                                    id: tool_call_id,
                                    args: String::new(),
                                },
                            );
                        }
                        ResponseEvent::Tool(ToolEvent::Chunk { index, delta }) => {
                            let Some(state) = tools.get_mut(&index) else {
                                continue;
                            };

                            state.args.push_str(&delta);
                        }
                        ResponseEvent::Tool(ToolEvent::Complete { index }) => {
                            let Some(state) = tools.remove(&index) else {
                                continue;
                            };

                            branch.push(BranchEntry::Tool {
                                id: BranchId::new_from_current_time(),
                                tool_call_id: state.id,
                                name: state.name,
                                arguments: state.args,
                            });
                        }
                        ResponseEvent::ToolResult(ToolResultEvent::Start {
                            index,
                            tool_call_id,
                            name,
                        }) => {
                            tool_results.insert(
                                index,
                                ToolResultState {
                                    name,
                                    id: tool_call_id,
                                    result: String::new(),
                                },
                            );
                        }
                        ResponseEvent::ToolResult(ToolResultEvent::Chunk { index, delta }) => {
                            let Some(state) = tool_results.get_mut(&index) else {
                                continue;
                            };

                            state.result.push_str(&delta);
                        }
                        ResponseEvent::ToolResult(ToolResultEvent::Complete { index }) => {
                            let Some(state) = tool_results.remove(&index) else {
                                continue;
                            };

                            branch.push(BranchEntry::ToolResult {
                                id: BranchId::new_from_current_time(),
                                tool_call_id: state.id,
                                name: state.name,
                                result: state.result,
                            });
                        }
                        _ => {
                            // TODO: display reasoning
                            // TODO: show LLM typing status in the terminal
                            continue;
                        }
                    }
                }

                self.sessions
                    .put(Some(session_identifier.clone()), branch.clone())
                    .await
                    .context("failed to write back session")?;

                if !channels.is_empty() {
                    error!("some stream channels were not closed");
                }
            }
        }
    }
}
