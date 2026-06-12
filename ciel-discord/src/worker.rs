use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ciel_core::provider::ProviderError;
use ciel_core::provider::request::Request;
use ciel_core::provider::response::MessageEvent;
use ciel_core::provider::response::ResponseEvent;
use ciel_core::provider::response::ResponseStream;
use ciel_core::provider::response::ToolEvent;
use ciel_core::provider::response::ToolResultEvent;
use ciel_core::session::branch::Branch;
use ciel_core::session::branch::BranchEntry;
use ciel_core::session::branch::BranchId;
use ciel_core::session::branch::Role;
use ciel_core::session::branch::UserInfo;
use ciel_core::session::store::SessionStore;
use ciel_util::queue_map::QueueMapReceiver;
use futures_util::TryStreamExt;
use rootcause::Report;
use rootcause::option_ext::OptionExt;
use time::OffsetDateTime;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tower::Service;
use tower::ServiceExt;
use tracing::debug;
use tracing::error;
use tracing::warn;
use twilight_gateway::Event;
use twilight_http::Client;
use twilight_model::channel::message::AllowedMentions;
use twilight_model::id::Id;
use twilight_model::id::marker::ChannelMarker;

use crate::Strings;

pub async fn channel_worker(
    mut receiver: QueueMapReceiver<Id<ChannelMarker>, Event>,
    sessions: SessionStore,
    mut chat: impl Service<Request, Response = ResponseStream, Error = Report<ProviderError>>,
    system_prompt: String,
    http: Arc<Client>,
    strings: Arc<Strings>,
) -> rootcause::Result<()> {
    let mut events = Vec::new();
    loop {
        // TODO: this is maybe not the ideal spot for this but its okay for now.
        match receiver.try_finish() {
            Ok(()) => {
                break;
            }
            Err(r) => {
                receiver = r;
            }
        }

        receiver.recv_many(&mut events, 100).await;
        if events.is_empty() {
            continue;
        }

        let session_identifier = format!("discord:{}", receiver.key().clone());
        let mut branch = sessions
            .by_session_id(&session_identifier)
            .await?
            .unwrap_or_else(|| Branch::new([]));

        for event in events.drain(..) {
            let Event::MessageCreate(message_create) = &event else {
                continue;
            };

            let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                message_create.timestamp.as_micros() as i128 * 1000,
            )
            .expect("valid timestamp");

            let nickname = message_create
                .member
                .clone()
                .and_then(|m| m.nick.clone())
                .or(message_create.author.global_name.as_ref().cloned());

            // Resolve mentions of the format `<@id>` to `@username` to allow the bot to see who is
            // being pinged.
            let mut content = message_create.content.clone();
            for mention in &message_create.mentions {
                content =
                    content.replace(&format!("<@{}>", mention.id), &format!("@{}", mention.name));
            }

            let new_session_entry = BranchEntry::Message {
                id: BranchId::new_from_time(timestamp),
                user: Some(UserInfo {
                    nickname,
                    username: message_create.author.name.clone(),
                }),
                role: Role::User,
                timestamp: timestamp.to_utc(),
                content,
            };

            branch.push(new_session_entry);
        }

        // TODO: better way to manage context (count token usage)
        if branch.len() >= 100 {
            debug!(branch.len = %branch.len(), "resizing branch");
            branch = branch.sliced(50..);
        }

        // Allow for multiple turns, for example for tool calls.
        let mut should_continue = true;
        while should_continue {
            should_continue = false;

            let mut request = Request::from_branch(branch.clone());
            request.set_system_prompt(system_prompt.clone());

            // TODO: properly handle this error by giving some feedback in the channel
            let service = chat.ready().await?;
            // TODO: properly handle this error by giving some feedback in the channel
            let mut response_stream = service.call(request).await?;

            let mut last_sent = None;
            // The bot simulates typing at 150 wpm.
            // 100 wpm ~ 500 cpm
            let cpm = (150 * 5) as f32;

            let mut channels = BTreeMap::new();
            let mut tools = BTreeMap::new();
            let mut tool_results = BTreeMap::new();
            let mut tool_messages = BTreeMap::new();

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

            // TODO: properly handle this error by giving some feedback in the channel
            while let Some(response_event) = response_stream.try_next().await? {
                match response_event {
                    ResponseEvent::Message(MessageEvent::Start { index }) => {
                        channels.insert(index, String::new());

                        // The typing trigger is not as important so it's okay to keep going after
                        // these errors.
                        if let Err(error) = http.create_typing_trigger(*receiver.key()).await {
                            error!(%error, "failed to send typing trigger");
                        }
                    }
                    ResponseEvent::Message(MessageEvent::Chunk { index, delta }) => {
                        let buf = channels.get_mut(&index).context("unknown stream channel")?;
                        buf.push_str(&delta);
                    }
                    ResponseEvent::Message(MessageEvent::Complete { index }) => {
                        let msg = channels.remove(&index).context("unknown stream channel")?;

                        if let Some(last_sent) = last_sent
                            && receiver.is_empty()
                        {
                            // .len() instead of counting chars is not really an issue here.
                            let duration = msg.len() as f32 * 60. / cpm;
                            let duration = duration.min(3.);
                            sleep_until(last_sent + Duration::from_secs_f32(duration)).await;
                        }
                        last_sent = Some(Instant::now());
                        let now = OffsetDateTime::now_utc();
                        let message_id = BranchId::new_from_time(now);

                        // Send the response in 2000 character chunks (if responses are too large).
                        let mut msg_ref = msg.as_str();
                        while !msg_ref.is_empty() {
                            let chunk_end = msg_ref
                                .char_indices()
                                .nth(2000)
                                .map(|(i, _)| i)
                                .unwrap_or(msg_ref.len());

                            let chunk;
                            (chunk, msg_ref) = msg_ref.split_at(chunk_end);

                            http.create_message(*receiver.key())
                                .content(chunk)
                                .allowed_mentions(Some(&AllowedMentions {
                                    parse: Vec::new(),
                                    replied_user: true,
                                    roles: Vec::new(),
                                    users: Vec::new(),
                                }))
                                .await?;
                        }

                        let entry = BranchEntry::Message {
                            id: message_id,
                            user: None,
                            role: Role::Assistant,
                            timestamp: now.to_utc(),
                            content: msg,
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

                        let tool_message = http
                            .create_message(*receiver.key())
                            .content(
                                &strings
                                    .tool_pending
                                    .as_deref()
                                    .unwrap_or("-# tool pending: $NAME")
                                    .replace("$NAME", &name),
                            )
                            .await?
                            .model()
                            .await?;
                        tool_messages.insert(tool_call_id.clone(), tool_message.id);

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

                        let tool_message = tool_messages.remove(&state.id);

                        let result = state.result;
                        let mut split_char = result
                            .char_indices()
                            .rev()
                            .skip(2000)
                            .skip_while(|(_, c)| *c != '\n');
                        let result = if let Some((split_index, char)) = split_char.next() {
                            // Add the char's len to make sure to get the content *after* this
                            // character.
                            let (truncated, remaining) =
                                result.split_at(split_index + char.len_utf8());
                            let truncated_lines = truncated.lines().count();

                            format!(
                                "... {} previous line(s) ...\n{}",
                                truncated_lines, remaining
                            )
                        } else {
                            result
                        };

                        branch.push(BranchEntry::ToolResult {
                            id: BranchId::new_from_current_time(),
                            tool_call_id: state.id.clone(),
                            name: state.name.clone(),
                            result,
                        });

                        if let Some(tool_message) = tool_message {
                            let res = http
                                .update_message(*receiver.key(), tool_message)
                                .content(Some(
                                    &strings
                                        .tool_complete
                                        .as_deref()
                                        .unwrap_or("-# tool complete: $NAME")
                                        .replace("$NAME", &state.name),
                                ))
                                .await;
                            if let Err(err) = res {
                                error!("could not update tool message: {err}");
                            }
                        }
                    }
                    _ => {
                        continue;
                    }
                }
            }

            sessions
                .put(Some(session_identifier.clone()), branch.clone())
                .await?;
        }
    }
    Ok(())
}
