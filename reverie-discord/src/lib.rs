use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use reverie_core::config::Config;
use reverie_core::config::ConfigError;
use reverie_core::context::Context;
use reverie_core::gateway::Gateway;
use reverie_core::gateway::GatewayImpl;
use reverie_core::provider::ProviderError;
use reverie_core::provider::request::Request;
use reverie_core::provider::response::MessageEvent;
use reverie_core::provider::response::ResponseEvent;
use reverie_core::provider::response::ResponseStream;
use reverie_core::provider::response::ToolEvent;
use reverie_core::provider::response::ToolResultEvent;
use reverie_core::session::branch::Branch;
use reverie_core::session::branch::BranchEntry;
use reverie_core::session::branch::BranchId;
use reverie_core::session::branch::Role;
use reverie_core::session::branch::UserInfo;
use reverie_core::session::store::SessionStore;
use reverie_util::queue_map::QueueMap;
use reverie_util::queue_map::QueueMapReceiver;
use reverie_util::secret::Secret;
use rootcause::option_ext::OptionExt;
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tower::Service;
use tower::ServiceExt;
use tracing::Instrument;
use tracing::Level;
use tracing::debug;
use tracing::error;
use tracing::span;
use tracing::warn;
use twilight_gateway::Event;
use twilight_gateway::EventTypeFlags;
use twilight_gateway::Intents;
use twilight_gateway::Shard;
use twilight_gateway::ShardId;
use twilight_gateway::StreamExt;
use twilight_http::Client;
use twilight_model::channel::message::AllowedMentions;
use twilight_model::id::Id;
use twilight_model::id::marker::ChannelMarker;

#[derive(Debug)]
pub struct Discord {
    token: Secret<String>,
    allowed_channels: HashSet<Id<ChannelMarker>>,
    strings: Arc<Strings>,
}

impl Discord {
    pub fn factory(config: Config) -> Result<Gateway, ConfigError> {
        let token = config.read::<Secret<String>>("token")?;
        let allowed_channels = config
            .read_optional::<HashSet<Id<ChannelMarker>>>("allowed-channels")?
            .unwrap_or_default();
        let strings = config
            .read_optional::<Strings>("strings")?
            .unwrap_or_default();

        if allowed_channels.is_empty() {
            warn!("no channels configured in `allowed-channels`; all messages will be ignored");
        }

        Ok(Arc::new(Self {
            token,
            allowed_channels,
            strings: Arc::new(strings),
        }))
    }
}

#[derive(Debug, Deserialize, Default)]
struct Strings {
    #[serde(rename = "tool.pending")]
    tool_pending: Option<String>,
    #[serde(rename = "tool.complete")]
    tool_complete: Option<String>,
}

#[async_trait]
impl GatewayImpl for Discord {
    async fn start(&self, ctx: Context) -> rootcause::Result<()> {
        let token = self.token.clone();
        let allowed_channels = self.allowed_channels.clone();

        let http = Arc::new(Client::builder().token(token.0.clone()).build());
        let current_user = http.current_user().await?;
        let current_user = current_user.model().await?;

        let strings = self.strings.clone();

        tokio::spawn(async move {
            let sessions = ctx.sessions().clone();
            let chat = ctx.model().clone();

            let mut shard = Shard::new(
                ShardId::ONE,
                token.0.clone(),
                Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
            );

            let queue_map = QueueMap::<Id<ChannelMarker>, Event>::new();

            debug!("listening for events");
            while let Some(item) = shard.next_event(EventTypeFlags::MESSAGE_CREATE).await {
                let Ok(discord_event) = item else {
                    tracing::warn!(source = ?item.unwrap_err(), "error receiving event");

                    continue;
                };

                if let Event::GatewayClose(Some(info)) = &discord_event {
                    debug!(code = info.code, reason = %info.reason, "gateway connection closed");
                }

                let Event::MessageCreate(message_create) = &discord_event else {
                    continue;
                };

                if !allowed_channels.contains(&message_create.channel_id) {
                    continue;
                }
                if message_create.author.id == current_user.id {
                    continue;
                }

                let Some(receiver) = queue_map.enqueue(message_create.channel_id, discord_event) else {
                    continue;
                };

                let span = span!(Level::DEBUG, "channel", id = %receiver.key().clone());
                tokio::spawn(
                    channel_worker(
                        receiver,
                        sessions.clone(),
                        chat.clone(),
                        ctx.system_prompt().to_string(),
                        http.clone(),
                        strings.clone(),
                    )
                    .map_err(|error| {
                            error!("error while processing channel: {error}");
                    })
                    .instrument(span)
                    .in_current_span()
                );

            }
        }.in_current_span());

        Ok(())
    }
}

async fn channel_worker(
    mut receiver: QueueMapReceiver<Id<ChannelMarker>, Event>,
    sessions: SessionStore,
    mut chat: impl Service<Request, Response = ResponseStream, Error = ProviderError>,
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
            .unwrap_or_else(|| {
                Branch::new([BranchEntry::System {
                    id: BranchId::new_from_current_time(),
                    message: system_prompt.clone(),
                }])
            });

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

            let new_session_entry = BranchEntry::Message {
                id: BranchId::new_from_time(timestamp),
                user: Some(UserInfo {
                    nickname,
                    username: message_create.author.name.clone(),
                }),
                role: Role::User,
                timestamp: timestamp.to_utc(),
                content: message_create.content.clone(),
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

            // TODO: properly handle this error by giving some feedback in the channel
            let service = chat.ready().await?;
            // TODO: properly handle this error by giving some feedback in the channel
            let mut response_stream = service.call(Request::from_branch(branch.clone())).await?;

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

                        branch.push(BranchEntry::ToolResult {
                            id: BranchId::new_from_current_time(),
                            tool_call_id: state.id.clone(),
                            name: state.name.clone(),
                            result: state.result,
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
