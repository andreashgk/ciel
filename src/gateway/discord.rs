use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use rootcause::option_ext::OptionExt;
use time::OffsetDateTime;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tower::Service;
use tower::ServiceBuilder;
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

use crate::adapter::chat::ChatAdapterLayer;
use crate::adapter::chat::ChatStream;
use crate::config::Config;
use crate::config::ConfigError;
use crate::context::Context;
use crate::gateway::Gateway;
use crate::gateway::GatewayImpl;
use crate::provider::ProviderError;
use crate::request::Request;
use crate::session::Branch;
use crate::session::BranchEntry;
use crate::session::BranchId;
use crate::session::Role;
use crate::session::SessionStore;
use crate::session::UserInfo;
use crate::stream::MessageEvent;
use crate::stream::ResponseEvent;
use crate::utils::queue_map::QueueMap;
use crate::utils::queue_map::QueueMapReceiver;
use crate::utils::secret::Secret;

#[derive(Debug)]
pub struct Discord {
    pub token: Secret<String>,
    pub allowed_channels: HashSet<Id<ChannelMarker>>,
}

impl Discord {
    pub fn factory(config: Config) -> Result<Gateway, ConfigError> {
        let token = config.read::<Secret<String>>("token")?;
        let allowed_channels = config
            .read_optional::<HashSet<Id<ChannelMarker>>>("allowed-channels")?
            .unwrap_or_default();

        if allowed_channels.is_empty() {
            warn!("no channels configured in `allowed-channels`; all messages will be ignored");
        }

        Ok(Arc::new(Self {
            token,
            allowed_channels,
        }))
    }
}

#[async_trait]
impl GatewayImpl for Discord {
    async fn start(&self, ctx: Context) -> rootcause::Result<()> {
        let token = self.token.clone();
        let allowed_channels = self.allowed_channels.clone();

        let http = Arc::new(Client::builder().token(token.0.clone()).build());
        let current_user = http.current_user().await?;
        let current_user = current_user.model().await?;

        tokio::spawn(async move {
            let sessions = ctx.sessions().clone();
            let model = ctx.model().clone();
            let chat = ServiceBuilder::new()
                .layer(ChatAdapterLayer::default())
                .service(model);

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
    mut chat: impl Service<Request, Response = ChatStream, Error = ProviderError>,
    system_prompt: String,
    http: Arc<Client>,
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

        // TODO: properly handle this error by giving some feedback in the channel
        let service = chat.ready().await?;
        // TODO: properly handle this error by giving some feedback in the channel
        let mut response_stream = service.call(Request::from_branch(branch.clone())).await?;

        let mut last_sent = None;
        // The bot simulates typing at 150 wpm.
        // 100 wpm ~ 500 cpm
        let cpm = (150 * 5) as f32;

        let mut channels = HashMap::new();

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
                _ => {
                    continue;
                }
            }
        }

        sessions.put(Some(session_identifier), branch).await?;
    }
    Ok(())
}
