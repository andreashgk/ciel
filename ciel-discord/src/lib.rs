mod worker;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use ciel_core::config::Config;
use ciel_core::config::ConfigError;
use ciel_core::context::Context;
use ciel_core::gateway::Gateway;
use ciel_core::gateway::GatewayImpl;
use ciel_util::queue_map::QueueMap;
use ciel_util::secret::Secret;
use futures_util::TryFutureExt;
use serde::Deserialize;
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
use twilight_model::id::Id;
use twilight_model::id::marker::ChannelMarker;

use crate::worker::channel_worker;

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
