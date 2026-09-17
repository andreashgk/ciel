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
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::Level;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::span;
use tracing::warn;
use twilight_gateway::CloseFrame;
use twilight_gateway::Event;
use twilight_gateway::EventTypeFlags;
use twilight_gateway::Intents;
use twilight_gateway::Message;
use twilight_gateway::Shard;
use twilight_gateway::ShardId;
use twilight_gateway::StreamExt as _;
use twilight_http::Client;
use twilight_model::id::Id;
use twilight_model::id::marker::ChannelMarker;

use crate::worker::channel_worker;

#[derive(Debug)]
pub struct Discord {
    token: Secret<String>,
    allowed_channels: HashSet<Id<ChannelMarker>>,
    state: Mutex<Option<RunningState>>,
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
            state: Default::default(),
        }))
    }
}

#[derive(Debug)]
struct RunningState {
    close_sender: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

#[async_trait]
impl GatewayImpl for Discord {
    async fn start(&self, ctx: Context) -> rootcause::Result<()> {
        let mut mu = self.state.lock().await;
        if mu.is_some() {
            return Ok(());
        }

        let token = self.token.clone();
        let allowed_channels = self.allowed_channels.clone();

        let http = Arc::new(Client::builder().token(token.0.clone()).build());
        let current_user = http.current_user().await?;
        let current_user = current_user.model().await?;

        let mut shard = Shard::new(
            ShardId::ONE,
            token.0.clone(),
            Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
        );

        let (close_sender, close_receiver) = oneshot::channel();
        let close_receiver = close_receiver.shared();

        let handle = tokio::spawn(async move {
            let sessions = ctx.sessions().clone();
            let chat = ctx.model().clone();

            let queue_map = QueueMap::<Id<ChannelMarker>, Event>::new();

            debug!("listening for events");
            loop {
                let item = select! {
                    item = shard.next_event(EventTypeFlags::MESSAGE_CREATE) => match item {
                        Some(item) => item,
                        None => {
                            break;
                        },
                    },
                    _ = close_receiver.clone() => {
                        break;
                    },
                };

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

            debug!("stopping gateway task");
            shard.close(CloseFrame::NORMAL);
            // Drain the shard receiver in order for discord to get the close event.
            while let Some(ev) = shard.next().await {
                if let Ok(Message::Close(_)) = ev {
                    break;
                }
            }
            info!("stopped gateway task");
        }.in_current_span());

        *mu = Some(RunningState {
            close_sender,
            task: handle,
        });

        Ok(())
    }

    async fn stop(&self) {
        let mut mu = self.state.lock().await;
        if let Some(state) = mu.take() {
            _ = state.close_sender.send(());
            // Wait until the task has finished.
            _ = state.task.await;
        }
    }
}
