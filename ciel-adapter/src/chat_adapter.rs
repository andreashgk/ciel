use std::collections::HashMap;
use std::fmt::Write;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use async_stream::try_stream;
use ciel_core::provider;
use ciel_core::provider::ProviderError;
use ciel_core::provider::request::Request;
use ciel_core::provider::response::MessageEvent;
use ciel_core::provider::response::ResponseEvent;
use ciel_core::session::branch::Branch;
use ciel_core::session::branch::BranchEntry;
use ciel_core::session::branch::UserInfo;
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use rootcause::Report;
use serde::Deserialize;
use serde::Serialize;
use time::format_description::parse_owned;
use tokio::pin;
use tower::Layer;
use tower::Service;
use tracing::debug;

/// Chat layer on top of an LLM service, oriented for conversational text chats.
///
/// This layer wrap around raw llm requests/responses to provide multi-user chat awareness, as well
/// as support multiple responses from the LLM.
#[derive(Clone)]
pub struct ChatAdapterLayer<S> {
    pd: PhantomData<S>,
    time_format: Arc<time::format_description::OwnedFormatItem>,
}

impl<S> Default for ChatAdapterLayer<S> {
    fn default() -> Self {
        let time_format = parse_owned::<2>("[year]-[month]-[day] [hour]:[minute]")
            .expect("valid time format at compile time");

        Self {
            pd: Default::default(),
            time_format: Arc::new(time_format),
        }
    }
}

pub type ChatStream = BoxStream<'static, io::Result<ResponseEvent>>;

impl<S, TokenStream> Layer<S> for ChatAdapterLayer<S>
where
    S: Service<Request, Response = TokenStream, Error = Report<ProviderError>>,
    TokenStream: Stream<Item = io::Result<ResponseEvent>>,
{
    type Service = ChatAdapterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ChatAdapterService {
            inner,
            time_format: self.time_format.clone(),
        }
    }
}

/// See [ChatAdapterLayer].
#[derive(Clone)]
pub struct ChatAdapterService<S> {
    inner: S,
    time_format: Arc<time::format_description::OwnedFormatItem>,
}

impl<S, TokenStream> Service<Request> for ChatAdapterService<S>
where
    S: Service<Request, Response = TokenStream, Error = Report<ProviderError>>,
    S::Future: Send + 'static,
    TokenStream: Stream<Item = io::Result<ResponseEvent>> + Send + 'static,
{
    type Response = ChatStream;
    type Error = Report<ProviderError>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let time_format = self.time_format.clone();

        // Basically the equivalent of a `try` block to build the request, so it can be wrapped into
        // a future afterwards.
        let build_request = move || {
            // TODO: it's very possible this is horribly inefficient right now
            let history = req
                .branch()
                .iter()
                .collect::<Vec<_>>()
                .chunk_by(|a, b| a.is_same_kind(b))
                .map(|chunk| match &chunk[0] {
                    BranchEntry::Message {
                        id,
                        role,
                        timestamp,
                        ..
                    } => {
                        let encoded = chunk
                            .iter()
                            .map(|entry| {
                                let BranchEntry::Message {
                                    user,
                                    timestamp,
                                    content,
                                    ..
                                } = entry
                                else {
                                    unreachable!();
                                };

                                Ok(UserMessage {
                                    user: user.to_owned(),
                                    time: timestamp
                                        .format(&time_format)
                                        .map_err(io::Error::other)?,
                                    content: content.clone(),
                                })
                            })
                            .collect::<Result<Vec<UserMessage>, ProviderError>>()?;

                        let msgs = serde_json::to_string(&encoded).map_err(io::Error::other)?;
                        Ok(vec![BranchEntry::Message {
                            id: *id,
                            role: role.clone(),
                            user: None,
                            timestamp: *timestamp,
                            content: msgs,
                        }])
                    }
                    _ => Ok(chunk.iter().map(|c| c.to_owned().to_owned()).collect()),
                })
                .collect::<Result<Vec<_>, ProviderError>>()?
                .into_iter()
                .flatten();

            let system_prompt = wrap_system_prompt(req.system_prompt());
            req.set_branch(Branch::new(history))
                .set_system_prompt(system_prompt);

            Ok(req)
        };

        let request = build_request();
        let inner_future: provider::Result<_> = request.map(|f| self.inner.call(f));

        async move {
            let response_stream = inner_future?.await?;

            #[derive(Debug, Deserialize)]
            struct JsonMsg {
                content: String,
            }
            // This is a temporary hack to prevent the LLM from responding JSON because it gets
            // confused by the context. In the long term there needs to be a better way to prevent
            // this behaviour.
            let s = try_stream! {
                pin!(response_stream);

                let mut channels = HashMap::new();

                while let Some(next) = response_stream.try_next().await? {
                    match next {
                        ResponseEvent::Message(MessageEvent::Start { index }) => {
                            yield ResponseEvent::Message(MessageEvent::Start { index });
                            channels.insert(index, String::new());
                        },
                        ResponseEvent::Message(MessageEvent::Chunk { index, delta }) => {
                            let Some(buf) = channels.get_mut(&index) else {
                                continue;
                            };
                            buf.push_str(&delta);
                        },
                        ResponseEvent::Message(MessageEvent::Complete { index }) => {
                            let Some(mut msg) = channels.remove(&index) else {
                                continue;
                            };

                            let v: Result<Vec<JsonMsg>, _> = serde_json::from_str(&msg);
                            if let Ok(value) = v && !value.is_empty() {
                                debug!(msgs = value.len(), "LLM responded in json; parsed and unwrapped");

                                msg = String::new();
                                for v in value {
                                    if msg.is_empty() {
                                        msg = v.content;
                                    } else {
                                      _ = write!(msg, "\n{}", v.content);
                                    }
                                }
                            }

                            yield ResponseEvent::Message(MessageEvent::Chunk { index, delta: msg });
                            yield ResponseEvent::Message(MessageEvent::Complete { index });
                        },
                        _ => yield next,
                    }
                }
            };
            Ok(ChatStream::from(s.boxed()))
        }
        .boxed()
    }
}

#[derive(Debug, Serialize)]
struct UserMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<UserInfo>,
    time: String,
    content: String,
}

fn wrap_system_prompt(original_prompt: &str) -> String {
    let prompt = include_str!("chat_adapter/prompt.md");
    format!("{original_prompt}\n{prompt}")
}
