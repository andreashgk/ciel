mod stream;

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use ciel_core::provider;
use ciel_core::provider::ProviderError;
use ciel_core::provider::request::Request;
use ciel_core::provider::request::ToolMode;
use ciel_core::provider::response::ResponseEvent;
use ciel_core::session::branch::Branch;
use ciel_core::session::branch::BranchEntry;
use ciel_core::session::branch::UserInfo;
use ciel_core::tool::ToolInfo;
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::FutureExt;
use futures_util::StreamExt;
use serde::Serialize;
use time::format_description::parse_owned;
use tower::Layer;
use tower::Service;

use crate::chat_adapter::stream::parse_token_stream;

/// Chat layer on top of an LLM service, oriented for conversational text chats.
///
/// This layer wrap around raw llm requests/responses to provide multi-user chat awareness, as well
/// as support multiple responses from the LLM.
#[derive(Clone)]
pub struct ChatAdapterLayer<S> {
    pd: PhantomData<S>,
    schema: Arc<serde_json::Value>,
    time_format: Arc<time::format_description::OwnedFormatItem>,
}

impl<S> Default for ChatAdapterLayer<S> {
    fn default() -> Self {
        let schema = include_str!("chat_adapter/schema.json");
        let schema = serde_json::from_str(schema).expect("valid schema");

        let time_format = parse_owned::<2>("[year]-[month]-[day] [hour]:[minute]")
            .expect("valid time format at compile time");

        Self {
            pd: Default::default(),
            schema: Arc::new(schema),
            time_format: Arc::new(time_format),
        }
    }
}

pub type ChatStream = BoxStream<'static, io::Result<ResponseEvent>>;

impl<S, TokenStream> Layer<S> for ChatAdapterLayer<S>
where
    S: Service<Request, Response = TokenStream, Error = ProviderError>,
    TokenStream: Stream<Item = io::Result<ResponseEvent>>,
{
    type Service = ChatAdapterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ChatAdapterService {
            inner,
            schema: self.schema.clone(),
            time_format: self.time_format.clone(),
        }
    }
}

/// See [ChatAdapterLayer].
#[derive(Clone)]
pub struct ChatAdapterService<S> {
    inner: S,
    schema: Arc<serde_json::Value>,
    time_format: Arc<time::format_description::OwnedFormatItem>,
}

impl<S, TokenStream> Service<Request> for ChatAdapterService<S>
where
    S: Service<Request, Response = TokenStream, Error = ProviderError>,
    S::Future: Send + 'static,
    TokenStream: Stream<Item = io::Result<ResponseEvent>> + Send + 'static,
{
    type Response = ChatStream;
    type Error = ProviderError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let time_format = self.time_format.clone();
        let schema = self.schema.clone();
        let do_use_tools = req.has_tools();

        // Basically the equivalent of a `try` block to build the request, so it can be wrapped into
        // a future afterwards.
        let build_request = move || {
            let schema_str = serde_json::to_string(schema.as_ref()).map_err(io::Error::other)?;

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
                .flatten()
                // Wrap the (first) system prompt.
                .enumerate()
                .map(|(i, entry)| match (i, entry) {
                    (0, BranchEntry::System { id, message }) => BranchEntry::System {
                        id,
                        message: wrap_system_prompt(do_use_tools, &schema_str, &message),
                    },
                    (_, other) => other,
                });

            req.set_branch(Branch::new(history));

            // Tools and structured output are mutually exclusive, so the workaround is making the
            // LLM output its structured output as another tool.
            if do_use_tools {
                req.add_tool(Arc::new(ToolInfo {
                    name: "respond".to_string(),
                    description: "Use this tool to chat with users.".to_string(),
                    arguments: Some(schema["schema"].clone()),
                }))
                .set_tool_mode(ToolMode::Required);
            } else {
                req.set_schema(schema).set_tool_mode(ToolMode::None);
            }

            Ok(req)
        };

        let request = build_request();
        let inner_future: provider::Result<_> = request.map(|f| self.inner.call(f));

        async move {
            let response_stream = inner_future?.await?;
            // TODO: apply instrument to this stream, maybe?
            let response_stream = parse_token_stream(do_use_tools, response_stream);

            Ok(response_stream.boxed())
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

fn wrap_system_prompt(tools: bool, schema_str: &str, original_prompt: &str) -> String {
    let prompt = if tools {
        include_str!("chat_adapter/prompt_tools.md")
    } else {
        include_str!("chat_adapter/prompt_notools.md")
    };
    let prompt = prompt.replace("$SCHEMA", schema_str);
    format!("{original_prompt}\n{prompt}")
}
