mod stream;

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::FutureExt;
use futures_util::StreamExt;
use serde::Serialize;
use time::format_description::parse_owned;
use tower::Layer;
use tower::Service;

use crate::adapter::chat::stream::parse_token_stream;
use crate::provider;
use crate::provider::LlmMessage;
use crate::provider::ProviderError;
use crate::provider::Role;
use crate::providers;
use crate::providers::Request;
use crate::session::Branch;
use crate::session::UserInfo;
use crate::stream::ResponseEvent;

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
        let schema = include_str!("chat/schema.json");
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

/// A request to a service wrapped with [ChatAdapterLayer].
#[derive(Clone)]
pub struct ChatRequest {
    pub system_prompt: String,
    pub messages: Branch,
}

pub type ChatStream = BoxStream<'static, io::Result<ResponseEvent>>;

impl<S, TokenStream> Layer<S> for ChatAdapterLayer<S>
where
    S: Service<providers::Request, Response = TokenStream, Error = ProviderError>,
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

impl<S, TokenStream> Service<ChatRequest> for ChatAdapterService<S>
where
    S: Service<providers::Request, Response = TokenStream, Error = ProviderError>,
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

    fn call(&mut self, req: ChatRequest) -> Self::Future {
        let time_format = self.time_format.clone();
        let schema = self.schema.clone();

        // Basically the equivalent of a `try` block to build the request, so it can be wrapped into
        // a future afterwards.
        let build_request = move || {
            let history = req
                .messages
                .iter()
                .collect::<Vec<_>>()
                .chunk_by(|a, b| a.role == b.role)
                .map(|chunk| {
                    let role = chunk[0].role.clone();
                    let chunk = chunk
                        .iter()
                        .map(|msg| {
                            Ok(UserMessage {
                                user: msg.user.clone(),
                                time: msg
                                    .timestamp
                                    .format(&time_format)
                                    .map_err(io::Error::other)?,
                                content: msg.content.clone(),
                            })
                        })
                        .collect::<Result<Vec<_>, ProviderError>>()?;
                    let msgs = serde_json::to_string(&chunk).map_err(io::Error::other)?;

                    Ok(LlmMessage {
                        role,
                        message: msgs,
                    })
                })
                .collect::<Result<Vec<_>, ProviderError>>()?;

            let schema_str = serde_json::to_string(schema.as_ref()).map_err(io::Error::other)?;
            let mut messages = vec![LlmMessage {
                role: Role::System,
                message: wrap_system_prompt(false, &schema_str, &req.system_prompt),
            }];
            for msg in history {
                messages.push(msg);
            }

            let request = Request {
                messages,
                schema: Some(schema.clone()),
                tools: Vec::new(),
                tool_mode: provider::ToolMode::None,
            };

            Ok(request)
        };

        let request = build_request();
        let inner_future: provider::Result<_> = request.map(|f| self.inner.call(f));

        async move {
            let response_stream = inner_future?.await?;
            // TODO: apply instrument to this stream, maybe?
            let response_stream = parse_token_stream(false, response_stream);

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
        include_str!("chat/prompt_tools.md")
    } else {
        include_str!("chat/prompt_notools.md")
    };
    let prompt = prompt.replace("$SCHEMA", schema_str);
    format!("{original_prompt}\n{prompt}")
}
