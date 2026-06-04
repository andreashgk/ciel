use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::io;
use std::ops::Not;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use eventsource_stream::Eventsource;
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::Request;
use hyper::Response;
use hyper::body::Incoming;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use tracing::error;

use crate::config::Config;
use crate::config::ConfigError;
use crate::provider;
use crate::provider::LlmMessage;
use crate::provider::ProviderError;
use crate::provider::ProviderImpl;
use crate::provider::openai::models::Error;
use crate::provider::openai::models::Event;
use crate::provider::openai::models::FunctionDelta;
use crate::provider::openai::models::FunctionTool;
use crate::provider::openai::models::RequestMessage;
use crate::provider::openai::models::ResponseFormat;
use crate::provider::openai::models::StreamOptions;
use crate::provider::openai::models::ToolCallDelta;
use crate::provider::openai::models::ToolChoice;
use crate::provider::openai::models::ToolChoiceMode;
use crate::provider::openai::models::ToolDefinition;
use crate::stream::ChannelIndex;
use crate::stream::MessageEvent;
use crate::stream::ResponseEvent;
use crate::stream::ResponseStream;
use crate::stream::ToolEvent;
use crate::tool::ToolInfo;
use crate::utils::secret::Secret;

mod models;

#[derive(Debug)]
pub struct OpenAI {
    config: OpenAIConfig,
    client: Client<HttpsConnector<HttpConnector>, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct OpenAIConfig {
    base_url: String,
    api_key: Option<Secret<String>>,
}

impl OpenAI {
    pub fn new(cfg: Config) -> provider::Result<Self> {
        let config = cfg.read("")?;

        let client = Client::builder(TokioExecutor::new()).build(
            HttpsConnector::<HttpConnector>::builder()
                .with_native_roots()
                .map_err(|err| ProviderError::IO(io::Error::other(err)))?
                .https_or_http()
                .enable_all_versions()
                .build(),
        );
        Ok(Self { config, client })
    }
}

#[async_trait]
impl ProviderImpl for OpenAI {
    async fn chat(
        &self,
        model: &str,
        messages: &[LlmMessage],
        tool_mode: super::ToolMode,
        tools: &[Arc<ToolInfo>],
        schema: Option<&serde_json::Value>,
    ) -> provider::Result<ResponseStream> {
        let url = format!("{}/chat/completions", self.config.base_url);
        let url = url
            .parse::<hyper::Uri>()
            .map_err(|err| format!("invalid base url: {err}"))
            .map_err(ConfigError::Other)?;

        let messages = messages
            .iter()
            .map(|msg| {
                let role = match msg.role {
                    crate::provider::Role::System => "system",
                    crate::provider::Role::Assistant => "assistant",
                    crate::provider::Role::User => "user",
                };
                RequestMessage {
                    name: None,
                    role,
                    content: &msg.message,
                }
            })
            .collect::<Vec<_>>();

        let tools = tools
            .iter()
            .map(|tool| {
                ToolDefinition::Function(FunctionTool {
                    name: &tool.name,
                    description: Some(&*tool.description).filter(|s| s.is_empty()),
                    parameters: tool.arguments.as_ref(),
                    strict: tool.arguments.is_some(),
                })
            })
            .collect::<Vec<_>>();

        let req = models::Request {
            messages: &messages,
            model,
            max_completion_tokens: None,
            stream: Some(true),
            stream_options: Some(StreamOptions {
                include_usage: Some(false),
            }),
            response_format: schema.map(|schema| ResponseFormat {
                r#type: "json_schema",
                json_schema: Some(schema),
            }),
            reasoning_effort: None,
            tools: &tools,
            tool_choice: tools.is_empty().not().then_some(match tool_mode {
                provider::ToolMode::None => ToolChoice::Mode(ToolChoiceMode::None),
                provider::ToolMode::Auto => ToolChoice::Mode(ToolChoiceMode::Auto),
                provider::ToolMode::Required => ToolChoice::Mode(ToolChoiceMode::Required),
            }),
        };

        let body = match serde_json::to_string(&req) {
            Ok(v) => v,
            Err(error) => {
                return Err(ProviderError::IO(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    error,
                )));
            }
        };

        let mut req = Request::builder().uri(url).method("POST");
        if let Some(api_key) = &self.config.api_key {
            req = req.header("Authorization", format!("Bearer {}", api_key.0))
        }
        let req = req.body(body);
        let req = match req {
            Ok(v) => v,
            Err(err) => {
                return Err(ProviderError::IO(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    err,
                )));
            }
        };
        let res = self.client.request(req).await;
        let res = match res {
            Ok(v) => v,
            Err(err) => {
                return Err(ProviderError::IO(io::Error::other(err)));
            }
        };

        if !res.status().is_success() {
            return Err(determine_error(res, model).await);
        }

        let mut stream = res
            .into_body()
            .into_data_stream()
            .eventsource()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err));

        /// Used to map OpenAI streams to a ChannelIndex.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        enum StreamKey {
            Message,
            Tool(i64),
        }

        // Quick helper function to close all open channels. Returns a list of close events that
        // need to be yielded.
        let close_all = |streams: &mut HashMap<_, _>| {
            let mut events = Vec::with_capacity(streams.len());
            for (key, channel_index) in streams {
                let ev = match key {
                    StreamKey::Message => ResponseEvent::Message(MessageEvent::Complete {
                        index: *channel_index,
                    }),
                    StreamKey::Tool(_) => ResponseEvent::Tool(ToolEvent::Complete {
                        index: *channel_index,
                    }),
                };
                events.push(ev);
            }
            events
        };

        let mut open_channels = HashMap::<StreamKey, ChannelIndex>::new();
        let mut next_channel_id = 0;

        let stream = try_stream! {
            while let Some(item) = stream.try_next().await? {
                if item.data == "[DONE]" {
                    return;
                }

                let event: Event = serde_json::from_str(&item.data)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("failed to parse event: {err}"),
                        )
                    })?;

                if event.choices.is_empty() {
                    continue;
                }

                // TODO: refusal
                let choice = event.choices.first().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "no choices present",
                    )
                })?;

                if let Some(content) = &choice.delta.reasoning_content {
                    yield ResponseEvent::Reasoning(content.clone());
                } else if let Some(reasoning) = &choice.delta.content {
                    let index = match open_channels.get(&StreamKey::Message) {
                        Some(v) => *v,
                        None => {
                            for ev in close_all(&mut open_channels) {
                                yield ev;
                            }

                            let id = next_channel_id;
                            open_channels.insert(StreamKey::Message, id);

                            yield ResponseEvent::Message(MessageEvent::Start { index: id });

                            next_channel_id += 1;
                            id
                        },
                    };

                    yield ResponseEvent::Message(MessageEvent::Chunk {
                        index,
                        delta: reasoning.clone(),
                    });
                } else if let Some(tool) = &choice.delta.tool_calls {
                    match &tool[0] {
                        ToolCallDelta {
                            id: Some(tool_id),
                            index: tool_index,
                            function:
                                Some(FunctionDelta {
                                    arguments,
                                    name: Some(name),
                                }),
                        } => {
                            for ev in close_all(&mut open_channels) {
                                yield ev;
                            }

                            let channel_index = next_channel_id;
                            open_channels.insert(StreamKey::Tool(*tool_index), channel_index);

                            yield ResponseEvent::Tool(ToolEvent::Start {
                                index: channel_index,
                                tool_call_id: tool_id.to_string(),
                                name: name.to_string(),
                                handled: false,
                            });

                            if let Some(args) = arguments {
                                yield ResponseEvent::Tool(ToolEvent::Chunk {
                                    index: channel_index,
                                    delta: args.to_string(),
                                });
                            }
                        },
                        ToolCallDelta {
                            index: tool_index,
                            function:
                                Some(FunctionDelta {
                                    arguments: Some(arguments),
                                    ..
                                }),
                            ..
                        } => {
                            let channel_index = *open_channels
                                .get(&StreamKey::Tool(*tool_index))
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("unknown tool index {tool_index}"),
                                    )
                                })?;

                            yield ResponseEvent::Tool(ToolEvent::Chunk {
                                index: channel_index,
                                delta: arguments.to_string(),
                            });
                        },
                        _ => {
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "unexpected tool object",
                            ))?;
                        },
                    }
                }
            }

            for ev in close_all(&mut open_channels) {
                yield ev;
            }
        };

        Ok(ResponseStream::new(stream))
    }
}

impl Display for OpenAI {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpenAI(")?;
        f.write_str(&self.config.base_url)?;
        f.write_str(")")
    }
}

/// Tries to determine the equivalent [ProviderError] of the request assumes there is an error. If
/// it cannot find a specific error, a generic error will be returned.
async fn determine_error(response: Response<Incoming>, model: &str) -> ProviderError {
    let status_code = response.status().as_u16();
    let body = response
        .into_body()
        .collect()
        .await
        .map(|body| {
            let b = &body.to_bytes();
            serde_json::from_slice(b).inspect_err(|why| {
                error!(%why, "failed to parse error");
            })
        })
        .inspect_err(|why| {
            error!(%why, "failed to read response body");
        });

    let body: Error = match body {
        Ok(Ok(v)) => v,
        // Try to determine the error from the status code if the body could not be resolved.
        _ => {
            return match status_code {
                400 => {
                    ProviderError::IO(io::Error::new(io::ErrorKind::InvalidInput, "bad request"))
                }
                401 => ProviderError::Unauthorized,
                404 => ProviderError::ModelNotFound(model.to_string()),
                429 => ProviderError::RateLimitExceeded,
                _ => ProviderError::IO(io::Error::other("failed with unknown error")),
            };
        }
    };

    match body.error.code.as_str() {
        "context_length_exceeded" => ProviderError::ContextExceeded,
        "invalid_api_key" => ProviderError::Unauthorized,
        "model_not_found" => ProviderError::ModelNotFound(model.to_string()),
        "rate_limit_exceeded" => ProviderError::RateLimitExceeded,
        "insufficient_quota" => ProviderError::QuotaExceeded,
        _ => {
            if status_code == 401 {
                return ProviderError::Unauthorized;
            }
            ProviderError::IO(io::Error::other(format!(
                "failed with unknown error: {}",
                body.error.message
            )))
        }
    }
}
