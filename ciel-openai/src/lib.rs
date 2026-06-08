use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::io;
use std::ops::Not;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use ciel_core::config::Config;
use ciel_core::config::ConfigError;
use ciel_core::provider;
use ciel_core::provider::Provider;
use ciel_core::provider::ProviderError;
use ciel_core::provider::ProviderImpl;
use ciel_core::provider::request::Request;
use ciel_core::provider::request::ToolMode;
use ciel_core::provider::response::ChannelIndex;
use ciel_core::provider::response::MessageEvent;
use ciel_core::provider::response::ResponseEvent;
use ciel_core::provider::response::ResponseStream;
use ciel_core::provider::response::ToolEvent;
use ciel_core::provider::response::UsageEvent;
use ciel_core::session::branch::BranchEntry;
use ciel_core::session::branch::Role;
use ciel_util::secret::Secret;
use eventsource_stream::Eventsource;
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::Request as HttpRequest;
use hyper::Response;
use hyper::body::Incoming;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rootcause::IntoReport;
use rootcause::Report;
use serde::Deserialize;
use uuid::Uuid;

use crate::models::Error;
use crate::models::Event;
use crate::models::FunctionDelta;
use crate::models::FunctionTool;
use crate::models::RequestMessage;
use crate::models::RequestToolCall;
use crate::models::RequestToolCallType;
use crate::models::ResponseFormat;
use crate::models::StreamOptions;
use crate::models::ToolCallDelta;
use crate::models::ToolChoice;
use crate::models::ToolChoiceMode;
use crate::models::ToolDefinition;

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
    pub fn factory(cfg: Config) -> Result<Provider, ConfigError> {
        let config = cfg.read("")?;

        let client = Client::builder(TokioExecutor::new()).build(
            HttpsConnector::<HttpConnector>::builder()
                .with_native_roots()
                .map_err(|err| ConfigError::Other(format!("failed to build http client: {err}")))?
                .https_or_http()
                .enable_all_versions()
                .build(),
        );
        Ok(Provider::new(Arc::new(Self { config, client })))
    }
}

#[async_trait]
impl ProviderImpl for OpenAI {
    async fn chat(&self, request: Request) -> provider::Result<ResponseStream> {
        let url = format!("{}/chat/completions", self.config.base_url);
        let url = url
            .parse::<hyper::Uri>()
            .map_err(|err| format!("invalid base url: {err}"))
            .map_err(ConfigError::Other)
            .map_err(ProviderError::Config)?;

        // TODO: assistant messages and tool calls should maybe be merged?
        let messages = request
            .branch()
            .iter()
            .map(|entry| match entry {
                BranchEntry::System { message, .. } => RequestMessage {
                    name: None,
                    role: "system",
                    content: Some(message),
                    tool_call_id: None,
                    tool_calls: None,
                },
                BranchEntry::Message { role, content, .. } => RequestMessage {
                    name: None,
                    role: match role {
                        Role::Assistant => "assistant",
                        Role::User => "user",
                    },
                    content: Some(content.as_str()),
                    tool_call_id: None,
                    tool_calls: None,
                },
                BranchEntry::Tool {
                    tool_call_id,
                    name,
                    arguments,
                    ..
                } => RequestMessage {
                    name: None,
                    role: "assistant",
                    content: None,
                    tool_calls: Some(vec![RequestToolCall {
                        id: tool_call_id.as_str(),
                        tool: RequestToolCallType::Function {
                            arguments: arguments.as_str(),
                            name: name.as_str(),
                        },
                    }]),
                    tool_call_id: None,
                },
                BranchEntry::ToolResult {
                    tool_call_id,
                    result,
                    ..
                } => RequestMessage {
                    name: None,
                    role: "tool",
                    content: Some(result.as_str()),
                    tool_calls: None,
                    tool_call_id: Some(tool_call_id.as_str()),
                },
            })
            .collect::<Vec<_>>();

        let tools = request
            .tools()
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
            model: request
                .model()
                .ok_or_else(|| ProviderError::ModelNotFound("(none specified)".to_string()))?,
            max_completion_tokens: None,
            stream: Some(true),
            stream_options: Some(StreamOptions {
                include_usage: Some(true),
            }),
            response_format: request.schema().map(|schema| ResponseFormat {
                r#type: "json_schema",
                json_schema: Some(schema),
            }),
            reasoning_effort: None,
            tools: &tools,
            tool_choice: tools.is_empty().not().then_some(match request.tool_mode() {
                ToolMode::None => ToolChoice::Mode(ToolChoiceMode::None),
                ToolMode::Auto => ToolChoice::Mode(ToolChoiceMode::Auto),
                ToolMode::Required => ToolChoice::Mode(ToolChoiceMode::Required),
            }),
            parallel_tool_calls: Some(true),
        };

        let body = match serde_json::to_string(&req) {
            Ok(v) => v,
            Err(error) => {
                return Err(
                    ProviderError::IO(io::Error::new(io::ErrorKind::InvalidInput, error)).into(),
                );
            }
        };

        let mut req = HttpRequest::builder().uri(url).method("POST");
        if let Some(api_key) = &self.config.api_key {
            req = req.header("Authorization", format!("Bearer {}", api_key.0))
        }
        let req = req.body(body);
        let req = match req {
            Ok(v) => v,
            Err(err) => {
                return Err(
                    ProviderError::IO(io::Error::new(io::ErrorKind::InvalidInput, err)).into(),
                );
            }
        };
        let res = self.client.request(req).await;
        let res = match res {
            Ok(v) => v,
            Err(err) => {
                return Err(ProviderError::IO(io::Error::other(err)).into());
            }
        };

        if !res.status().is_success() {
            return Err(determine_error(res, request.model().unwrap_or("default")).await);
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
        let close_all = |streams: &mut BTreeMap<_, _>| {
            let mut events = Vec::with_capacity(streams.len());
            for (key, channel_index) in streams.iter() {
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
            streams.clear();
            events
        };

        // Since this will basically always have one element, BTreeMap is a slightly better fit
        // here.
        // Don't judge, it was more convenient for me this way.
        let mut open_channels = BTreeMap::<StreamKey, ChannelIndex>::new();

        let stream = try_stream! {
            while let Some(item) = stream.try_next().await? {
                if item.data == "[DONE]" {
                    break;
                }

                let event: Event = serde_json::from_str(&item.data)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("failed to parse event: {err}"),
                        )
                    })?;

                if event.choices.is_empty() {
                    let Some(usage) = event.usage else {
                        continue;
                    };

                    yield ResponseEvent::Usage(UsageEvent {
                        current_total_usage: usage.total_tokens,
                        input_tokens: Some(usage.prompt_tokens),
                        output_tokens: Some(usage.completion_tokens),
                        cached_tokens: usage.prompt_token_details.and_then(|u| u.cached_tokens),
                    });
                    continue;
                }

                // TODO: refusal
                let choice = &event.choices[0];

                if let Some(content) = &choice.delta.reasoning_content {
                    if content.is_empty() {
                        continue;
                    }
                    yield ResponseEvent::Reasoning(content.clone());
                } else if let Some(content) = &choice.delta.content {
                    if content.is_empty() {
                        continue;
                    }

                    let index = match open_channels.get(&StreamKey::Message) {
                        Some(v) => *v,
                        None => {
                            for ev in close_all(&mut open_channels) {
                                yield ev;
                            }

                            let id = Uuid::now_v7();
                            open_channels.insert(StreamKey::Message, id);

                            yield ResponseEvent::Message(MessageEvent::Start { index: id });
                            id
                        },
                    };

                    yield ResponseEvent::Message(MessageEvent::Chunk {
                        index,
                        delta: content.clone(),
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

                            let channel_index = Uuid::now_v7();
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
async fn determine_error(response: Response<Incoming>, model: &str) -> Report<ProviderError> {
    let status_code = response.status().as_u16();
    let body = response
        .into_body()
        .collect()
        .await
        .map(|body| {
            let b = &body.to_bytes();
            serde_json::from_slice(b)
                .map_err(|why| format!("failed to parse provider error: {why}\n(this indicates the provider did not return an openai-compatible error)"))
        })
        .map_err(|why| format!("failed to read error: {why}"))
        .flatten();

    let body: Error = match body {
        Ok(v) => v,
        // Try to determine the error from the status code if the body could not be resolved.
        Err(err) => {
            return match status_code {
                400 => {
                    ProviderError::IO(io::Error::new(io::ErrorKind::InvalidInput, "bad request"))
                }
                401 => ProviderError::Unauthorized,
                404 => ProviderError::ModelNotFound(model.to_string()),
                429 => ProviderError::RateLimitExceeded,
                other => ProviderError::IO(io::Error::other(format!(
                    "failed with unknown error (http code {other})"
                ))),
            }
            .into_report()
            .attach(err);
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
                return ProviderError::Unauthorized.into();
            }
            ProviderError::IO(io::Error::other(format!(
                "failed with unknown error: {}",
                body.error.message
            )))
        }
    }
    .into_report()
    .attach(body.error.message)
}
