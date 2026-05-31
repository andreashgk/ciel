#![allow(unused)]

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct Request<'a> {
    pub messages: &'a [RequestMessage<'a>],
    pub model: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<&'a str>,
    // TODO: tools
}

#[derive(Debug, Serialize)]
pub struct RequestMessage<'a> {
    /// An optional name for the participant. Provides the model information to differentiate
    /// between participants of the same role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
    /// The role of the messages author, generally one of `assistant`, `developer`, `system`,
    /// `tool` or `user`.
    pub role: &'a str,
    pub content: &'a str,
    // TODO: multi-modal content
}

#[derive(Debug, Serialize)]
pub struct StreamOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_usage: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ResponseFormat<'a> {
    pub r#type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<&'a Value>,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub id: String,
    pub created: i64,
    pub usage: Option<Usage>,
    pub choices: Vec<EventChoice>,
}

#[derive(Debug, Deserialize)]
pub struct EventChoice {
    pub index: u32,
    pub delta: EventDelta,
}

#[derive(Debug, Deserialize)]
pub struct EventDelta {
    pub content: Option<String>,
    pub refusal: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Deserialize)]
pub struct Error {
    pub error_info: ErrorInfo,
}

#[derive(Debug, Deserialize)]
pub struct ErrorInfo {
    #[serde(default)]
    pub message: String,
    pub code: String,
}
