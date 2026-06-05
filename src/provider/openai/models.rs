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
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub tools: &'a [ToolDefinition<'a>],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<RequestToolCall<'a>>>,
    /// The tool call ID that this message corresponds to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<&'a str>,
    // TODO: multi-modal content
}

#[derive(Debug, Serialize)]
pub struct RequestToolCall<'a> {
    /// ID of the tool call.
    pub id: &'a str,
    #[serde(flatten)]
    pub tool: RequestToolCallType<'a>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "function", rename_all = "lowercase")]
pub enum RequestToolCallType<'a> {
    Function { arguments: &'a str, name: &'a str },
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

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "function", rename_all = "lowercase")]
pub enum ToolDefinition<'a> {
    Function(FunctionTool<'a>),
}

#[derive(Debug, Serialize)]
pub struct FunctionTool<'a> {
    pub name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<&'a Value>,
    pub strict: bool,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolChoiceMode {
    None,
    Auto,
    Required,
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
    pub reasoning_content: Option<String>,
    pub content: Option<String>,
    pub refusal: Option<String>,
    pub tool_calls: Option<Vec<ToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
pub struct ToolCallDelta {
    pub index: i64,
    pub id: Option<String>,
    pub function: Option<FunctionDelta>,
}

#[derive(Debug, Deserialize)]
pub struct FunctionDelta {
    pub arguments: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Deserialize)]
pub struct Error {
    pub error: ErrorInfo,
}

#[derive(Debug, Deserialize)]
pub struct ErrorInfo {
    #[serde(default)]
    pub message: String,
    pub code: String,
}
