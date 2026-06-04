use std::fmt::Debug;
use std::fmt::Display;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::Span;
use tracing::instrument;

use crate::config::Config;
use crate::config::ConfigError;

pub mod openai;

#[async_trait]
pub trait ProviderImpl: Display + Debug + Send + Sync {
    async fn chat(
        &self,
        model: &str,
        messages: &[LlmMessage],
        tool_mode: ToolMode,
        tools: &[Tool],
        schema: Option<&Value>,
    ) -> Result<TokenStream>;
}

pub type ProviderCreateFn =
    Arc<dyn Fn(Config) -> BoxFuture<'static, Result<Arc<dyn ProviderImpl>>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Provider(Arc<dyn ProviderImpl>);

impl Provider {
    pub fn new(provider: Arc<dyn ProviderImpl>) -> Self {
        Self(provider)
    }

    #[instrument(fields(provider = %self, %model), skip(self, messages, schema))]
    pub async fn chat(
        &self,
        model: &str,
        messages: &[LlmMessage],
        tool_mode: ToolMode,
        tools: &[Tool],
        schema: Option<&Value>,
    ) -> Result<TokenStream> {
        self.0
            .chat(model, messages, tool_mode, tools, schema)
            .await
            .map(|s| s.instrumented())
    }
}

#[derive(Debug, Clone)]
pub struct LlmMessage {
    pub role: Role,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    System,
    Assistant,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolMode {
    None,
    Auto,
    Required,
}

#[derive(Debug, Clone)]
pub struct Tool {
    pub name: String,
    /// Describes what the tool does. Leave empty to omit this field.
    pub description: String,
    /// Optionally define a schema to allow parameters for this function call.
    pub parameters: Option<Arc<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Reasoning(String),
    Response(String),
    Tool(ToolToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolToken {
    Start(ToolInfo),
    Arguments(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolInfo {
    pub id: String,
    pub name: String,
}

pub struct TokenStream {
    inner: BoxStream<'static, io::Result<Token>>,
    span: Span,
}

impl Unpin for TokenStream {}

impl TokenStream {
    /// Wraps an existing stream.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = io::Result<Token>> + Send + 'static,
    {
        Self {
            inner: stream.boxed(),
            span: Span::none(),
        }
    }

    fn instrumented(mut self) -> Self {
        self.span = Span::current();
        self
    }
}

impl Stream for TokenStream {
    type Item = io::Result<Token>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let _guard = this.span.enter();

        this.inner.poll_next_unpin(cx)
    }
}

pub type Result<V> = std::result::Result<V, ProviderError>;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("rate limit exceeded")]
    RateLimitExceeded,
    #[error("quota exceeded")]
    QuotaExceeded,
    #[error("context length exceeded")]
    ContextExceeded,
    #[error("unknown model: {0}")]
    ModelNotFound(String),
    /// The provider has incorrect or malformed configuration.
    #[error("invalid config: {0}")]
    Config(#[from] ConfigError),
    /// An arbitrary IO error.
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
}

impl Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}
