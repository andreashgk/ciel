use std::fmt::Debug;
use std::fmt::Display;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures_core::future::BoxFuture;
use thiserror::Error;
use tracing::instrument;

use crate::config::Config;
use crate::config::ConfigError;
use crate::request::Request;
use crate::stream::ResponseStream;

pub mod openai;

#[async_trait]
pub trait ProviderImpl: Display + Debug + Send + Sync {
    async fn chat(&self, request: Request) -> Result<ResponseStream>;
}

pub type ProviderCreateFn =
    Arc<dyn Fn(Config) -> BoxFuture<'static, Result<Arc<dyn ProviderImpl>>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Provider(Arc<dyn ProviderImpl>);

impl Provider {
    pub fn new(provider: Arc<dyn ProviderImpl>) -> Self {
        Self(provider)
    }

    #[instrument(fields(provider = %self, model = %request.model().unwrap_or("/")), skip(self, request))]
    pub async fn chat(&self, request: Request) -> Result<ResponseStream> {
        self.0.chat(request).await.map(|s| s.instrumented())
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
