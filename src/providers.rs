use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::task::Poll;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;
use tower::Service;

use crate::config::Config;
use crate::provider;
use crate::provider::LlmMessage;
use crate::provider::Provider;
use crate::provider::ProviderCreateFn;
use crate::provider::ProviderError;
use crate::provider::Tool;
use crate::provider::ToolMode;
use crate::stream::ResponseStream;

#[derive(Default, Clone)]
pub struct Providers {
    types: HashMap<String, ProviderCreateFn>,
    map: HashMap<String, Provider>,
}

impl Providers {
    pub fn register_type(&mut self, name: String, create_fn: ProviderCreateFn) {
        self.types.insert(name, create_fn);
    }

    pub async fn apply_config(&mut self, config: Config) -> provider::Result<()> {
        let entries: BTreeMap<String, ProviderConfig> = config.read("")?;

        let mut map = HashMap::with_capacity(entries.len());

        for (name, c) in entries {
            let provider_type = &c.r#type;
            let provider_fn = self.types.get(provider_type).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown provider type: {provider_type}"),
                )
            })?;

            let provider_impl = provider_fn(config.scoped(&name)).await?;
            map.insert(name.to_string(), Provider::new(provider_impl));
        }

        self.map = map;
        Ok(())
    }

    /// Get a provider by its name.
    pub fn get(&self, name: &str) -> Option<&Provider> {
        self.map.get(name)
    }
}

pub struct ProviderRequest {
    pub provider: String,
    pub request: ModelRequest,
}

pub struct ModelRequest {
    pub model: String,
    pub request: Request,
}

pub struct Request {
    pub messages: Vec<LlmMessage>,
    pub tools: Vec<Tool>,
    pub tool_mode: ToolMode,
    pub schema: Option<Arc<serde_json::Value>>,
}

impl Service<ProviderRequest> for Providers {
    type Response = ResponseStream;
    type Error = ProviderError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ProviderRequest) -> Self::Future {
        let provider = self.get(&req.provider).cloned();
        handle(provider, req).boxed()
    }
}

// TODO: dont literally have both here
impl Service<ProviderRequest> for Arc<Providers> {
    type Response = ResponseStream;
    type Error = ProviderError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ProviderRequest) -> Self::Future {
        let provider = self.get(&req.provider).cloned();
        handle(provider, req).boxed()
    }
}

async fn handle(
    provider: Option<Provider>,
    req: ProviderRequest,
) -> provider::Result<ResponseStream> {
    let Some(provider) = provider else {
        return Err(ProviderError::IO(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no such provider: {}", req.provider),
        )));
    };

    let req = req.request;
    provider
        .chat(
            &req.model,
            &req.request.messages,
            req.request.tool_mode,
            &req.request.tools,
            req.request.schema.as_deref(),
        )
        .await
}

#[derive(Debug, Deserialize)]
struct ProviderConfig {
    r#type: String,
}
