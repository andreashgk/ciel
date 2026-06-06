use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::task::Poll;

use ciel_core::config::Config;
use ciel_core::provider;
use ciel_core::provider::Provider;
use ciel_core::provider::ProviderError;
use ciel_core::provider::ProviderFactory;
use ciel_core::provider::request::Request;
use ciel_core::provider::response::ResponseStream;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;
use tower::Service;

#[derive(Default, Clone)]
pub struct Providers {
    types: HashMap<String, ProviderFactory>,
    map: HashMap<String, Provider>,
}

impl Providers {
    pub fn register(&mut self, name: impl Into<String>, create_fn: ProviderFactory) {
        self.types.insert(name.into(), create_fn);
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

            let provider = provider_fn(config.scoped(&name))?;
            map.insert(name.to_string(), provider);
        }

        self.map = map;
        Ok(())
    }

    /// Get a provider by its name.
    pub fn get(&self, name: &str) -> Option<&Provider> {
        self.map.get(name)
    }
}

impl Service<Request> for Providers {
    type Response = ResponseStream;
    type Error = ProviderError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // TODO: default provider
        let provider = req.provider().and_then(|p| self.get(p).cloned());
        handle(provider, req).boxed()
    }
}

async fn handle(provider: Option<Provider>, req: Request) -> provider::Result<ResponseStream> {
    let Some(provider) = provider else {
        return Err(ProviderError::IO(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "no such provider: {}",
                req.provider().unwrap_or("(none specified)")
            ),
        )));
    };

    provider.chat(req).await
}

#[derive(Debug, Deserialize)]
struct ProviderConfig {
    r#type: String,
}
