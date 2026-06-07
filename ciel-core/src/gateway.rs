use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryFutureExt;
use futures_util::future::join_all;
use rootcause::report;
use serde::Deserialize;
use tracing::Instrument;
use tracing::Level;
use tracing::error;
use tracing::info;
use tracing::span;

use crate::config::Config;
use crate::config::ConfigError;
use crate::context::Context;
use crate::context::ModelService;
use crate::session::store::SessionStore;

#[async_trait]
pub trait GatewayImpl {
    async fn start(&self, ctx: Context) -> rootcause::Result<()>;

    /// Stops the gateway. Should not return until the shutdown process has completed.
    async fn stop(&self);
}

pub type Gateway = Arc<dyn GatewayImpl + Send + Sync>;

pub type GatewayFactory = fn(Config) -> Result<Gateway, ConfigError>;

#[derive(Default)]
pub struct Gateways {
    factories: HashMap<String, GatewayFactory>,
    gateways: HashMap<String, Gateway>,
}

impl Gateways {
    pub fn register(&mut self, name: &str, factory: GatewayFactory) -> &mut Self {
        self.factories.insert(name.to_string(), factory);
        self
    }

    pub fn load(&mut self, config: Config) -> Result<(), ConfigError> {
        let gateways = config.read::<HashMap<String, GatewayConfig>>("")?;

        for (name, c) in gateways {
            let typ = c.r#type;
            let gateway = self
                .factories
                .get(&typ)
                .ok_or_else(|| ConfigError::Other(format!("unknown gateway type `{typ}`")))?;

            // Create the gateway object with a tracing span attached so any messages (like
            // warnings) show up as coming from this gateway.
            let span = span!(Level::DEBUG, "gateway", %name);
            let gateway = span.in_scope(|| gateway(config.scoped(&name)))?;

            self.gateways.insert(name.to_string(), gateway);
        }
        Ok(())
    }

    /// Start all configured gateways.
    ///
    /// Returns when all gateways have finished their startup prodecure.
    pub async fn run_all(
        &self,
        sessions: SessionStore,
        model: ModelService,
        system_prompt: String,
    ) -> rootcause::Result<()> {
        let ctx = Context::new(sessions, model, system_prompt);
        let futs = self.gateways.iter().map(|(name, v)| {
            let span = span!(Level::DEBUG, "gateway", %name);
            span.in_scope(|| info!("starting gateway"));

            v.start(ctx.clone())
                .map_err(|error| {
                    error!("failed to start: {error}");
                    error
                })
                .instrument(span)
        });
        let results = join_all(futs).await;
        let succeeded = results.iter().any(|r| r.is_ok());

        if !succeeded {
            return Err(report!("all gateways failed to start"));
        }
        Ok(())
    }

    /// Stops all running gateways.
    ///
    /// This method will wait for all gateways to fully stop before returning.
    pub async fn stop_all(&self) {
        join_all(self.gateways.values().map(|g| g.stop())).await;
    }
}

#[derive(Debug, Deserialize)]
struct GatewayConfig {
    r#type: String,
}
