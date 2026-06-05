use std::env;
use std::panic;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use fjall::SingleWriterTxDatabase;
use rootcause::prelude::ResultExt;
use rootcause_tracing::RootcauseLayer;
use tokio::io;
use tokio::signal::ctrl_c;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower::timeout::error::Elapsed;
use tracing::error;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

use crate::adapter::chat::ChatAdapterLayer;
use crate::adapter::tool::ToolAdapterLayer;
use crate::config::Config;
use crate::gateway::Gateways;
use crate::gateway::cli::Cli;
use crate::provider::ProviderError;
use crate::provider::ProviderImpl;
use crate::provider::openai::OpenAI;
use crate::providers::Providers;
use crate::request::Request;
use crate::session::SessionStore;
use crate::utils::panic_hook;

pub mod adapter;
pub mod config;
pub mod context;
pub mod gateway;
pub mod provider;
pub mod providers;
pub mod request;
pub mod session;
pub mod stream;
pub mod tool;
pub mod utils;

#[tokio::main]
async fn main() -> ExitCode {
    let subscriber = Registry::default().with(RootcauseLayer).with(
        tracing_subscriber::fmt::layer().with_filter(EnvFilter::new(
            "info,reverie=debug,fjall=warn,lsm_tree=warn",
        )),
    );

    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");

    #[cfg(debug_assertions)]
    {
        use rootcause::hooks::Hooks;
        use rootcause_tracing::SpanCollector;
        Hooks::new()
            .report_creation_hook(SpanCollector::new())
            .install()
            .expect("failed to install hooks");
    }

    let prev_hook = std::panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        prev_hook(panic_info);
        panic_hook::panic_hook(panic_info);
    }));

    // No need overcomplicate arg parsing for one argument.
    let subcommand = env::args().nth(1);
    let subcommand = match subcommand.as_deref() {
        Some("cli") => Subcommand::Cli,
        None => Subcommand::None,
        Some(other) => {
            eprintln!("Unknown subcommand: {other}");
            eprintln!("Expected 'cli' or no subcommand");
            return ExitCode::FAILURE;
        }
    };

    if let Err(err) = do_main(subcommand).await {
        error!("unrecoverable error encountered: {err}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[derive(Debug)]
enum Subcommand {
    Cli,
    None,
}

async fn do_main(subcommand: Subcommand) -> rootcause::Result<()> {
    let config_path = env::var("CONFIG").unwrap_or_else(|_| "config.toml".to_string());

    let config = Config::new(config_path)
        .await
        .context("cannot read configuration file")?;

    let db = SingleWriterTxDatabase::builder(
        config
            .read::<String>("database.path")
            .context("invalid configuration")?,
    )
    .temporary(config.read_optional("database.temporary")?.unwrap_or(false))
    .open()
    .context("unable to open database")?;

    let mut providers = Providers::default();
    providers.register_type(
        "openai".to_string(),
        Arc::new(|v| {
            Box::pin(async { OpenAI::new(v).map(|v| Arc::new(v) as Arc<dyn ProviderImpl>) })
        }),
    );
    providers
        .apply_config(config.scoped("provider"))
        .await
        .context("could not instantiate providers")?;

    let provider: String = config.read::<String>("model.default.provider")?;
    let model: String = config.read("model.default.name")?;

    let service = ServiceBuilder::new()
        .layer(ToolAdapterLayer::default())
        .layer(ChatAdapterLayer::default())
        .map_request(move |mut req: Request| {
            req.set_provider(provider.clone()).set_model(model.clone());
            req
        })
        // Cast the error produced by the timeout layer back to a ProviderError.
        .map_err(|err: BoxError| match err.downcast::<ProviderError>() {
            Ok(err) => *err,
            Err(err) => match err.downcast::<Elapsed>() {
                Ok(_) => ProviderError::IO(io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "request timed out",
                )),
                Err(err) => ProviderError::IO(io::Error::other(err)),
            },
        })
        .timeout(Duration::from_secs(10))
        .service(providers);

    let sessions = SessionStore::new(db.clone());
    let system_prompt: String = config.read("model.default.soul")?;

    match subcommand {
        Subcommand::Cli => {
            let tui = Cli {
                sessions,
                system_prompt: system_prompt.clone(),
                service: service.boxed(),
            };
            tui.run().await?;
        }
        Subcommand::None => {
            let mut gateways = Gateways::default();
            #[cfg(feature = "discord")]
            gateways.register("discord", gateway::discord::Discord::factory);
            gateways
                .load(config.scoped("gateway"))
                .context("failed to load gateways")?;
            gateways
                .run_all(sessions, service.clone().boxed_clone(), system_prompt)
                .await?;

            ctrl_c().await?;
            info!("exiting");

            // TODO: graceful gateway shutdown
        }
    }

    Ok(())
}
