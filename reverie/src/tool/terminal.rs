use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use reverie_core::config::Config;
use reverie_core::config::ConfigError;
use reverie_core::tool::ToolImpl;
use reverie_core::tool::ToolInfo;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use serde::Deserialize;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tracing::debug;

/// Allows the agent to run commands over SSH.
#[derive(Clone)]
pub struct TerminalTool {
    info: Arc<ToolInfo>,
    cfg: TerminalConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TerminalConfig {
    ssh_host: String,
    ssh_port: Option<u16>,
    ssh_path: Option<String>,
}

impl TerminalTool {
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let cfg: TerminalConfig = config.read("")?;

        // TODO: don't hardcode, use better way to define schemas
        let schema = r#"{
              "type": "object",
              "properties": {
                "command": {
                  "type": "string",
                  "description": "The terminal command to execute."
                }
              },
              "required": ["command"],
              "additionalProperties": false
            }"#;
        Ok(Self {
            info: Arc::new(ToolInfo {
                name: "terminal".to_string(),
                description: "Run a command in your terminal.".to_string(),
                arguments: Some(serde_json::from_str(schema).expect("valid schema")),
            }),
            cfg,
        })
    }
}

#[async_trait]
impl ToolImpl for TerminalTool {
    fn info(&self) -> &Arc<ToolInfo> {
        &self.info
    }

    async fn call(&self, mut arguments: mpsc::Receiver<String>, output: mpsc::Sender<String>) {
        let mut argument_buf = String::new();
        while let Some(arg_part) = arguments.recv().await {
            argument_buf.push_str(arg_part.as_str());
        }

        if let Err(err) = do_tool(&argument_buf, &self.cfg, output.clone()).await {
            _ = output.send(format!("Tool error: {err}")).await;
        }
    }
}

async fn do_tool(
    args: &str,
    cfg: &TerminalConfig,
    output: mpsc::Sender<String>,
) -> rootcause::Result<()> {
    let schema: Schema = serde_json::from_str(args).context("failed to parse arguments")?;
    debug!(command = %schema.command, "terminal tool is being called");

    let mut child = tokio::process::Command::new(cfg.ssh_path.as_deref().unwrap_or("ssh"))
        .arg("-p")
        .arg(format!("{}", cfg.ssh_port.unwrap_or(22)))
        .arg(&cfg.ssh_host)
        .arg(schema.command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn command")?;
    let stdout = child.stdout.take().context("failed to open stdout")?;
    let stderr = child.stderr.take().context("failed to open stderr")?;

    let tx_stdout = output.clone();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = tx_stdout.send(line).await;
        }
    });

    let tx_stderr = output.clone();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = tx_stderr.send(line).await;
        }
    });

    drop(output);
    let _status = child.wait().await;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Schema {
    command: String,
}
