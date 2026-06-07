use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ciel_core::config::Config;
use ciel_core::config::ConfigError;
use ciel_core::tool::ToolImpl;
use ciel_core::tool::ToolInfo;
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
    /// Specifies the [OpenSSH Control Path](https://man.openbsd.org/ssh_config#ControlPath).
    ssh_control_path: Option<String>,
    default_timeout_seconds: Option<u32>,
    max_timeout_seconds: Option<u32>,
}

impl TerminalTool {
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let cfg: TerminalConfig = config.read("")?;

        // TODO: don't hardcode, use better way to define schemas
        let schema = &r#"{
          "type": "object",
          "properties": {
            "command": {
              "type": "string",
              "description": "The terminal command to execute."
            },
            "timeout_seconds": {
              "type": ["integer", "null"],
              "description": "The maximum execution time for the command, in seconds. Pass null for default timeout.",
              "minimum": 1
            }
          },
          "required": ["command", "timeout_seconds"],
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
    debug!(command = %schema.command, "running command");

    let timeout = schema
        .timeout_seconds
        .or(cfg.default_timeout_seconds)
        .unwrap_or(10)
        .min(cfg.max_timeout_seconds.unwrap_or(60));

    let control_path = cfg
        .ssh_control_path
        .as_deref()
        .unwrap_or("ControlPath=/tmp/ssh-%C");

    let remote_command = format!("exec </dev/null 2>&1;\n{}", schema.command);

    let mut child = tokio::process::Command::new(cfg.ssh_path.as_deref().unwrap_or("ssh"))
        .arg("-o")
        // Sets up multiplexing to hosts automatically and prevents the SSH handshake from having to
        // be executed for every invocation.
        .arg("ControlMaster=auto")
        .arg("-o")
        .arg(control_path)
        .arg("-o")
        .arg("ControlPersist=5m")
        .arg("-p")
        .arg(format!("{}", cfg.ssh_port.unwrap_or(22)))
        .arg(&cfg.ssh_host)
        .arg(remote_command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn command")?;
    let stdout = child.stdout.take().context("failed to open stdout")?;

    let tx_stderr = output.clone();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = tx_stderr.send(line).await;
        }
    });

    let timeout_duration = Duration::from_secs(timeout as u64);

    match tokio::time::timeout(timeout_duration, child.wait()).await {
        Ok(Ok(_status)) => return Ok(()),
        Ok(Err(e)) => {
            output.send(format!("---\nCommand with error: {e}")).await?;
            return Ok(());
        }
        Err(_) => {
            debug!(
                "command timed out after {} seconds, killing process",
                timeout_duration.as_secs()
            );

            child
                .kill()
                .await
                .context("failed to kill timed-out command")?;

            rootcause::bail!(
                "command timed out after {} seconds",
                timeout_duration.as_secs()
            );
        }
    }
}

#[derive(Debug, Deserialize)]
struct Schema {
    command: String,
    timeout_seconds: Option<u32>,
}
