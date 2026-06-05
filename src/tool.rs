pub mod terminal;

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Tool {
    inner: Arc<dyn ToolImpl>,
}

impl Tool {
    /// Create a tool from a concrete implementation.
    pub fn new(tool: impl ToolImpl + 'static) -> Self {
        Self {
            inner: Arc::new(tool),
        }
    }

    /// See [ToolInfo].
    pub fn info(&self) -> &Arc<ToolInfo> {
        self.inner.info()
    }

    /// See [ToolImpl::call].
    pub async fn call(&self, arguments: mpsc::Receiver<String>, output: mpsc::Sender<String>) {
        self.inner.call(arguments, output).await
    }
}

/// Information about a tool.
#[derive(Debug, Clone)]
pub struct ToolInfo {
    /// The name of the tool.
    ///
    /// The LLM will call the tool with this name, so it should be clear from the name what it is.
    pub name: String,
    /// Optional description of the tool.
    ///
    /// Provides more context to the LLM about the tool. Can be left empty to provide no
    /// description.
    pub description: String,
    // TODO: make actual schema type
    pub arguments: Option<Value>,
}

/// ToolImpl specifies how to implement a tool callable by an LLM.
#[async_trait]
pub trait ToolImpl: Send + Sync {
    /// Returns static information about the tool.
    fn info(&self) -> &Arc<ToolInfo>;

    /// Perform a tool call.
    ///
    /// Both streaming arguments and streaming the function outputs is possible, but optional.
    async fn call(&self, mut arguments: mpsc::Receiver<String>, output: mpsc::Sender<String>);
}
