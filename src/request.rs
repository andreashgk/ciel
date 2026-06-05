use std::sync::Arc;

use serde_json::Value;

use crate::session::Branch;
use crate::tool::ToolInfo;

#[derive(Clone)]
pub struct Request {
    provider: Option<String>,
    model: Option<String>,
    branch: Branch,
    schema: Option<Arc<Value>>,
    tool_definitions: Vec<Arc<ToolInfo>>,
    tool_mode: ToolMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolMode {
    None,
    Auto,
    Required,
}

impl Request {
    pub fn from_branch(branch: Branch) -> Self {
        Self {
            provider: None,
            model: None,
            branch,
            schema: None,
            tool_definitions: Vec::new(),
            tool_mode: ToolMode::Auto,
        }
    }

    pub fn branch(&self) -> &Branch {
        &self.branch
    }

    pub fn branch_mut(&mut self) -> &mut Branch {
        &mut self.branch
    }

    pub fn set_branch(&mut self, branch: Branch) -> &mut Self {
        self.branch = branch;
        self
    }

    pub fn schema(&self) -> Option<&Arc<Value>> {
        self.schema.as_ref()
    }

    pub fn set_schema(&mut self, schema: Arc<Value>) -> &mut Self {
        self.schema = Some(schema);
        self
    }

    pub fn tools(&self) -> impl Iterator<Item = &Arc<ToolInfo>> {
        self.tool_definitions.iter()
    }

    pub fn has_tools(&self) -> bool {
        !self.tool_definitions.is_empty()
    }

    pub fn add_tool(&mut self, tool: Arc<ToolInfo>) -> &mut Self {
        self.tool_definitions.push(tool);
        self
    }

    pub fn tool_mode(&self) -> &ToolMode {
        &self.tool_mode
    }

    pub fn set_tool_mode(&mut self, mode: ToolMode) -> &mut Self {
        self.tool_mode = mode;
        self
    }

    pub fn provider(&self) -> Option<&str> {
        self.provider.as_deref()
    }

    pub fn set_provider(&mut self, provider: String) -> &mut Self {
        self.provider = Some(provider);
        self
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn set_model(&mut self, model: String) -> &mut Self {
        self.model = Some(model);
        self
    }
}
