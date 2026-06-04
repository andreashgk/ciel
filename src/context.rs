use tower::util::BoxCloneService;

use crate::provider::ProviderError;
use crate::providers;
use crate::session::SessionStore;
use crate::stream::ResponseStream;

#[derive(Clone)]
pub struct Context {
    sessions: SessionStore,
    provider: ModelService,
    system_prompt: String,
}

pub type ModelService = BoxCloneService<providers::Request, ResponseStream, ProviderError>;

impl Context {
    pub fn new(sessions: SessionStore, provider: ModelService, system_prompt: String) -> Self {
        Self {
            sessions,
            provider,
            system_prompt,
        }
    }

    pub fn sessions(&self) -> &SessionStore {
        &self.sessions
    }

    pub fn model(&self) -> &ModelService {
        &self.provider
    }

    pub fn system_prompt(&self) -> &str {
        self.system_prompt.as_str()
    }
}
