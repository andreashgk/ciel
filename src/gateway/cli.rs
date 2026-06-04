use std::io::Write;
use std::io::stdin;
use std::io::stdout;

use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use nu_ansi_term::Color;
use nu_ansi_term::Style;
use rootcause::prelude::ResultExt;
use time::OffsetDateTime;
use tower::Service;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower::util::BoxService;

use crate::adapter::chat::AssistantEvent;
use crate::adapter::chat::ChatAdapterLayer;
use crate::adapter::chat::ChatRequest;
use crate::provider::ProviderError;
use crate::provider::Role;
use crate::provider::TokenStream;
use crate::providers::Request;
use crate::session::BranchEntry;
use crate::session::BranchId;
use crate::session::SessionStore;

pub struct Cli {
    pub sessions: SessionStore,
    pub service: BoxService<Request, TokenStream, ProviderError>,
    pub system_prompt: String,
}

impl Cli {
    pub async fn run(self) -> rootcause::Result<()> {
        let mut chat = ServiceBuilder::new()
            .layer(ChatAdapterLayer::default())
            .service(self.service);

        let mut stdin = stdin().lines();
        let mut stdout = stdout();
        loop {
            _ = write!(stdout, "{} ", Style::new().bold().paint(">"));
            _ = stdout.flush();

            let Some(Ok(line)) = stdin.next() else {
                return Ok(());
            };

            let now = OffsetDateTime::now_utc();
            let new_session_entry = BranchEntry {
                id: BranchId::new_from_time(now),
                user: None,
                role: Role::User,
                timestamp: now.to_utc(),
                content: line,
            };

            let session_identifier = "cli".to_string();
            let mut branch = self
                .sessions
                .by_session_id(&session_identifier)
                .await
                .context("failed to fetch session")?
                .unwrap_or_default();
            branch.push(new_session_entry);

            // TODO: better way to do this
            if branch.len() >= 100 {
                branch = branch.sliced(50..);
            }

            let system_prompt = self.system_prompt.clone();
            let branch_clone = branch.clone();
            let mut response_stream = chat
                .ready()
                .and_then(|service| async move {
                    service
                        .call(ChatRequest {
                            system_prompt,
                            messages: branch_clone,
                        })
                        .await
                })
                .await
                .context("failed to get LLM response")?;

            while let Some(response_event) = response_stream
                .try_next()
                .await
                .context("failed to get next event")?
            {
                match response_event {
                    AssistantEvent::Reasoning(_) => {
                        // TODO: display reasoning
                        continue;
                    }
                    AssistantEvent::Typing => {
                        // TODO: show typing in the terminal
                        continue;
                    }
                    AssistantEvent::Message(msg) => {
                        let now = OffsetDateTime::now_utc();
                        let message_id = BranchId::new_from_time(now);

                        _ = writeln!(stdout, "{}", Color::White.dimmed().paint(&msg));

                        let entry = BranchEntry {
                            id: message_id,
                            user: None,
                            role: Role::Assistant,
                            timestamp: now.to_utc(),
                            content: msg,
                        };

                        branch.push(entry);
                    }
                }
            }

            self.sessions
                .put(Some(session_identifier), branch)
                .await
                .context("failed to write back session")?;
        }
    }
}
