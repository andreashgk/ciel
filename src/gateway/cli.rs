use std::collections::HashMap;
use std::io::Write;
use std::io::stdin;
use std::io::stdout;

use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use nu_ansi_term::Color;
use nu_ansi_term::Style;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use time::OffsetDateTime;
use tower::Service;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower::util::BoxService;
use tracing::error;

use crate::adapter::chat::ChatAdapterLayer;
use crate::adapter::chat::ChatRequest;
use crate::provider::ProviderError;
use crate::provider::Role;
use crate::providers::Request;
use crate::session::BranchEntry;
use crate::session::BranchId;
use crate::session::SessionStore;
use crate::stream::MessageEvent;
use crate::stream::ResponseEvent;
use crate::stream::ResponseStream;

pub struct Cli {
    pub sessions: SessionStore,
    pub service: BoxService<Request, ResponseStream, ProviderError>,
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

            let mut channels = HashMap::new();

            while let Some(response_event) = response_stream
                .try_next()
                .await
                .context("failed to get next event")?
            {
                match response_event {
                    ResponseEvent::Message(MessageEvent::Start { index }) => {
                        channels.insert(index, String::new());
                    }
                    ResponseEvent::Message(MessageEvent::Chunk { index, delta }) => {
                        let buf = channels.get_mut(&index).context("unknown stream channel")?;
                        buf.push_str(&delta);

                        _ = write!(stdout, "{}", Color::White.dimmed().paint(&delta));
                        _ = stdout.flush();
                    }
                    ResponseEvent::Message(MessageEvent::Complete { index }) => {
                        let buf = channels.remove(&index).context("unknown stream channel")?;

                        _ = writeln!(stdout);

                        let now = OffsetDateTime::now_utc();
                        let message_id = BranchId::new_from_time(now);

                        let entry = BranchEntry {
                            id: message_id,
                            user: None,
                            role: Role::Assistant,
                            timestamp: now.to_utc(),
                            content: buf,
                        };

                        branch.push(entry);
                    }
                    _ => {
                        // TODO: display reasoning
                        // TODO: show LLM typing status in the terminal
                        continue;
                    }
                }
            }

            self.sessions
                .put(Some(session_identifier), branch)
                .await
                .context("failed to write back session")?;

            if !channels.is_empty() {
                error!("some stream channels were not closed");
            }
        }
    }
}
