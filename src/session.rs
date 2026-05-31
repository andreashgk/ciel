use std::collections::VecDeque;
use std::io;

use fjall::KeyspaceCreateOptions;
use fjall::Readable;
use fjall::SingleWriterTxDatabase;
use fjall::Slice;
use fjall::Snapshot;
use serde::Deserialize;
use serde::Serialize;
use tokio::task;
use uuid::Uuid;

mod branch;
pub use branch::*;

const TABLE_MESSAGES: &str = "sessions/messages";
const TABLE_SESSION_MAP: &str = "sessions/id-mapping";

#[derive(Clone)]
pub struct SessionStore {
    db: SingleWriterTxDatabase,
}

impl SessionStore {
    pub fn new(db: SingleWriterTxDatabase) -> Self {
        Self { db }
    }

    pub async fn by_session_id(&self, id: impl Into<String>) -> io::Result<Option<Branch>> {
        let s = self.clone();
        let id = id.into();

        task::spawn_blocking(move || {
            let tx = s.db.read_tx();

            let table =
                s.db.keyspace(TABLE_SESSION_MAP, KeyspaceCreateOptions::default)
                    .map_err(io::Error::other)?;

            let Some(val) = tx.get(&table, id.as_bytes()).map_err(io::Error::other)? else {
                return Ok(None);
            };
            let uuid = Uuid::from_slice(&val)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

            s.branch_from_snapshot(tx, BranchId(uuid))
        })
        .await
        .map_err(io::Error::other)
        .flatten()
    }

    pub async fn by_branch_id(&self, id: BranchId) -> io::Result<Option<Branch>> {
        if id.is_null() {
            return Ok(None);
        }

        let s = self.clone();
        task::spawn_blocking(move || {
            let tx = s.db.read_tx();
            s.branch_from_snapshot(tx, id)
        })
        .await
        .map_err(io::Error::other)
        .flatten()
    }

    pub async fn put(
        &self,
        session_id: Option<impl Into<String>>,
        entries: Branch,
    ) -> io::Result<()> {
        let db = self.db.clone();
        let session_id = session_id.map(|id| id.into());

        task::spawn_blocking(move || {
            let mut tx = db.write_tx();
            let table = db
                .keyspace(TABLE_MESSAGES, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            let mut session_head = entries.entries.front().map(|e| e.id);

            // Iterate from newest to oldest entry.
            for idx in (0..entries.entries.len()).rev() {
                let entry = &entries.entries[idx];

                let entry_bytes = serde_json::to_vec(&SessionEntryContainer {
                    parent: entries.entries.get(idx.saturating_sub(1)).map(|e| e.id),
                    session_head: session_head.take(),
                    content: entry.clone(),
                })
                .map_err(io::Error::other)?;

                let previous_value = tx
                    .fetch_update(&table, entry.id.0.as_bytes(), |_| {
                        Some(Slice::new(&entry_bytes))
                    })
                    .map_err(io::Error::other)?;
                // If the value existed before we reached the part of the tree that has already been
                // written. In this case, stop writing.
                if previous_value.is_some() {
                    break;
                }
            }

            if let Some(id) = session_id {
                let table = db
                    .keyspace(TABLE_SESSION_MAP, KeyspaceCreateOptions::default)
                    .map_err(io::Error::other)?;

                tx.insert(&table, id.as_bytes(), entries.id().0.as_bytes());
            }

            tx.commit().map_err(io::Error::other)?;
            Ok(())
        })
        .await
        .map_err(io::Error::other)
        .flatten()
    }

    /// Implementation of getting a branch from a branch id based on an existing snapshot.
    ///
    /// This is a blocking function.
    fn branch_from_snapshot(&self, tx: Snapshot, id: BranchId) -> io::Result<Option<Branch>> {
        let id = id.0;
        let messages = self
            .db
            .keyspace(TABLE_MESSAGES, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let mut result = VecDeque::new();
        let mut next_member_id = id;
        let mut session_head = None;

        loop {
            let next = tx
                .get(messages.clone(), next_member_id)
                .map_err(io::Error::other)?;
            let Some(next) = next else {
                // If this was the first element, the session simply doesn't exist.
                if result.is_empty() {
                    return Ok(None);
                }
                // Otherwise, there is some message missing in the tree. This is an error.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "incomplete session tree",
                ));
            };

            let next: SessionEntryContainer = serde_json::from_slice(&next)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            result.push_front(next.content);

            // Store the first session head we find and don't update it if we encounter new ones.
            if let Some(new_session_head) = next.session_head
                && session_head.is_none()
            {
                session_head = Some(new_session_head.0);
            }

            // End (or start, depending on point of view) of session reached, stop traversing tree.
            if session_head == Some(next_member_id) {
                break;
            }
            let Some(parent) = next.parent else {
                break;
            };
            next_member_id = parent.0;
        }

        Ok(Some(Branch { entries: result }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEntryContainer {
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<BranchId>,
    /// Pointer to the earliest message in the tree that is still part of this session.
    ///
    /// As sessions grow, old messages are only periodically removed in bulk in order to make use of
    /// prompt cache. This session_head models this by simply pointing to a more recent message.
    #[serde(skip_serializing_if = "Option::is_none")]
    session_head: Option<BranchId>,
    content: BranchEntry,
}
