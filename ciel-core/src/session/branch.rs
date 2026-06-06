use std::collections::VecDeque;
use std::ops::RangeBounds;

use serde::Deserialize;
use serde::Serialize;
use time::UtcDateTime;
use uuid::NoContext;
use uuid::Timestamp;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct Branch {
    pub(super) entries: VecDeque<BranchEntry>,
}

impl Branch {
    pub fn new(entries: impl IntoIterator<Item = BranchEntry>) -> Self {
        Self {
            entries: entries.into_iter().collect(),
        }
    }

    /// The unique identifier for this branch. Corresponds to the most recent message in the branch.
    ///
    /// See [BranchId] for more info.
    pub fn id(&self) -> BranchId {
        self.entries
            .back()
            .map(|e| e.id())
            .unwrap_or(BranchId::null())
    }

    /// The amount of messages in this session.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if there are no messages in this branch.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all messages in the branch, from the oldest to the most recent message.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &BranchEntry> {
        self.entries.iter()
    }

    pub fn push(&mut self, entry: BranchEntry) {
        self.entries.push_back(entry);
    }

    /// Returns a new branch that represernts only a part of this branch.
    ///
    /// WARNING: it is easy to accidentally remove the system prompt this way.
    pub fn sliced(mut self, range: impl RangeBounds<usize>) -> Self {
        self.entries = self.entries.drain(range).collect();
        self
    }
}

/// A unique identifier for a branch of a session, corresponding with every message of a session.
///
/// Each message counts as its own branch and references its parent branch which is itself another
/// message.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
// TODO: move away from uuids for message ids and use 'scoped' ids
pub struct BranchId(pub(super) Uuid);

impl BranchId {
    pub fn new_from_time(timestamp: time::OffsetDateTime) -> Self {
        let uts = Timestamp::from_unix(
            NoContext,
            timestamp.unix_timestamp() as u64,
            timestamp.nanosecond(),
        );
        BranchId(Uuid::new_v7(uts))
    }

    pub fn new_from_current_time() -> Self {
        BranchId(Uuid::new_v7(Timestamp::now(NoContext)))
    }

    pub fn null() -> Self {
        BranchId(Uuid::nil())
    }

    pub fn is_null(&self) -> bool {
        self.0.is_nil()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BranchEntry {
    System {
        id: BranchId,
        message: String,
    },
    Message {
        id: BranchId,
        role: Role,
        #[serde(skip_serializing_if = "Option::is_none")]
        user: Option<UserInfo>,
        timestamp: UtcDateTime,
        content: String,
    },
    Tool {
        id: BranchId,
        tool_call_id: String,
        name: String,
        arguments: String,
    },
    ToolResult {
        id: BranchId,
        tool_call_id: String,
        name: String,
        result: String,
    },
}

impl BranchEntry {
    pub fn id(&self) -> BranchId {
        *match self {
            BranchEntry::System { id, .. } => id,
            BranchEntry::Message { id, .. } => id,
            BranchEntry::Tool { id, .. } => id,
            BranchEntry::ToolResult { id, .. } => id,
        }
    }

    pub fn is_same_kind(&self, other: &BranchEntry) -> bool {
        use BranchEntry::*;
        match (self, other) {
            (System { .. }, System { .. }) => true,
            (Tool { .. }, Tool { .. }) => true,
            (ToolResult { .. }, ToolResult { .. }) => true,
            (Message { role: role1, .. }, Message { role: role2, .. }) => role1 == role2,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    Assistant,
    User,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
    pub username: String,
}
