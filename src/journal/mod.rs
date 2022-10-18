use std::sync::{RwLock, atomic::{AtomicUsize, Ordering}};

use serde::{Serialize, Deserialize};
use tracing::warn;

impl Journal {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<JournalEntry>> {
        self.entries.read().expect("Journal lock was posioned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<JournalEntry>> {
        self.entries.write().expect("Journal lock was posioned")
    }

    pub fn append_entry(&self, entry: JournalEntry) {
        self.write().push(entry);
    }

    pub fn append(&self, term: u64, cmd: String) {
        let mut entries = self.write();
        let next_index = entries.len().try_into().expect("Journal entries length overflowed entry.index field");
        let entry = crate::journal::JournalEntry {
            term,
            index: next_index,
            cmd,
        };
        entries.push(entry);
    }

    pub fn truncate(&self, index: usize) {
        self.write().truncate(index);
    }

    pub fn get(&self, index: usize) -> Option<JournalEntry> {
        self.read().get(index).cloned()
    }

    // lastApplied
    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn last(&self) -> Option<JournalEntry> {
        self.read().last().cloned()
    }

    pub fn get_update(&self, index: usize) -> JournalUpdate {
        let entries = self.read();

        let len = entries.len();
        let index = if index > len {
            warn!(%index, "update requested beyond journal end");
            len
        } else {
            index
        };

        let prev_index = if index > 0 { index - 1 } else { 0 };
        let prev_term = if let Some(prev) = entries.get(prev_index) {
            prev.term
        } else {
            0
        };

        let update_entries: Vec<_> = entries.get(index..len)
            .expect("checked bounds above")
            .into();

        tracing::info!(%index, %len, %prev_index, %prev_term, ?entries, "journal update");

        JournalUpdate {
            prev_term,
            prev_index,
            entries: update_entries,
            commit_index: self.commit_index.load(Ordering::Acquire),
        }
    }
}


#[derive(Debug)]
pub struct Journal {
    entries: RwLock<Vec<JournalEntry>>,
    pub commit_index: AtomicUsize,
}

impl Default for Journal {
    fn default() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            commit_index: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub term: u64, // TODO make these a u32
    pub index: u64, // TODO make these a u32
    pub cmd: String,
}   

pub struct JournalUpdate {
    pub prev_term: u64, // TODO make these a u32
    pub prev_index: usize, // TODO make these a u32
    pub entries: Vec<JournalEntry>,
    pub commit_index: usize, // TODO make these a u32
}