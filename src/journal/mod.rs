use std::sync::{RwLock, atomic::{AtomicUsize, Ordering}};

use serde::{Serialize, Deserialize};
use tracing::warn;

impl Journal {
    pub fn append(&mut self, entry: JournalEntry) {
        self.entries.write().expect("Journal lock was posioned").push(entry);
    }

    pub fn get(&self, index: usize) -> Option<JournalEntry> {
        self.entries.read().expect("Journal lock was posioned").get(index).cloned()
    }

    // lastApplied
    pub fn len(&self) -> usize {
        self.entries.read().expect("Journal lock was posioned").len()
    }

    pub fn get_update(&self, index: usize) -> JournalUpdate {
        let entries = self.entries.read().expect("Journal lock was posioned");

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
    commit_index: AtomicUsize,
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
    term: u64,
    cmd: String,
}   

pub struct JournalUpdate {
    pub prev_term: u64,
    pub prev_index: usize,
    pub entries: Vec<JournalEntry>,
    pub commit_index: usize,
}