use std::{sync::{RwLock, atomic::{AtomicUsize, Ordering}}, fmt::{self, Display}};

use serde::{Serialize, Deserialize};
use tokio::sync::watch;
use tracing::{warn, trace};

impl Journal {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<JournalEntry>> {
        self.entries.read().expect("Journal lock was posioned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<JournalEntry>> {
        self.entries.write().expect("Journal lock was posioned")
    }

    pub fn append_entry(&self, entry: JournalEntry) -> u64 {
        let mut entries = self.write();
        entries.push(entry);
        let last_index = entries.len() - 1;
        last_index.try_into().expect("journal.len() overflowed u64")
    }

    /// Append a command to the journal
    /// 
    /// Returns: index of the appended entry
    pub fn append(&self, term: u64, cmd: String) -> u64 {
        let mut entries = self.write();
        entries.push(
            crate::journal::JournalEntry {
                term,
                cmd,
            }
        );
        let last_index = entries.len() - 1;
        last_index.try_into().expect("journal.len() overflowed u64")
    }

    pub fn truncate(&self, index: u64) {
        let index: usize = index.try_into().expect("index overflowed usize");
        self.write().truncate(index);
    }

    pub fn get(&self, index: u64) -> Option<JournalEntry> {
        let index: usize = index.try_into().expect("index overflowed usize");
        self.read().get(index).cloned()
    }

    // lastApplied
    pub fn last_index(&self) -> Option<u64> {
        let len = self.read().len();
        let last_index = if len > 0 {
            len - 1
        } else {
            return None;
        };
        let last_index = last_index.try_into().expect("journal.len() overflowed u64");
        Some(last_index)
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
            .try_into().expect("commit_index overflowed u64")
    }

    pub fn set_commit_index(&self, index: u64) {
        let index: usize = index.try_into().expect("index overflowed usize");
        self.commit_index.store(index, Ordering::Release);
    }

    pub fn get_update(&self, index: Option<u64>) -> JournalUpdate {
        let entries = self.read();

        let index = index.map(|i| usize::try_from(i).expect("index overflowed usize"));
        let prev_index = index.map(|i| i.saturating_sub(1));
        let prev_term = prev_index.and_then(|i| entries.get(i)).map(|e| e.term);

        let update_start_index = index.unwrap_or(0);
        let update_entries: Vec<_> = match entries.get(update_start_index..) {
            Some(v) => v.into(),
            None => {
                if !entries.is_empty() {
                    warn!(index = %update_start_index, "update requested beyond journal end");
                }
                vec![]
            },
        };

        // Adapted from TLA+ spec: https://github.com/ongardie/raft.tla/blob/974fff7236545912c035ff8041582864449d0ffe/raft.tla#L222
        let last_index = entries.len().saturating_sub(1);
        let commit_index = std::cmp::min(last_index, self.commit_index.load(Ordering::Acquire));

        trace!(?index, ?prev_index, ?prev_term, ?update_entries, "journal update");

        JournalUpdate {
            prev_term: prev_term.unwrap_or(0),
            prev_index: prev_index.map(|i| i.try_into().expect("prev_index overflowed u64")),
            entries: update_entries,
            commit_index: commit_index.try_into().expect("commit_index overflowed u64"),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.change_sender.subscribe()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.read().len()
    }
}


#[derive(Debug, Serialize)]
pub struct Journal {
    entries: RwLock<Vec<JournalEntry>>,
    pub commit_index: AtomicUsize,
    #[serde(skip)]
    change_sender: watch::Sender<()>,
}

impl Display for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json_value = serde_json::to_value(self)
            .map_err(|_| fmt::Error::default())?;
        write!(f, "{:#}", json_value)
    }
}

impl Default for Journal {
    fn default() -> Self {
        let (sender, _) = watch::channel(());
        Self {
            entries: RwLock::new(Vec::new()),
            commit_index: AtomicUsize::new(0),
            change_sender: sender,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub term: u64, // TODO make these a u32
    pub cmd: String,
}   

pub struct JournalUpdate {
    pub prev_term: u64, // TODO make these a u32
    pub prev_index: Option<u64>, // TODO make these a u32
    pub entries: Vec<JournalEntry>,
    pub commit_index: u64, // TODO make these a u32
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_journal_has_len_zero() {
        let j = Journal::default();

        assert!(j.len() == 0);
    }

    #[test]
    fn new_journal_has_expected_len() {
        let j = Journal::default();

        // add one entry, should be len 1
        j.append(1, "LOL".into());
        assert!(j.len() == 1);

        // add another entry
        j.append(1, "LOL2".into());
        assert!(j.len() == 2);

        // check that it actually appended
        assert!(j.get(1).expect("didn't have an entry where we expected").cmd == "LOL2");
    }

    #[test]
    fn new_journal_has_expected_len_after_truncate() {
        let j = Journal::default();

        // add one entry, should be len 1
        j.append(1, "1".into());
        j.append(1, "2".into());
        assert!(j.len() == 2);

        // truncate
        j.truncate(1);
        assert!(j.len() == 1);
    }
}








