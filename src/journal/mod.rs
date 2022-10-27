mod snapshot;

use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        RwLock,
    },
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{trace, warn};

pub trait Journal<D, V>
where
    Self: Debug + Display + Default,
    D: Journalable,
    V: Journalable
{
    fn append_entry(&self, entry: JournalEntry<D, V>) -> u64;
    fn append(&self, term: u64, value: V) -> u64;
    fn truncate(&self, index: u64);
    fn get(&self, index: u64) -> Option<JournalEntry<D, V>>;
    fn last_index(&self) -> Option<u64>;
    fn commit_index(&self) -> Option<u64>;
    fn set_commit_index(&self, index: u64);
    fn get_update(&self, index: Option<u64>) -> JournalUpdate<D, V>;
    fn subscribe(&self) -> watch::Receiver<()>;
}

#[derive(Debug)]
pub struct VecJournal<D, V>
where
    D: Journalable,
    V: Journalable,
{
    entries: RwLock<Vec<JournalEntry<D, V>>>,
    commit_index: AtomicUsize,
    has_commits: AtomicBool,
    change_sender: watch::Sender<()>,
}

impl<D, V> VecJournal<D, V>
where
    D: Journalable,
    V: Journalable,
{
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<JournalEntry<D, V>>> {
        self.entries.read().expect("Journal lock was posioned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<JournalEntry<D, V>>> {
        self.entries.write().expect("Journal lock was posioned")
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.read().len()
    }
}

impl<D, V> Journal<D, V> for VecJournal<D, V>
where
    D: Journalable,
    V: Journalable,
{
    fn append_entry(&self, entry: JournalEntry<D, V>) -> u64 {
        let mut entries = self.write();
        entries.push(entry);
        let last_index = entries.len() - 1;
        last_index.try_into().expect("journal.len() overflowed u64")
    }

    /// Append a command to the journal
    ///
    /// Returns: index of the appended entry
    fn append(&self, term: u64, value: V) -> u64 {
        let mut entries = self.write();
        entries.push(crate::journal::JournalEntry {
            term,
            value: JournalEntryType::Value(value),
        });
        let last_index = entries.len() - 1;
        last_index.try_into().expect("journal.len() overflowed u64")
    }

    fn truncate(&self, index: u64) {
        let index: usize = index.try_into().expect("index overflowed usize");
        self.write().truncate(index);
    }

    fn get(&self, index: u64) -> Option<JournalEntry<D, V>> {
        let index: usize = index.try_into().expect("index overflowed usize");
        self.read().get(index).cloned()
    }

    // lastApplied
    fn last_index(&self) -> Option<u64> {
        let len = self.read().len();
        let last_index = if len > 0 {
            len - 1
        } else {
            return None;
        };
        let last_index = last_index.try_into().expect("journal.len() overflowed u64");
        Some(last_index)
    }

    fn commit_index(&self) -> Option<u64> {
        let has_entries = self.has_commits.load(Ordering::Acquire);
        has_entries.then(|| {
            self.commit_index
                .load(Ordering::Acquire)
                .try_into()
                .expect("commit_index overflowed u64")
        })
    }

    fn set_commit_index(&self, index: u64) {
        let index: usize = index.try_into().expect("index overflowed usize");
        let prev_commit = self.commit_index.swap(index, Ordering::Release);
        if prev_commit == 0 {
            self.has_commits.store(true, Ordering::Release);
        }
    }

    fn get_update(&self, index: Option<u64>) -> JournalUpdate<D, V> {
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
            }
        };

        // Adapted from TLA+ spec: https://github.com/ongardie/raft.tla/blob/974fff7236545912c035ff8041582864449d0ffe/raft.tla#L222
        let last_index = <u64>::try_from(entries.len())
            .expect("journal length overflowed")
            .checked_sub(1);
        let commit_index = std::cmp::min(last_index, self.commit_index());

        trace!(
            ?index,
            ?prev_index,
            ?prev_term,
            ?update_entries,
            "generate journal update"
        );

        JournalUpdate {
            prev_term: prev_term.unwrap_or(0),
            prev_index: prev_index.map(|i| i.try_into().expect("prev_index overflowed u64")),
            entries: update_entries,
            commit_index,
        }
    }

    fn subscribe(&self) -> watch::Receiver<()> {
        self.change_sender.subscribe()
    }
}

impl<D, V> Display for VecJournal<D, V>
where
    D: Journalable,
    V: Journalable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl<D, V> Default for VecJournal<D, V>
where
    D: Journalable,
    V: Journalable,
{
    fn default() -> Self {
        let (sender, _) = watch::channel(());
        Self {
            entries: RwLock::new(Vec::new()),
            commit_index: 0.into(),
            has_commits: false.into(),
            change_sender: sender,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry<D, V>
where
    D: Journalable,
    V: Journalable,
{
    pub term: u64, // TODO make these a u32
    #[serde(bound = "V: DeserializeOwned")]
    pub value: JournalEntryType<D, V>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEntryType<D, V>
where
    D: Journalable,
    V: Journalable,
{
    #[serde(bound = "D: DeserializeOwned")]
    Snapshot(D),
    #[serde(bound = "V: DeserializeOwned")]
    Value(V),
}

pub trait Journalable:
    Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl<T> Journalable for T where
    T: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

pub struct JournalUpdate<D, V>
where
    D: Journalable,
    V: Journalable,
{
    pub prev_term: u64,          // TODO make these a u32
    pub prev_index: Option<u64>, // TODO make these a u32
    pub entries: Vec<JournalEntry<D, V>>,
    pub commit_index: Option<u64>, // TODO make these a u32
}

/* TODO: re-enable after Journal<V> refactor
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
        assert!(j.get(1).expect("didn't have an entry where we expected").value == "LOL2");
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
*/
