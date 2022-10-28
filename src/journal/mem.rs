use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::result::Result as StdResult;
use std::sync::{Mutex, Arc};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    RwLock,
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::watch;
use tracing::{trace, warn, error};

use crate::ServerError;
use crate::raft::Result;

use super::snapshot::{ApplyEntry, Snapshot};
use super::{Journal, JournalEntry, JournalEntryType, JournalUpdate, Journalable, ApplyResult};

#[derive(Debug)]
pub struct VecJournal<D, V, W>
where
    D: Journalable,
    V: Journalable,
    W: ApplyEntry<V> + Snapshot<D>,
{
    pub(crate) entries: RwLock<Vec<JournalEntry<D, V>>>,
    pub(crate) commit_index: AtomicUsize,
    pub(crate) has_commits: AtomicBool,
    pub(crate) change_sender: watch::Sender<()>,
    storage: Arc<W>,
}

impl<D, V, W> VecJournal<D, V, W>
where
    D: Journalable,
    V: Journalable,
    W: ApplyEntry<V> + Snapshot<D>,
{
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<JournalEntry<D, V>>> {
        self.entries.read().expect("Journal lock was posioned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<JournalEntry<D, V>>> {
        self.entries.write().expect("Journal lock was posioned")
    }

    fn set_commit_index(&self, index: usize) -> Option<usize> {
        let prev_commit = self.commit_index.swap(index, Ordering::Release);
        if prev_commit == 0 {
            self.has_commits.store(true, Ordering::Release);
            None
        } else {
            Some(prev_commit)
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.read().len()
    }
}

#[async_trait]
impl<D, V, R, W> Journal<D, V, R> for VecJournal<D, V, W>
where
    D: Journalable,
    V: Journalable,
    W: ApplyEntry<V, Ok = R> + Snapshot<D>,
    R: ApplyResult,
{
    type Error = <W as ApplyEntry<V>>::Error;

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

    fn commit_and_apply(&self, index: u64, results: impl IntoIterator<Item = (u64, tokio::sync::oneshot::Sender<Result<R, V>>)>) {
        let commit_index: usize = index.try_into().expect("index overflowed usize");
        let begin_apply_range = self.set_commit_index(commit_index)
            .map_or(0, |i| i + 1);
        let committed_entries: Vec<_> = self.read()[begin_apply_range..=commit_index].iter()
            // Enumerate and index using original index
            .enumerate()
            .map(|(offset, entry)| (begin_apply_range + offset, entry))
            // Exclude snapshots
            .filter_map(|(i, e)| match &e.value {
                JournalEntryType::Value(v) => Some((i, v.clone())),
                _ => None,
            })
            .collect();

        let mut results: HashMap<usize, _> = results.into_iter()
            .map(|(i, r)| (i.try_into().expect("index overflowed usize"), r))
            .collect();
        let apply_storage = Arc::clone(&self.storage);
        tokio::spawn(async move {
            for (index, entry) in committed_entries {
                let result = apply_storage.apply(entry).await
                    .map_err(|e| ServerError::RequestError(Box::new(e)));
                if let Some(sender) = results.remove(&index) {
                    let send_result = sender.send(result);
                    if send_result.is_err() {
                        warn!(%index, "result receiver was dropped");
                    }
                }
            }
        });
    }

    async fn snapshot_without_commit(&self) -> Result<D, V> {
        self.storage.snapshot().await
            .map_err(|e| ServerError::RequestError(Box::new(e)))
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

impl<D, V, W> Display for VecJournal<D, V, W>
where
    D: Journalable,
    V: Journalable,
    W: ApplyEntry<V> + Snapshot<D>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl<V> Default for VecJournal<V, V, MemValue<V, V>>
where
    V: Journalable + Default,
{
    fn default() -> Self {
        let (sender, _) = watch::channel(());
        Self {
            entries: RwLock::new(Vec::new()),
            commit_index: 0.into(),
            has_commits: false.into(),
            change_sender: sender,
            storage: MemValue::default().into(),
        }
    }
}

#[derive(Debug, Default)]
pub struct MemValue<D: Journalable, V: Journalable> {
    value: Mutex<Option<V>>,
    _snapshot: PhantomData<fn() -> D>,
}

#[async_trait]
impl<V: Journalable> ApplyEntry<V> for MemValue<V, V> {
    type Ok = ();
    type Error = std::convert::Infallible;

    async fn apply(&self, entry: V) -> StdResult<Self::Ok, Self::Error> {
        *self.value.lock().expect("MemValue lock poisoned") = Some(entry);
        Ok(())
    }
}

#[async_trait]
impl<V> Snapshot<V> for MemValue<V, V>
where
    V: Journalable + Clone
{
    type Error = MemValueError;

    async fn snapshot(&self) -> StdResult<V, Self::Error> {
        let value = self.value.lock().expect("MemValue lock poisoned");
        let value = value.clone().ok_or(MemValueError::Uninitialized)?;
        Ok(value)
    }

    async fn restore(&self, snapshot: V) -> StdResult<(), Self::Error> {
        *self.value.lock().expect("MemValue lock poisoned") = Some(snapshot);
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MemValueError {
    #[error("can't restore snapshot; no value has been applied yet")]
    Uninitialized,
}
