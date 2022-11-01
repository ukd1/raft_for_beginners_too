use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::panic;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{watch, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use super::snapshot::Snapshot;
use super::{Journal, JournalEntry, JournalEntryType, JournalIndex, JournalUpdate, Journalable};
use crate::raft::{Result, Term};
use crate::ServerError;

const DEFAULT_ENTRY_COMPACTION_THRESHOLD: usize = 100;

type EntryVec<W> = Vec<JournalEntry<<W as Snapshot>::Snapshot, <W as Snapshot>::Entry>>;
type SnapshotTask<W> = JoinHandle<StdResult<<W as Snapshot>::Snapshot, <W as Snapshot>::Error>>;

enum VecJournalError {
    AlreadySnapshotting,
}

#[derive(Debug)]
pub struct VecJournal<W>
where
    W: Snapshot,
{
    pub(crate) entries: RwLock<EntryVec<W>>,
    pub(crate) commit_offset: AtomicUsize,
    pub(crate) has_commits: AtomicBool,
    pub(crate) change_sender: watch::Sender<()>,
    storage: Arc<W>,
    compaction_threshold: usize,
    snapshot_task: RwLock<Option<SnapshotTask<W>>>,
}

impl<W> VecJournal<W>
where
    W: Snapshot,
{
    pub fn new(storage: W, compaction_threshold: usize) -> Self {
        let (sender, _) = watch::channel(());
        Self {
            entries: RwLock::new(Vec::new()),
            commit_offset: 0.into(),
            has_commits: false.into(),
            change_sender: sender,
            storage: storage.into(),
            snapshot_task: Default::default(),
            compaction_threshold,
        }
    }

    fn set_commit_offset(&self, offset: usize) -> Option<usize> {
        debug!(%offset, "setting commit offset");
        let prev_commit = self.commit_offset.swap(offset, Ordering::Release);
        if prev_commit == 0 {
            self.has_commits.store(true, Ordering::Release);
            None
        } else {
            Some(prev_commit)
        }
    }

    fn get_commit_offset(&self) -> Option<usize> {
        self.has_commits
            .load(Ordering::Acquire)
            .then(|| self.commit_offset.load(Ordering::Acquire))
    }

    fn remove_entries(
        &self,
        entries: &mut RwLockWriteGuard<'_, EntryVec<W>>,
        range: impl RangeBounds<usize>,
    ) -> usize {
        use std::ops::Bound::*;

        let range_end = match range.end_bound() {
            Unbounded => entries.len() - 1,
            Included(n) => *n,
            Excluded(n) => n.saturating_sub(1),
        };

        let len_before = entries.len();
        entries.drain(range);
        let len_after = entries.len();
        let removed_count = len_before - len_after;
        let _ = self
            .commit_offset
            .fetch_update(Ordering::Release, Ordering::Acquire, |o| {
                assert!(o > range_end, "attempt to remove uncommitted entries");
                (o > range_end).then_some(o - removed_count)
            });
        removed_count
    }

    async fn snapshot(
        &self,
        entries: &mut RwLockWriteGuard<'_, EntryVec<W>>,
    ) -> StdResult<(), VecJournalError> {
        let mut snapshot_task = self.snapshot_task.write().await;
        if snapshot_task.is_some() {
            return Err(VecJournalError::AlreadySnapshotting);
        }

        let commit_offset = self.commit_offset.load(Ordering::Acquire);
        let (last_committed_index, last_committed_term) = {
            let last_committed = entries
                .get(commit_offset)
                .expect("commit_offset does not point to an entry");
            (last_committed.index, last_committed.term)
        };
        info!(index = %last_committed_index, "beginning journal snapshot");
        entries.insert(
            commit_offset + 1,
            JournalEntry {
                index: last_committed_index,
                term: last_committed_term,
                value: JournalEntryType::Snapshotting,
            },
        );
        let snapshot_storage = Arc::clone(&self.storage);
        snapshot_task.replace(tokio::spawn(
            async move { snapshot_storage.snapshot().await },
        ));

        Ok(())
    }

    async fn get_snapshot_result(&self, entries: &mut RwLockWriteGuard<'_, EntryVec<W>>) {
        {
            let is_finished_task_waiting = match self.snapshot_task.try_read() {
                Err(_) => false,
                Ok(t) => t.as_ref().map_or(false, |t| t.is_finished()),
            };
            if !is_finished_task_waiting {
                return;
            }
        }

        let maybe_task = self.snapshot_task.write().await.take();

        let snapshot = if let Some(snapshot_task) = maybe_task {
            match snapshot_task.await {
                Err(e) if e.is_panic() => panic::resume_unwind(e.into_panic()),
                // TODO: return an error?
                Err(_) => {
                    error!("snapshot task was cancelled unexpectedly");
                    return;
                }
                Ok(Err(e)) => {
                    error!(error = ?e, "snapshot task failed with error");
                    return;
                }
                Ok(Ok(s)) => Some(s),
            }
        } else {
            // TODO: simplify to .map()?
            None
        };

        if let Some(snapshot) = snapshot {
            // n.b. Never start the slice from anywhere but 0 or unbounded, because .enumerate()
            // offsets wouldn't correspond to entries offset
            let mut first_entry_offset = None;
            let mut snapshot_offset = None;
            for (offset, snapshot_entry) in entries.iter().enumerate() {
                match snapshot_entry.value {
                    JournalEntryType::Value(_) => {
                        if first_entry_offset.is_none() {
                            first_entry_offset = Some(offset);
                        }
                    }
                    JournalEntryType::Snapshotting => {
                        assert!(
                            snapshot_offset.is_none(),
                            "found more than one Snapshotting entry without breaking loop"
                        );
                        snapshot_offset.replace(offset);
                        break;
                    }
                    _ => continue,
                }
            }
            let snapshot_offset =
                snapshot_offset.expect("snapshot task implies a Snapshotting journal entry");
            entries[snapshot_offset].value = JournalEntryType::Snapshot(snapshot);
            info!(index = %entries[snapshot_offset].index, "storing snapshot result in journal");
            self.remove_entries(entries, ..snapshot_offset);
        }
    }

    fn locked_commit_index(
        &self,
        entries: &RwLockReadGuard<'_, EntryVec<W>>,
    ) -> Option<JournalIndex> {
        if self.has_commits.load(Ordering::Acquire) {
            let commit_offset = self.commit_offset.load(Ordering::Acquire);
            Some(entries[commit_offset].index)
        } else {
            None
        }
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.entries.read().await.len()
    }
}

#[async_trait]
impl<W> Journal for VecJournal<W>
where
    W: Snapshot,
{
    type Applied = W::Applied;
    type Value = W::Entry;
    type Snapshot = W::Snapshot;
    type Error = W::Error;

    async fn append_entry(&self, entry: JournalEntry<W::Snapshot, W::Entry>) -> JournalIndex {
        let mut entries = self.entries.write().await;
        let next_index = entry.index;

        entries.push(entry);

        next_index
    }

    /// Append a command to the journal
    ///
    /// Returns: index of the appended entry
    async fn append(&self, term: Term, value: W::Entry) -> JournalIndex {
        let mut entries = self.entries.write().await;
        let index = entries.last().map_or(0, |e| e.index + 1);
        entries.push(JournalEntry {
            index,
            term,
            value: JournalEntryType::Value(value),
        });

        index
    }

    async fn truncate(&self, index: JournalIndex) {
        let mut entries = self.entries.write().await;
        let index_offset = entries.partition_point(|e| e.index < index);
        entries.truncate(index_offset);
    }

    async fn indices_in_range(
        &self,
        start_inclusive: JournalIndex,
        end_inclusive: JournalIndex,
    ) -> Vec<JournalIndex> {
        let entries = self.entries.read().await;
        let start_offset = entries.partition_point(|e| e.index < start_inclusive);
        let end_offset = entries.partition_point(|e| e.index <= end_inclusive);
        entries
            .get(start_offset..end_offset)
            .unwrap_or_default()
            .iter()
            .map(|e| e.index)
            .collect()
    }

    async fn get(&self, index: JournalIndex) -> Option<JournalEntry<W::Snapshot, W::Entry>> {
        let entries = self.entries.read().await;
        let entry_offset = entries.binary_search_by_key(&index, |e| e.index).ok();
        entry_offset.map(|i| entries[i].clone())
    }

    // lastApplied
    async fn last_index(&self) -> Option<JournalIndex> {
        self.entries.read().await.last().map(|e| e.index)
    }

    async fn commit_index(&self) -> Option<JournalIndex> {
        let entries = self.entries.read().await;
        self.locked_commit_index(&entries)
    }

    async fn commit_and_apply(
        &self,
        index: JournalIndex,
        results: impl IntoIterator<
                Item = (
                    JournalIndex,
                    tokio::sync::oneshot::Sender<Result<Self::Applied, W::Entry>>,
                ),
            > + Send,
    ) {
        let mut entries = self.entries.write().await;
        self.get_snapshot_result(&mut entries).await;

        let current_commit_offset = self.get_commit_offset();
        debug!(?current_commit_offset, "commit_and_apply"); // DEBUG
        let uncompacted_entry_count =
            current_commit_offset.map_or(0, |offset| offset.saturating_sub(1));
        if uncompacted_entry_count >= self.compaction_threshold {
            let snapshot_status = self.snapshot(&mut entries).await;
            match snapshot_status {
                Ok(()) => (),
                Err(VecJournalError::AlreadySnapshotting) => {
                    warn!("tried to snapshot during an in-progress snapshot operation")
                }
            }
        }

        let mut new_commit_offset = entries.partition_point(|e| e.index < index);
        let prev_commit_offset = self.set_commit_offset(new_commit_offset);

        let mut begin_apply_range = prev_commit_offset.map_or(0, |i| i + 1);

        // If there is an uncommitted snapshot, it must have come from the leader,
        // so remove all log entries before it, then restore the snapshot to storage
        // before applying further entries.
        let commit_snapshot = {
            let commitable_entries = &entries[begin_apply_range..=new_commit_offset];
            commitable_entries
                .iter()
                // Walk through any snapshot entries...
                .enumerate()
                .map_while(|(i, e)| match &e.value {
                    JournalEntryType::Snapshot(s) => Some((begin_apply_range + i, s)),
                    _ => None,
                })
                // ...keeping only the last (most recent one)...
                .last()
                // ...and only clone that one.
                .map(|(i, s)| (i, s.to_owned()))
        };
        if let Some((snapshot_offset, _)) = commit_snapshot {
            let removed_count = self.remove_entries(&mut entries, ..snapshot_offset);
            // Commit offset was adjusted down by remove_entries, so load it
            new_commit_offset = self.commit_offset.load(Ordering::Acquire);
            begin_apply_range = snapshot_offset - removed_count;
        }

        let commit_values: Vec<_> = entries[begin_apply_range..=new_commit_offset]
            .iter()
            // Exclude snapshots
            .filter_map(|e| match &e.value {
                JournalEntryType::Value(v) => Some((e.index, v.clone())),
                _ => None,
            })
            .collect();

        let mut results: HashMap<_, _> = results.into_iter().collect();
        let apply_storage = Arc::clone(&self.storage);
        tokio::spawn(async move {
            if let Some((index, snapshot)) = commit_snapshot {
                let restore_result = apply_storage.restore(snapshot).await;
                if let Err(error) = restore_result {
                    error!(%index, %error, "could not restore snapshot");
                    panic!("failed to restore snapshot at index {}: {:?}", index, error);
                }
            }

            for (index, value) in commit_values {
                let result = apply_storage
                    .apply_entry(value)
                    .await
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

    async fn snapshot_without_commit(&self) -> Result<W::Snapshot, W::Entry> {
        self.storage
            .snapshot()
            .await
            .map_err(|e| ServerError::RequestError(Box::new(e)))
    }

    async fn get_update(
        &self,
        index: Option<JournalIndex>,
    ) -> JournalUpdate<W::Snapshot, W::Entry> {
        let entries = self.entries.read().await;

        let index_offset = index.map(|i| entries.partition_point(|e| e.index < i));
        let mut update_start_offset = index_offset.unwrap_or(0);
        let mut prev_entry = update_start_offset
            .checked_sub(1)
            .and_then(|i| entries.get(i));

        // Precondition: snapshots only ever occur at the beginning of the journal
        let snapshots_count = entries
            .get(update_start_offset..)
            .into_iter()
            .flatten()
            .take_while(|e| e.is_snapshot())
            .count();
        if snapshots_count > 0 {
            prev_entry = None;
            // Include the last snapshot
            update_start_offset += snapshots_count - 1;
        }

        let prev_index = prev_entry.map(|e| e.index);
        let prev_term = prev_entry.map(|e| e.term);

        let update_entries: Vec<_> = match entries.get(update_start_offset..) {
            Some(v) => v.into(),
            None => {
                if !entries.is_empty() {
                    warn!(index = %update_start_offset, "update requested beyond journal end");
                }
                vec![]
            }
        };

        // Adapted from TLA+ spec: https://github.com/ongardie/raft.tla/blob/974fff7236545912c035ff8041582864449d0ffe/raft.tla#L222
        let last_index = entries.last().map(|e| e.index);
        let commit_index = std::cmp::min(last_index, self.locked_commit_index(&entries));

        trace!(
            ?index,
            ?prev_index,
            ?prev_term,
            ?update_entries,
            "generate journal update"
        );

        JournalUpdate {
            prev_term: prev_term.unwrap_or(0),
            prev_index,
            entries: update_entries,
            commit_index,
        }
    }

    fn subscribe(&self) -> watch::Receiver<()> {
        self.change_sender.subscribe()
    }
}

impl<W> Display for VecJournal<W>
where
    W: Snapshot,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl<V> Default for VecJournal<MemValue<V, V>>
where
    V: Journalable + Default,
{
    fn default() -> Self {
        let (sender, _) = watch::channel(());
        Self {
            entries: RwLock::new(Vec::new()),
            commit_offset: 0.into(),
            has_commits: false.into(),
            change_sender: sender,
            storage: MemValue::default().into(),
            snapshot_task: Default::default(),
            compaction_threshold: DEFAULT_ENTRY_COMPACTION_THRESHOLD,
        }
    }
}

#[derive(Debug, Default)]
pub struct MemValue<D: Journalable, V: Journalable> {
    value: Mutex<Option<V>>,
    _snapshot: PhantomData<fn() -> D>,
}

#[async_trait]
impl<V> Snapshot for MemValue<V, V>
where
    V: Journalable + Clone,
{
    type Entry = V;
    type Snapshot = V;
    type Applied = ();
    type Error = MemValueError;

    async fn apply_entry(&self, entry: Self::Entry) -> StdResult<Self::Applied, Self::Error> {
        *self.value.lock().await = Some(entry);
        Ok(())
    }

    async fn snapshot(&self) -> StdResult<Self::Snapshot, Self::Error> {
        let value = self.value.lock().await;
        let value = value.clone().ok_or(MemValueError::Uninitialized)?;
        Ok(value)
    }

    async fn restore(&self, snapshot: Self::Snapshot) -> StdResult<(), Self::Error> {
        *self.value.lock().await = Some(snapshot);
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MemValueError {
    #[error("can't restore snapshot; no value has been applied yet")]
    Uninitialized,
}
