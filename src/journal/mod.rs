pub mod mem;
mod snapshot;

use std::fmt::{Debug, Display};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{oneshot, watch};

pub use self::snapshot::ApplyResult;
use crate::raft::{Result, Term};

pub type JournalIndex = u32;

#[async_trait]
pub trait Journal
where
    Self: Debug + Display + Send + Sync + 'static,
{
    // It would be simpler here to have:
    // ```
    // type Storage: Snapshot;
    // ```
    //
    // Then, use `Self::Storage::Value`, `Self::Storage::Applied`, etc.
    // throughout. As of Rust 1.64.0, this isn't possible and results
    // in an "ambiguous associated type" error. The workaround is to
    // use `<Self::Storage as Snapshot>::Value`. This would be fine
    // if the associated types were used only in the trait function
    // signatures, but the implementation of `Server` uses these associated
    // types extensively. Sprinkling fully-qualified syntax everywhere
    // would make the `Server` code less readable. The related issue is
    // https://github.com/rust-lang/rust/issues/38078.
    //
    // So, instead of having an associated type with a `Snapshot` bound
    // here, implementers of `Journal` (such as `mem::VecJournal`) may
    // use the `Snapshot` trait to make themselves generic over a storage
    // adapter (that provides entry applying and snapshotting functions).
    // Then, the `impl Journal` block passes through the associated types
    // from the `Snapshot`-implementing adapter, e.g.:
    // ```
    // impl Journal for SomeJournal<W: Snapshot> {
    //     type Value = W::Value;
    //     type Snapshot = W::Snapshot;
    //     type Applied = W::Applied;
    //     ...
    // }
    // ```
    //
    type Value: Journalable;
    type Snapshot: Journalable;
    type Applied: ApplyResult;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn append_entry(&self, entry: JournalEntry<Self::Snapshot, Self::Value>) -> JournalIndex;
    async fn append(&self, term: Term, value: Self::Value) -> JournalIndex;
    async fn truncate(&self, index: JournalIndex);
    async fn indices_in_range(
        &self,
        start_inclusive: JournalIndex,
        end_inclusive: JournalIndex,
    ) -> Vec<JournalIndex>;
    async fn get(&self, index: JournalIndex) -> Option<JournalEntry<Self::Snapshot, Self::Value>>;
    async fn last_index(&self) -> Option<JournalIndex>;
    async fn commit_index(&self) -> Option<JournalIndex>;
    async fn commit_and_apply(
        &self,
        index: JournalIndex,
        results: impl IntoIterator<
                Item = (
                    JournalIndex,
                    oneshot::Sender<Result<Self::Applied, Self::Value>>,
                ),
            > + Send,
    );
    // TODO: remove after testing
    async fn snapshot_without_commit(&self) -> Result<Self::Snapshot, Self::Value>;
    async fn get_update(
        &self,
        index: Option<JournalIndex>,
    ) -> JournalUpdate<Self::Snapshot, Self::Value>;
    fn subscribe(&self) -> watch::Receiver<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry<D, V>
where
    D: Journalable,
    V: Journalable,
{
    pub index: JournalIndex,
    pub term: Term,
    #[serde(bound = "V: DeserializeOwned")]
    pub value: JournalEntryType<D, V>,
}

impl<D, V> JournalEntry<D, V>
where
    D: Journalable,
    V: Journalable,
{
    pub fn is_snapshot(&self) -> bool {
        matches!(self.value, JournalEntryType::Snapshot(_))
    }
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
    Snapshotting,
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
    pub prev_term: Term,
    pub prev_index: Option<JournalIndex>,
    pub entries: Vec<JournalEntry<D, V>>,
    pub commit_index: Option<JournalIndex>,
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
