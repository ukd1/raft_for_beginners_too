pub mod mem;
mod snapshot;

use std::fmt::{Debug, Display};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{watch, oneshot};

pub trait Journal<D, V>
where
    Self: Debug + Display + Send + Sync + 'static,
    D: Journalable,
    V: Journalable,
{
    type Ok: Debug + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn append_entry(&self, entry: JournalEntry<D, V>) -> u64;
    fn append(&self, term: u64, value: V) -> u64;
    fn truncate(&self, index: u64);
    fn get(&self, index: u64) -> Option<JournalEntry<D, V>>;
    fn last_index(&self) -> Option<u64>;
    fn commit_index(&self) -> Option<u64>;
    // fn set_commit_index(&self, index: u64);
    fn commit_and_apply(&self, index: u64, results: impl IntoIterator<Item = (u64, oneshot::Sender<Result<Self::Ok, Self::Error>>)>);
    fn get_update(&self, index: Option<u64>) -> JournalUpdate<D, V>;
    fn subscribe(&self) -> watch::Receiver<()>;
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
