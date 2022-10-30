use std::fmt::Debug;

use async_trait::async_trait;

use super::Journalable;

pub trait ApplyResult: Debug + Send + Sync + 'static {}

impl<T> ApplyResult for T
where
    T: Debug + Send + Sync + 'static
{}

#[async_trait]
pub trait Snapshot: Debug + Send + Sync + 'static {
    type Entry: Journalable;
    type Snapshot: Journalable;
    type Applied: ApplyResult;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply_entry(&self, entry: Self::Entry) -> Result<Self::Applied, Self::Error>;
    async fn snapshot(&self) -> Result<Self::Snapshot, Self::Error>;
    async fn restore(&self, snapshot: Self::Snapshot) -> Result<(), Self::Error>;
}
