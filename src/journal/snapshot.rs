use std::fmt::Debug;

use async_trait::async_trait;

use super::Journalable;

#[async_trait]
pub trait ApplyEntry<V: Journalable>: Debug + Send + Sync + 'static {
    type Ok: ApplyResult;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply(&self, entry: V) -> Result<Self::Ok, Self::Error>;
}

pub trait ApplyResult: Debug + Send + Sync + 'static {}

impl<T> ApplyResult for T
where
    T: Debug + Send + Sync + 'static
{}

#[async_trait]
pub trait Snapshot<D: Journalable>: Debug + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn snapshot(&self) -> Result<D, Self::Error>;
    async fn restore(&self, snapshot: D) -> Result<(), Self::Error>;
}
