use std::fmt::{Debug, Display};

use async_trait::async_trait;

use super::Journalable;

#[async_trait]
pub trait ApplyEntry<V: Journalable> {
    type Ok: Debug + Display;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply(&self, entry: V) -> Result<Self::Ok, Self::Error>;
}

#[async_trait]
pub trait Snapshot<D: Journalable> {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn snapshot(&self) -> Result<D, Self::Error>;
    async fn restore(&self, snapshot: D) -> Result<(), Self::Error>;
}
