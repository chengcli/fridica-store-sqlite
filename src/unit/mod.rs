//! The storage contract (`fridica_core::store`) on SQLite: a unit of work is
//! one transaction on the store thread.
use crate::Store;
use fridica_core::store::{self, Pending, Work};
use rusqlite::Connection;

mod actor;
mod attention;
mod controls;
mod events;
mod ingest;
mod ledger;
mod neighbours;
mod views;

/// The area traits over one connection or transaction. While Fridica still
/// runs its own queries, it wraps a transaction in this to reach the traits.
pub struct Sqlite<'a>(pub &'a Connection);

impl store::Store for Store {
    fn run(&self, work: Work) -> Pending<'_> {
        Box::pin(self.call(move |c| {
            let tx = c.transaction()?;
            let result = work(&mut Sqlite(&tx))?;
            tx.commit()?;
            Ok(result)
        }))
    }
}
