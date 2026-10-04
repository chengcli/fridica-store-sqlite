//! All schema changes live here. Existing Python schema versions are immutable.
use anyhow::{bail, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

pub const VERSION: usize = 11;
pub const MIGRATIONS: [&str; VERSION] = [
    include_str!("migrations/001.sql"),
    include_str!("migrations/002.sql"),
    include_str!("migrations/003.sql"),
    include_str!("migrations/004.sql"),
    include_str!("migrations/005.sql"),
    include_str!("migrations/006.sql"),
    include_str!("migrations/007.sql"),
    include_str!("migrations/008.sql"),
    include_str!("migrations/009.sql"),
    include_str!("migrations/010.sql"),
    include_str!("migrations/011.sql"),
];

pub fn version(c: &Connection) -> Result<usize> {
    let exists: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='meta' AND type='table')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(0);
    }
    let value: Option<String> = c
        .query_row(
            "SELECT value FROM meta WHERE key='schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(value.map(|s| s.parse()).transpose()?.unwrap_or(0))
}

pub fn migrate(c: &mut Connection) -> Result<()> {
    let current = version(c)?;
    if current > VERSION {
        bail!("state database schema v{current} is newer than v{VERSION}");
    }
    // A new database returns freed pages a little at a time (archiving, #114);
    // this must be set before its first table.
    if current == 0 {
        c.execute_batch("PRAGMA auto_vacuum=INCREMENTAL;")?;
    }
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(current) {
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(sql)?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version',?)",
            [(index + 1).to_string()],
        )?;
        tx.commit()?;
    }
    Ok(())
}

/// Track *every* durable table mutation, not merely audit inserts. A write followed
/// by a compensating write must still invalidate automatic rollback.
pub fn install_mutation_guards(c: &Connection) -> Result<()> {
    let tables: Vec<String> = c
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    c.execute(
        "INSERT OR IGNORE INTO meta VALUES('durable_generation','0')",
        [],
    )?;
    for table in tables {
        let name = table.replace('"', "\"\"");
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            let condition = if table == "meta" {
                if operation == "DELETE" {
                    "WHEN OLD.key!='durable_generation'"
                } else {
                    "WHEN NEW.key!='durable_generation'"
                }
            } else {
                ""
            };
            c.execute_batch(&format!(
                "CREATE TRIGGER IF NOT EXISTS \"guard_{name}_{operation}\" AFTER {operation} ON \"{name}\" {condition}
                 BEGIN UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='durable_generation'; END;"))?;
        }
    }
    Ok(())
}
