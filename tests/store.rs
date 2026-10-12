//! The store on its own: a fresh database at the current schema, closures on
//! the store thread, and one daemon per database.
use fridica_store_sqlite::{schema, Store};

#[tokio::test]
async fn a_fresh_store_is_current_private_incremental_and_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = Store::open(path.clone()).await.unwrap();
    let (version, vacuum, keys): (usize, i64, i64) = store
        .call(|c| {
            Ok((
                schema::version(c)?,
                c.query_row("PRAGMA auto_vacuum", [], |r| r.get(0))?,
                c.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?,
            ))
        })
        .await
        .unwrap();
    assert_eq!((version, vacuum, keys), (schema::VERSION, 2, 1));
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    // A second store on the same database is refused while the first is open.
    assert!(Store::open(path.clone()).await.is_err());
    store.close().await.unwrap();
    let again = Store::open(path).await.unwrap();
    again.close().await.unwrap();
}

/// A database that applied v10 before `archived_threads` reached that
/// migration (fridica#115 appended it to a shipped file) reports v11 without
/// the table; until #1 every message intake noted `archive_revive_failed` and
/// the archive round failed. v12 creates the table and drops that noise.
#[test]
fn a_database_that_missed_the_archive_table_gets_it_at_v12() {
    use rusqlite::Connection;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let mut c = Connection::open(&path).unwrap();
    for sql in &schema::MIGRATIONS[..11] {
        c.execute_batch(sql).unwrap();
    }
    c.execute_batch(
        "DROP TABLE archived_threads;
         INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','11');
         INSERT INTO health_events(kind,details_json,created) VALUES
           ('archive_revive_failed','{\"session\":\"T:C:1\",\"error\":\"no such table: archived_threads\"}',1),
           ('archive_failed','{\"error\":\"no such table: archived_threads\"}',2),
           ('archive_failed','{\"error\":\"disk full\"}',3);",
    )
    .unwrap();
    assert_eq!(schema::version(&c).unwrap(), 11);
    assert!(fridica_store_sqlite::archive::revive_tx(&c, "T:C:1", 4.).is_err());

    schema::migrate(&mut c).unwrap();

    assert_eq!(schema::version(&c).unwrap(), schema::VERSION);
    // Nothing archived: intake no longer fails, it finds no archive to revive.
    assert!(!fridica_store_sqlite::archive::revive_tx(&c, "T:C:1", 5.).unwrap());
    let kept: Vec<String> = c
        .prepare("SELECT details_json FROM health_events ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(kept, vec![r#"{"error":"disk full"}"#.to_string()]);

    // A fresh database already has the table from v10; v12 leaves it alone.
    let mut fresh = Connection::open_in_memory().unwrap();
    schema::migrate(&mut fresh).unwrap();
    let archived: i64 = fresh
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='archived_threads'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        (schema::version(&fresh).unwrap(), archived),
        (schema::VERSION, 1)
    );
}

/// A shipped migration is immutable: databases that already applied it never
/// see a later edit (how #1 happened). A schema change goes in a new file,
/// and this digest list grows by one line with it.
#[test]
fn shipped_migrations_are_immutable() {
    use sha2::{Digest, Sha256};
    let digests: Vec<String> = schema::MIGRATIONS
        .iter()
        .map(|sql| format!("{:x}", Sha256::digest(sql.as_bytes()))[..16].to_string())
        .collect();
    let expected = [
        "0f8104c6f9b53b44", // 001.sql
        "e47413c8e733d26a", // 002.sql
        "753ecf135b51775e", // 003.sql
        "7a55d3ac155b103a", // 004.sql
        "01b305ec342620d3", // 005.sql
        "de7f3e71da95356e", // 006.sql
        "2dea2f9c8ba28c3a", // 007.sql
        "18327099751071ce", // 008.sql
        "bffa66685b19dd19", // 009.sql
        "3403ef10c9d7d873", // 010.sql
        "d8f6c7fe269459ca", // 011.sql
        "8f26be5d9942fc68", // 012.sql
    ];
    assert_eq!(digests, expected, "a shipped migration file changed");
}
