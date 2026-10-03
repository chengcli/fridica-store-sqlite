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
