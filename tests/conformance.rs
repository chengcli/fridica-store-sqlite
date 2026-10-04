//! fridica-core's storage conformance suite (`fridica_core::store::conformance`)
//! against a fresh SQLite store in a temporary directory.
use fridica_core::store::conformance::Backend;

struct Sqlite;

impl Backend for Sqlite {
    type Guard = tempfile::TempDir;
    type Store = fridica_store_sqlite::Store;
    async fn fresh() -> (Self::Guard, Self::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = fridica_store_sqlite::Store::open(dir.path().join("state.sqlite3"))
            .await
            .unwrap();
        (dir, store)
    }
}

fridica_core::conformance_tests!(Sqlite);
