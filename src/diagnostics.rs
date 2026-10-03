//! Optional diagnostic metadata. Never opens the normal store or migrates state.
use std::path::Path;
pub async fn slack_scopes(path: &Path) -> Option<String> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let db =
            rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .ok()?;
        db.busy_timeout(std::time::Duration::from_millis(100))
            .ok()?;
        db.query_row(
            "SELECT value FROM meta WHERE key = 'slack_scopes' AND length(value) <= 65536",
            [],
            |row| row.get(0),
        )
        .ok()
    })
    .await
    .ok()
    .flatten()
}
