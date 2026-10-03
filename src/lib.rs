//! One bounded queue and one dedicated SQLite thread; callers never share a connection.
/// The SQLite binding the store's API exposes, until every query is behind
/// the storage traits (#117 stage 2).
pub use rusqlite;
pub mod approvals;
pub mod archive;
pub mod configuration;
pub mod diagnostics;
pub mod fetch;
pub mod links;
pub mod migration;
pub mod outbox;
pub mod record;
pub mod schema;
pub mod work;
pub mod worker_controls;

use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use rusqlite::Connection;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

type Request = Box<dyn FnOnce(&mut Connection) + Send>;
/// What the database thread receives: work, or the order to stop.
enum Message {
    Call(Request),
    Close,
}

pub fn lock(path: &Path) -> Result<File> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
    let path = canonical.as_path();
    if let Some(parent) = path.parent() {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(parent)?;
    }
    let file = private_file(&path.with_extension("lock"))?;
    file.try_lock_exclusive()
        .context("another Fridica process holds the state database")?;
    Ok(file)
}

pub fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

#[derive(Clone)]
pub struct Store {
    sender: mpsc::Sender<Message>,
    /// Turns true once the database thread has closed the connection and
    /// released the lock; `close` waits for it.
    stopped: watch::Receiver<bool>,
}
impl Store {
    /// Existing databases must be explicitly migrated with backups first.
    pub async fn open(path: PathBuf) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Message>(128);
        let (ready, wait) = oneshot::channel();
        let (stopped_tx, stopped) = watch::channel(false);
        std::thread::Builder::new()
            .name("fridica-sqlite".into())
            .spawn(move || {
                let opened = (|| -> Result<_> {
                    let path = std::fs::canonicalize(&path).unwrap_or(path);
                    let guard = lock(&path)?;
                    migration::check_ready(&path)?;
                    private_file(&path)?;
                    let mut c = Connection::open(&path)?;
                    c.busy_timeout(Duration::from_secs(5))?;
                    let version = schema::version(&c)?;
                    if version == 0 {
                        schema::migrate(&mut c)?;
                        schema::install_mutation_guards(&c)?;
                    } else if version != schema::VERSION {
                        anyhow::bail!("schema v{version}: run fridica migrate first");
                    }
                    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
                    Ok((c, guard))
                })();
                match opened {
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                    Ok((mut connection, guard)) => {
                        if ready.send(Ok(())).is_ok() {
                            while let Some(message) = receiver.blocking_recv() {
                                match message {
                                    Message::Call(request) => request(&mut connection),
                                    Message::Close => break,
                                }
                            }
                        }
                        // Release the lock only after the connection is closed.
                        drop(connection);
                        drop(guard);
                    }
                }
                let _ = stopped_tx.send(true);
            })?;
        wait.await
            .context("database thread stopped during startup")??;
        Ok(Self { sender, stopped })
    }
    /// Close the database and release its lock, waiting until that is done,
    /// so another opener (a second `Store`, `migrate`, `rollback`) in the
    /// same process does not race the thread's exit. Every clone of this
    /// store stops working; later calls fail with "database thread stopped".
    pub async fn close(&self) -> Result<()> {
        let _ = self.sender.send(Message::Close).await;
        let mut stopped = self.stopped.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !*stopped.borrow_and_update() {
                if stopped.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .context("database thread did not stop")?;
        Ok(())
    }

    pub async fn call<T, F>(&self, function: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (reply, wait) = oneshot::channel();
        self.sender
            .send(Message::Call(Box::new(move |connection| {
                // A panicking adapter cannot silently kill the only database thread.
                let mut result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| function(connection)))
                        .unwrap_or_else(|_| Err(anyhow!("database operation panicked")));
                if !connection.is_autocommit() {
                    let _ = connection.execute_batch("ROLLBACK");
                    result = Err(anyhow!(
                        "database operation left an uncommitted transaction"
                    ));
                }
                let _ = reply.send(result);
            })))
            .await
            .map_err(|_| anyhow!("database thread stopped"))?;
        wait.await.context("database request cancelled")?
    }
}
