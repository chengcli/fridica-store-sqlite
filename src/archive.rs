//! Weekly archives (#114): threads that finished and stayed quiet, and
//! completed replay events, move out of the live database into one SQLite
//! file per ISO week beside it (`<db>.archive/2026-W40.sqlite3`), so the live
//! database stays small. A thread keeps every row it owns when it moves; a
//! new message in an archived thread brings it back before it is handled
//! (`revive`), and `search` finds archived threads by their text.
//!
//! Rows are copied through a second connection: written to the archive
//! first, then deleted from the live database. A crash between the two leaves
//! a copy in both, which the next round replaces, never a loss.
//!
//! Numbered rows (messages, inbox items, posts, parent turns, notes) take new
//! numbers wherever they are copied, and the inbox and post numbers other
//! rows refer to (`inbox_id`, `outbox_id`) follow them. SQLite gives a deleted
//! highest number to the next new row, so a number kept from the archive
//! could belong to another thread by the time this one comes back.
use super::schema;
use anyhow::{Context, Result};
use rusqlite::{params, params_from_iter, types::Value, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

/// What a thread owns (`?1` is its id), parents before children. Filters
/// with subqueries read the database the rows come from.
const THREAD_ROWS: &[(&str, &str)] = &[
    ("threads", "id=?1"),
    ("messages", "workspace||':'||channel||':'||root_ts=?1"),
    ("workers", "session_id=?1"),
    ("thread_inbox", "session_id=?1"),
    ("jobs", "session_id=?1"),
    (
        "job_progress",
        "job_id IN (SELECT id FROM jobs WHERE session_id=?1)",
    ),
    ("artifacts", "session_id=?1"),
    ("approvals", "session_id=?1"),
    ("outbox", "session_id=?1"),
    ("obligations", "session_id=?1"),
    (
        "obligation_posts",
        "obligation_id IN (SELECT id FROM obligations WHERE session_id=?1)",
    ),
    (
        "report_posts",
        "outbox_id IN (SELECT id FROM outbox WHERE session_id=?1)",
    ),
    ("reply_reservations", "session_id=?1"),
    ("parent_turns", "session_id=?1"),
    ("notes", "session_id=?1"),
    ("item_links", "session_id=?1"),
    ("thread_links", "session_id=?1"),
];
/// Rows of other threads that point at this one, removed from the live
/// database with it (their foreign keys would break) but not archived with
/// it: a link to it, or another thread's ask settled by one of its posts.
const POINTING_IN: &[(&str, &str)] = &[
    ("thread_links", "target=?1"),
    (
        "obligation_posts",
        "outbox_id IN (SELECT id FROM outbox WHERE session_id=?1)",
    ),
];
/// Tables whose rows are numbered (`id INTEGER PRIMARY KEY`) and renumbered
/// when copied; the second name is the column other rows refer to them by.
const NUMBERED: &[(&str, &str)] = &[
    ("messages", ""),
    ("thread_inbox", "inbox_id"),
    ("outbox", "outbox_id"),
    ("parent_turns", ""),
    ("notes", ""),
];
/// Replay events a feature still looks up by key stay live.
const KEPT_EVENTS: &[&str] = &["obligations_backfill"];

/// The archive directory of a state database.
pub fn directory(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(".archive");
    PathBuf::from(name)
}
/// The ISO week a Unix time falls in, as the archive's file stem.
pub fn week(time: f64) -> String {
    chrono::DateTime::from_timestamp(time.max(0.) as i64, 0)
        .unwrap_or_default()
        .format("%G-W%V")
        .to_string()
}
fn file(db: &Path, week: &str) -> PathBuf {
    directory(db).join(format!("{week}.sqlite3"))
}

/// An archive, created on first use with the live schema. Archives hold
/// copies, so they enforce no foreign keys.
fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    let mut c = Connection::open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    if schema::version(&c)? < schema::VERSION {
        schema::migrate(&mut c)?;
    }
    c.execute_batch("PRAGMA foreign_keys=OFF;")?;
    Ok(c)
}

fn columns(c: &Connection, table: &str) -> Result<Vec<String>> {
    Ok(
        c.prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?,
    )
}
/// New numbers given to copied rows, by the column that refers to them.
type Numbers = HashMap<&'static str, HashMap<i64, i64>>;
/// Copy the rows of `table` matching `filter` from one database to another,
/// never replacing a row already there. Numbered rows take new numbers (kept
/// in `numbers`) and references to rows copied before follow them. With
/// `checked`, a row whose foreign-key parent is missing in `to` is left out
/// (a link to a thread that stays archived). With `numbers` None, rows keep
/// their numbers and replace earlier copies (replay events, numbered once).
fn copy(
    from: &Connection,
    to: &Connection,
    table: &str,
    filter: &str,
    key: &[&dyn rusqlite::ToSql],
    checked: bool,
    mut numbers: Option<&mut Numbers>,
) -> Result<usize> {
    let all = columns(from, table)?;
    let numbered = numbers
        .as_ref()
        .and_then(|_| NUMBERED.iter().find(|(t, _)| *t == table));
    let cols: Vec<&String> = all
        .iter()
        .filter(|c| numbered.is_none() || c.as_str() != "id")
        .collect();
    let list = cols
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let marks = vec!["?"; cols.len()].join(",");
    let parents: Vec<(String, String, String)> = if checked {
        to.prepare(&format!(
            "SELECT \"table\",\"from\",\"to\" FROM pragma_foreign_key_list('{table}')"
        ))?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?
    } else {
        vec![]
    };
    let verb = if numbers.is_some() {
        "INSERT OR IGNORE"
    } else {
        "INSERT OR REPLACE"
    };
    let id_at = all.iter().position(|c| c == "id");
    let mut select = from.prepare(&format!(
        "SELECT {} FROM {table} WHERE {filter}",
        all.iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(",")
    ))?;
    let mut insert = to.prepare(&format!("{verb} INTO {table}({list}) VALUES({marks})"))?;
    let mut rows = select.query(key)?;
    let mut count = 0;
    'rows: while let Some(row) = rows.next()? {
        let mut values: Vec<(String, Value)> = Vec::with_capacity(all.len());
        for (i, column) in all.iter().enumerate() {
            values.push((column.clone(), row.get::<_, Value>(i)?));
        }
        let old_id = id_at.and_then(|i| match values[i].1 {
            Value::Integer(n) => Some(n),
            _ => None,
        });
        if let Some(numbers) = numbers.as_deref() {
            for (column, value) in values.iter_mut() {
                if let (Some(map), Value::Integer(n)) = (numbers.get(column.as_str()), &*value) {
                    if let Some(new) = map.get(n) {
                        *value = Value::Integer(*new);
                    }
                }
            }
        }
        let values: Vec<(String, Value)> = values
            .into_iter()
            .filter(|(c, _)| numbered.is_none() || c != "id")
            .collect();
        for (parent, column, target) in &parents {
            let Some((_, value)) = values.iter().find(|(c, _)| c == column) else {
                continue;
            };
            if *value != Value::Null
                && !to.query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {parent} WHERE \"{target}\"=?)"),
                    [value],
                    |r| r.get::<_, bool>(0),
                )?
            {
                continue 'rows;
            }
        }
        if insert.execute(params_from_iter(values.into_iter().map(|(_, v)| v)))? == 0 {
            continue;
        }
        count += 1;
        if let (Some((_, by)), Some(old), Some(numbers)) =
            (numbered, old_id, numbers.as_deref_mut())
        {
            if !by.is_empty() {
                numbers
                    .entry(by)
                    .or_default()
                    .insert(old, to.last_insert_rowid());
            }
        }
    }
    Ok(count)
}
/// Remove what a thread owns from one database, children first; in the live
/// database also what points in at it from other threads.
fn delete(c: &Connection, session: &str, live: bool) -> Result<()> {
    if live {
        for (table, filter) in POINTING_IN {
            c.execute(&format!("DELETE FROM {table} WHERE {filter}"), [session])?;
        }
    }
    for (table, filter) in THREAD_ROWS.iter().rev() {
        c.execute(&format!("DELETE FROM {table} WHERE {filter}"), [session])?;
    }
    Ok(())
}
fn copy_thread(from: &Connection, to: &Connection, session: &str, checked: bool) -> Result<()> {
    let mut numbers = Numbers::new();
    for (table, filter) in THREAD_ROWS {
        copy(
            from,
            to,
            table,
            filter,
            &[&session],
            checked,
            Some(&mut numbers),
        )?;
    }
    Ok(())
}

/// What one maintenance round moved.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct Round {
    pub threads: usize,
    pub events: usize,
}

/// Threads idle for `after` seconds with nothing pending, oldest first: no
/// inbox item waiting, job queued or running, worker busy, ask open or post
/// unsent, and not paused (an owner pause awaits the owner).
fn quiet(c: &Connection, cutoff: f64, limit: usize) -> Result<Vec<(String, f64)>> {
    Ok(c.prepare(
        "SELECT id,last FROM (SELECT t.id,MAX(t.updated,t.created,COALESCE((SELECT MAX(m.received_at) FROM messages m
            WHERE m.workspace=t.workspace AND m.channel=t.channel AND m.root_ts=t.root_ts),0)) AS last FROM threads t
          WHERE t.control!='paused'
            AND NOT EXISTS(SELECT 1 FROM thread_inbox i WHERE i.session_id=t.id AND i.state IN ('pending','processing'))
            AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.session_id=t.id AND j.status IN ('queued','running'))
            AND NOT EXISTS(SELECT 1 FROM workers w WHERE w.session_id=t.id AND w.status NOT IN ('idle','stopped','lost'))
            AND NOT EXISTS(SELECT 1 FROM obligations o WHERE o.session_id=t.id AND o.state IN ('open','deferred','awaiting_delivery'))
            AND NOT EXISTS(SELECT 1 FROM outbox p WHERE p.session_id=t.id AND p.state IN ('pending','sending','ambiguous','blocked'))
            AND NOT EXISTS(SELECT 1 FROM approvals a WHERE a.session_id=t.id AND a.status='pending'))
         WHERE last<? ORDER BY last,id LIMIT ?",
    )?
    .query_map(params![cutoff, limit as i64], |r| Ok((r.get(0)?, r.get(1)?)))?
    .collect::<rusqlite::Result<_>>()?)
}

/// What a round may move: threads quiet for `threads_after` seconds (up to
/// `threads`), and completed replay events older than `events_after` seconds
/// (up to `events`). An age of 0 or less moves nothing of that kind.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub threads_after: f64,
    pub threads: usize,
    pub events_after: f64,
    pub events: usize,
}
/// Move what `limits` allows into the weekly archives. A thread archived
/// before (and revived since) leaves its old week's copy too.
pub fn round(c: &mut Connection, db: &Path, now: f64, limits: Limits) -> Result<Round> {
    let mut done = Round::default();
    if !now.is_finite() {
        return Ok(done);
    }
    let quiet_threads = if limits.threads_after > 0. {
        quiet(c, now - limits.threads_after, limits.threads)?
    } else {
        vec![]
    };
    for (session, last) in quiet_threads {
        let path = file(db, &week(last));
        let earlier: Option<String> = c
            .query_row(
                "SELECT archive FROM archived_threads WHERE session_id=?",
                [&session],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(earlier) = earlier.as_deref().filter(|e| Path::new(e) != path) {
            if Path::new(earlier).exists() {
                let old = open(Path::new(earlier))?;
                let tx = old.unchecked_transaction()?;
                delete(&tx, &session, false)?;
                tx.commit()?;
            }
        }
        let archive = open(&path)?;
        {
            let tx = archive.unchecked_transaction()?;
            delete(&tx, &session, false)?;
            copy_thread(c, &tx, &session, false)?;
            tx.commit()?;
        }
        let tx = c.transaction()?;
        delete(&tx, &session, true)?;
        tx.execute(
            "INSERT INTO archived_threads(session_id,archive,last_activity,archived_at,restored_at) VALUES(?,?,?,?,NULL)
             ON CONFLICT(session_id) DO UPDATE SET archive=excluded.archive,last_activity=excluded.last_activity,
             archived_at=excluded.archived_at,restored_at=NULL",
            params![session, path.to_string_lossy(), last, now],
        )?;
        tx.commit()?;
        done.threads += 1;
    }
    // Completed events only (an incomplete one is a pending intent), never the
    // newest (its number would be given out again), and not those a feature
    // still looks up by key.
    let kept = serde_json::to_string(KEPT_EVENTS)?;
    let rows: Vec<(i64, f64)> = if limits.events_after > 0. {
        c.prepare(
            "SELECT seq,time FROM replay_events WHERE complete=1 AND time<?1
               AND seq<(SELECT MAX(seq) FROM replay_events)
               AND kind NOT IN (SELECT value FROM json_each(?3)) ORDER BY seq LIMIT ?2",
        )?
        .query_map(
            params![now - limits.events_after, limits.events as i64, kept],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .collect::<rusqlite::Result<_>>()?
    } else {
        vec![]
    };
    let mut by_week: std::collections::BTreeMap<String, Vec<i64>> = Default::default();
    for (seq, time) in rows {
        by_week.entry(week(time)).or_default().push(seq);
    }
    for (week, seqs) in by_week {
        let ids = serde_json::to_string(&seqs)?;
        let archive = open(&file(db, &week))?;
        {
            let tx = archive.unchecked_transaction()?;
            copy(
                c,
                &tx,
                "replay_events",
                "seq IN (SELECT value FROM json_each(?1))",
                &[&ids],
                false,
                None,
            )?;
            tx.commit()?;
        }
        c.execute(
            "DELETE FROM replay_events WHERE seq IN (SELECT value FROM json_each(?1))",
            [&ids],
        )?;
        done.events += seqs.len();
    }
    // Return the freed pages to the file system a little at a time.
    if done != Round::default() {
        let incremental: i64 = c.query_row("PRAGMA auto_vacuum", [], |r| r.get(0))?;
        if incremental == 2 {
            c.query_row("PRAGMA incremental_vacuum(4096)", [], |_| Ok(()))
                .optional()?;
        }
    }
    Ok(done)
}

/// Bring an archived thread back into the live database, inside the caller's
/// transaction (a message arrived in it, or the owner asked). Its rows take
/// new numbers and never replace a live row; a row whose foreign-key parent is
/// not live (a link to a thread still archived) is left out. Its rows stay in
/// the archive until the thread is archived again.
pub fn revive_tx(c: &Connection, session: &str, now: f64) -> Result<bool> {
    let row: Option<String> = c
        .query_row(
            "SELECT archive FROM archived_threads WHERE session_id=? AND restored_at IS NULL",
            [session],
            |r| r.get(0),
        )
        .optional()?;
    let Some(path) = row else {
        return Ok(false);
    };
    let exists: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM threads WHERE id=?)",
        [session],
        |r| r.get(0),
    )?;
    if !exists {
        let archive = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("archive {path} is missing"))?;
        copy_thread(&archive, c, session, true)?;
    }
    c.execute(
        "UPDATE archived_threads SET restored_at=? WHERE session_id=?",
        params![now, session],
    )?;
    Ok(true)
}
/// `revive_tx` for intake: an archive that cannot be read (moved, deleted,
/// damaged) must not block messages, so a failed revival is undone, noted as
/// a health event, and the message starts the thread afresh.
pub fn revive_or_note(c: &Connection, session: &str, now: f64) -> Result<()> {
    c.execute_batch("SAVEPOINT revive")?;
    match revive_tx(c, session, now) {
        Ok(_) => c.execute_batch("RELEASE revive")?,
        Err(error) => {
            c.execute_batch("ROLLBACK TO revive; RELEASE revive")?;
            c.execute(
                "INSERT INTO health_events(kind,details_json,created) VALUES('archive_revive_failed',?,?)",
                params![json!({"session":session,"error":error.to_string()}).to_string(), now],
            )?;
        }
    }
    Ok(())
}

/// A thread found in an archive.
#[derive(Debug, Serialize)]
pub struct Hit {
    pub thread: String,
    pub week: String,
    pub last_activity: f64,
    pub summary: String,
    pub matches: Vec<String>,
}
/// Archived threads whose messages or summary contain `query` (case
/// insensitive), newest week first. Reads the archive files only.
pub fn search(db: &Path, query: &str, limit: usize) -> Result<Vec<Hit>> {
    let mut weeks: Vec<PathBuf> = match std::fs::read_dir(directory(db)) {
        Ok(entries) => entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "sqlite3"))
            .collect(),
        Err(_) => vec![],
    };
    weeks.sort();
    weeks.reverse();
    let pattern = format!(
        "%{}%",
        query
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    let mut hits = vec![];
    for path in weeks {
        let c = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let week = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let threads: Vec<(String, String, f64)> = c
            .prepare(
                "SELECT t.id,t.summary,MAX(t.updated,t.created) FROM threads t WHERE t.summary LIKE ?1 ESCAPE '\\'
                 OR EXISTS(SELECT 1 FROM messages m WHERE m.workspace=t.workspace AND m.channel=t.channel AND m.root_ts=t.root_ts
                   AND m.text LIKE ?1 ESCAPE '\\') ORDER BY t.updated DESC",
            )?
            .query_map([&pattern], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        for (thread, summary, last) in threads {
            let matches: Vec<String> = c
                .prepare(
                    "SELECT ts||' '||sender||': '||substr(text,1,200) FROM messages WHERE workspace||':'||channel||':'||root_ts=?1
                     AND text LIKE ?2 ESCAPE '\\' ORDER BY CAST(ts AS REAL) LIMIT 3",
                )?
                .query_map(params![thread, pattern], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            hits.push(Hit {
                thread,
                week: week.clone(),
                last_activity: last,
                summary: summary.chars().take(300).collect(),
                matches,
            });
            if hits.len() >= limit {
                return Ok(hits);
            }
        }
    }
    Ok(hits)
}
