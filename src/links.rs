//! The channel ledger (#108): which threads of a channel refer to which pull
//! request or issue, and to which other threads. Links are extracted from
//! message text by fixed patterns when a message is stored; the parent then
//! sees the linked threads' state on every turn. Links never leave a channel.
use anyhow::Result;
use regex::Regex;
use rusqlite::{params, Connection};
use std::sync::LazyLock;

/// What a message's text refers to.
#[derive(Debug, Default, PartialEq)]
pub struct References {
    /// `(item, repository)`: the item is `#<number>`; the repository is the
    /// name a message gave it (`snapy`, from `chengcli/snapy#269` or a GitHub
    /// URL), or empty for a bare `#269`.
    pub items: Vec<(String, String)>,
    /// Thread or message timestamps, from `1790927185.684379` or a permalink.
    pub threads: Vec<String>,
}

static URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"github\.com/[A-Za-z0-9-]+/([A-Za-z0-9._-]+?)(?:\.git)?/(?:pull|issues)/([0-9]{1,6})\b",
    )
    .unwrap()
});
static QUALIFIED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[A-Za-z0-9-]+/([A-Za-z0-9._-]+)#([0-9]{1,6})\b").unwrap());
// `#269`, not `&#123;` (an HTML entity), `foo/bar#1` (qualified above) or a
// colour such as `#333333`.
static BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|[^\w&/#])#([0-9]{2,5})\b").unwrap());
static TIMESTAMP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(1[0-9]{9}\.[0-9]{6})\b").unwrap());
static PERMALINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/archives/[A-Z0-9]+/p(1[0-9]{9})([0-9]{6})\b").unwrap());

pub fn references(text: &str) -> References {
    let mut found = References::default();
    let mut item = |number: &str, repo: &str| {
        let key = format!("#{}", number.trim_start_matches('0'));
        let repo = repo.trim_end_matches(".git").to_ascii_lowercase();
        if key.len() < 3 {
            return;
        }
        match found.items.iter_mut().find(|(k, _)| *k == key) {
            Some((_, known)) if known.is_empty() => *known = repo,
            Some(_) => {}
            None => found.items.push((key, repo)),
        }
    };
    for c in URL.captures_iter(text) {
        item(&c[2], &c[1]);
    }
    for c in QUALIFIED.captures_iter(text) {
        item(&c[2], &c[1]);
    }
    for c in BARE.captures_iter(text) {
        item(&c[1], "");
    }
    let mut thread = |ts: String| {
        if !found.threads.contains(&ts) {
            found.threads.push(ts);
        }
    };
    for c in PERMALINK.captures_iter(text) {
        thread(format!("{}.{}", &c[1], &c[2]));
    }
    for c in TIMESTAMP.captures_iter(text) {
        thread(c[1].to_owned());
    }
    found
}

/// Record a stored message's references for its thread, when that thread
/// exists (a top-level post of Fridica's own, such as a debrief, has none). A timestamp links
/// only to a thread of the same channel that exists (the thread itself, or the
/// thread a reply with that timestamp belongs to); a thread never links to
/// itself.
pub fn record_tx(
    c: &Connection,
    workspace: &str,
    channel: &str,
    session: &str,
    text: &str,
    now: f64,
) -> Result<()> {
    let found = references(text);
    for (item, repo) in &found.items {
        c.execute(
            "INSERT INTO item_links(workspace,channel,item,session_id,repo,first_seen,last_seen)
             SELECT ?1,?2,?3,?4,?5,?6,?7 WHERE EXISTS(SELECT 1 FROM threads WHERE id=?4)
             ON CONFLICT(workspace,channel,item,session_id) DO UPDATE SET last_seen=MAX(last_seen,excluded.last_seen),
             repo=CASE WHEN repo='' THEN excluded.repo ELSE repo END",
            params![workspace, channel, item, session, repo, now, now],
        )?;
    }
    for ts in &found.threads {
        c.execute(
            "INSERT OR IGNORE INTO thread_links(session_id,target,created)
             SELECT ?1,t.id,?5 FROM threads t WHERE t.id!=?1 AND EXISTS(SELECT 1 FROM threads WHERE id=?1) AND t.id=COALESCE(
                (SELECT workspace||':'||channel||':'||root_ts FROM messages WHERE workspace=?2 AND channel=?3 AND ts=?4 LIMIT 1),
                ?2||':'||?3||':'||?4)",
            params![session, workspace, channel, ts, now],
        )?;
    }
    Ok(())
}

/// Link the messages of the last `window` seconds once, so a database that
/// predates the ledger starts with its recent threads linked. Idempotent.
pub fn backfill_tx(c: &Connection, now: f64, window: f64) -> Result<usize> {
    let done: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key='links_backfilled')",
        [],
        |r| r.get(0),
    )?;
    if done {
        return Ok(0);
    }
    let rows: Vec<(String, String, String, String, f64)> = c
        .prepare(
            "SELECT workspace,channel,workspace||':'||channel||':'||root_ts,text,received_at FROM messages
             WHERE received_at>=? ORDER BY received_at,id",
        )?
        .query_map([now - window], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    for (workspace, channel, session, text, at) in &rows {
        record_tx(c, workspace, channel, session, text, *at)?;
    }
    c.execute(
        "INSERT INTO meta VALUES('links_backfilled',?)",
        [now.to_string()],
    )?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn references_name_items_and_threads_and_skip_noise() {
        let found = references(
            "Review https://github.com/chengcli/snapy/pull/269 and chengcli/kintera#138; \
             also #273, &#123; #7 and #0099. See thread 1790927185.684379 and \
             https://x.slack.com/archives/C0C2D3PCW20/p1790956209867469 (twice: 1790927185.684379).",
        );
        assert_eq!(
            found.items,
            [
                ("#269".to_string(), "snapy".to_string()),
                ("#138".to_string(), "kintera".to_string()),
                ("#273".to_string(), String::new()),
                ("#99".to_string(), String::new()),
            ]
        );
        assert_eq!(found.threads, ["1790956209.867469", "1790927185.684379"]);
        // A later named mention fills the repository of a bare one.
        assert_eq!(
            references("#269 first, then snapy/x#2 and owner/snapy#269").items,
            [("#269".to_string(), "snapy".to_string())]
        );
    }
}
