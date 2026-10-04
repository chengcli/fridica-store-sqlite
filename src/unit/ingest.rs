//! Catch-up watermarks, the Socket Mode status, attachment lookups and the
//! worker supervisor's writes.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{Catchup, FileLookups, SocketStatus, Supervision, Watermark};
use rusqlite::{params, OptionalExtension};

impl Catchup for Sqlite<'_> {
    fn catchup_mark(&mut self, workspace: &str, channel: &str) -> Result<Option<f64>> {
        Ok(self
            .0
            .query_row(
                "SELECT last_complete_pass FROM channel_watermarks WHERE workspace=? AND channel=?",
                params![workspace, channel],
                |r| r.get(0),
            )
            .optional()?)
    }
    fn latest_message_ts(
        &mut self,
        workspace: &str,
        channel: &str,
        before: Option<f64>,
    ) -> Result<Option<f64>> {
        Ok(self.0.query_row("SELECT MAX(CAST(ts AS REAL)) FROM messages WHERE workspace=? AND channel=? AND (? IS NULL OR received_at<?)",params![workspace,channel,before,before],|r|r.get(0))?)
    }
    fn recent_thread_roots(
        &mut self,
        workspace: &str,
        channel: &str,
        since: f64,
    ) -> Result<Vec<String>> {
        Ok(self.0.prepare("SELECT root_ts FROM (SELECT root_ts,updated FROM threads WHERE workspace=? AND channel=? ORDER BY updated DESC LIMIT 200) WHERE updated>=?")?
            .query_map(params![workspace,channel,since],|r|r.get(0))?.collect::<rusqlite::Result<Vec<String>>>()?)
    }
    fn truncated_passes(&mut self, workspace: &str, channel: &str) -> Result<Option<String>> {
        let runs_key = format!("catchup:{workspace}:{channel}:truncated");
        Ok(self
            .0
            .query_row("SELECT value FROM meta WHERE key=?", [&runs_key], |r| {
                r.get(0)
            })
            .optional()?)
    }
    fn keep_watermark(&mut self, w: &Watermark) -> Result<()> {
        let (workspace, channel, mark, pinned) = (&w.workspace, &w.channel, w.mark, w.pinned);
        let key = format!("catchup:{workspace}:{channel}");
        let runs_key = format!("{key}:truncated");
        let tx = self.0;
        tx.execute("INSERT INTO channel_watermarks VALUES(?,?,?,?) ON CONFLICT(workspace,channel) DO UPDATE SET last_complete_pass=excluded.last_complete_pass,pinned=excluded.pinned",params![workspace,channel,mark,pinned])?;
        tx.execute(
            "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, format!("{mark:.6}")],
        )?;
        tx.execute(
            "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![runs_key, w.truncated_passes.to_string()],
        )?;
        Ok(())
    }
}

impl SocketStatus for Sqlite<'_> {
    fn socket_status(&mut self) -> Result<Option<String>> {
        Ok(self.0.query_row(
            "SELECT (SELECT value FROM meta WHERE key='slack_status')",
            [],
            |r| r.get(0),
        )?)
    }
    fn keep_socket_status(&mut self, status: &str) -> Result<()> {
        let tx = self.0;
        tx.execute("INSERT INTO meta VALUES('slack_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[status])?;
        tx.execute("UPDATE runtime SET slack_status=? WHERE id=1", [status])?;
        Ok(())
    }
}

impl FileLookups for Sqlite<'_> {
    fn own_uploads(
        &mut self,
        channel: &str,
        thread: &str,
        files: &[(String, String)],
    ) -> Result<Vec<String>> {
        let mut own = vec![];
        let mut ours = self.0.prepare_cached("SELECT EXISTS(SELECT 1 FROM outbox WHERE kind='upload' AND (sent_ts=? OR ((sent_ts IS NULL OR sent_ts='') AND channel=? AND thread_ts=? AND filename=?)))")?;
        for (id, name) in files {
            if ours.query_row(params![id, channel, thread, name], |r| r.get::<_, bool>(0))? {
                own.push(id.clone());
            }
        }
        Ok(own)
    }
    fn session_attachments(&mut self, session: &str) -> Result<Vec<String>> {
        Ok(self.0
            .prepare("SELECT attachments_json FROM messages WHERE workspace||':'||channel||':'||root_ts=?")?
            .query_map([session], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
}

impl Supervision for Sqlite<'_> {
    fn interrupt_worker(&mut self, worker: &str, now: f64) -> Result<()> {
        let tx = self.0;
        tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'owner','worker.interrupt',?,'{}')",params![now,worker])?;
        tx.execute("UPDATE approvals SET status='cancelled',decided_by='system',decided_at=? WHERE worker_id=? AND status='pending'",params![now,worker])?;
        Ok(())
    }
    fn instructions_fingerprint(&mut self, worker: &str) -> Result<Option<String>> {
        let key = format!("worker_instructions:{worker}");
        Ok(self
            .0
            .query_row("SELECT value FROM meta WHERE key=?", [&key], |r| r.get(0))
            .optional()?)
    }
    fn begin_instructions(
        &mut self,
        worker: &str,
        fingerprint: &str,
        fresh_session: bool,
    ) -> Result<()> {
        let key = format!("worker_instructions:{worker}");
        let tx = self.0;
        if fresh_session {
            tx.execute(
                "UPDATE workers SET backend_session_id='' WHERE id=?",
                [&worker],
            )?;
        }
        tx.execute(
            "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, fingerprint],
        )?;
        Ok(())
    }
}
