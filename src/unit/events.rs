//! Recorded Slack names, ledger lookups, the GitHub read pause and the event
//! feed's single reads.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{
    FeedLookups, GithubPause, LedgerLookups, OutboxPost, RecordedNames, SlackIdentity, SlackNames,
};
use rusqlite::{params, OptionalExtension};

impl Sqlite<'_> {
    fn meta(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.0
            .prepare_cached("SELECT value FROM meta WHERE key=?")?
            .query_row([key], |r| r.get(0))
            .optional()
    }
}

impl SlackNames for Sqlite<'_> {
    fn slack_names(&mut self) -> Result<RecordedNames> {
        Ok(RecordedNames {
            workspace: self.meta("slack_workspace_name")?,
            channels: self.meta("slack_channel_names")?,
            users: self.meta("slack_user_names")?,
        })
    }
    fn user_names(&mut self) -> Result<Option<String>> {
        Ok(self.meta("slack_user_names")?)
    }
    fn keep_user_names(&mut self, users: &str) -> Result<()> {
        self.0.execute(
            "INSERT INTO meta VALUES('slack_user_names',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [users],
        )?;
        Ok(())
    }
    fn keep_identity(&mut self, identity: &SlackIdentity) -> Result<()> {
        let c = self.0;
        c.execute("INSERT INTO meta VALUES('slack_scopes',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[&identity.scopes])?;
        c.execute("INSERT INTO meta VALUES('slack_channel_names',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[&identity.channels])?;
        c.execute("INSERT INTO meta VALUES('slack_workspace_name',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[&identity.workspace])?;
        Ok(())
    }
}

impl LedgerLookups for Sqlite<'_> {
    fn intake_senders_after(&mut self, seq: i64) -> Result<Vec<String>> {
        Ok(self
            .0
            .prepare("SELECT json_extract(payload_json,'$.message.sender') FROM replay_events WHERE seq>? AND kind='intake' ORDER BY seq LIMIT 500")?
            .query_map([seq], |r| r.get::<_, Option<String>>(0))?
            .filter_map(|r| r.ok().flatten())
            .collect())
    }
    fn has_recent_intake(&mut self, event_id: &str, seq: i64, time: f64) -> Result<bool> {
        Ok(self.0.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM replay_events WHERE kind='intake' AND seq<?1 AND seq>=?1-1000 AND time=?2 AND json_extract(payload_json,'$.message.event_id')=?3)",
        )?
        .query_row(params![seq, time, event_id], |r| r.get::<_, bool>(0))?)
    }
    fn attachment_context(&mut self, key: &str) -> Result<Option<String>> {
        Ok(self.0.query_row("SELECT json_extract(payload_json,'$.context') FROM replay_events WHERE kind='parent_attachment_result' AND complete=1 AND json_extract(payload_json,'$.key')=? ORDER BY seq DESC LIMIT 1",[key],|r|r.get::<_,String>(0)).optional()?)
    }
    fn backfill_record(&mut self, client_id: &str) -> Result<Option<String>> {
        Ok(self.0.query_row("SELECT payload_json FROM replay_events WHERE kind='obligations_backfill' AND json_extract(payload_json,'$.request.client_id')=?",[client_id],|r|r.get(0)).optional()?)
    }
}

impl FeedLookups for Sqlite<'_> {
    fn outbox_post(&mut self, id: i64) -> Result<Option<OutboxPost>> {
        Ok(self
            .0
            .prepare_cached("SELECT kind,session_id FROM outbox WHERE id=?")?
            .query_row([id], |r| {
                Ok(OutboxPost {
                    kind: r.get(0)?,
                    session: r.get(1)?,
                })
            })
            .optional()?)
    }
    fn job_session(&mut self, id: &str) -> Result<Option<String>> {
        Ok(self
            .0
            .prepare_cached("SELECT session_id FROM jobs WHERE id=?")?
            .query_row([id], |r| r.get(0))
            .optional()?)
    }
    fn message_received_at(&mut self, event_id: &str) -> Result<Option<f64>> {
        Ok(self
            .0
            .prepare_cached("SELECT received_at FROM messages WHERE event_id=?")?
            .query_row([event_id], |r| r.get(0))
            .optional()?)
    }
}

impl GithubPause for Sqlite<'_> {
    fn github_paused_until(&mut self) -> Result<Option<String>> {
        Ok(self
            .0
            .query_row(
                "SELECT value FROM meta WHERE key='github:read:pause_until'",
                [],
                |r| r.get(0),
            )
            .optional()?)
    }
    fn pause_github(&mut self, until: f64) -> Result<()> {
        self.0.execute(
            "INSERT INTO meta(key,value) VALUES('github:read:pause_until',?) ON CONFLICT(key) DO UPDATE SET value=MAX(CAST(value AS REAL),CAST(excluded.value AS REAL))",
            [until.to_string()],
        )?;
        Ok(())
    }
}
