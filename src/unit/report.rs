//! The daily channel report.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{ChannelActivity, PendingExport, Report, Reports};
use rusqlite::{params, OptionalExtension};

impl Reports for Sqlite<'_> {
    fn channel_activity(&mut self, channel: &str, from: f64, to: f64) -> Result<ChannelActivity> {
        let count = |sql: &str| -> Result<i64> {
            Ok(self
                .0
                .query_row(sql, params![channel, from, to], |r| r.get(0))?)
        };
        Ok(ChannelActivity {
            messages: count("SELECT count(*) FROM messages WHERE channel=? AND received_at>=? AND received_at<? AND source!='self'")?,
            mentions: count("SELECT count(*) FROM messages WHERE channel=? AND received_at>=? AND received_at<? AND mentions_owner=1")?,
            replies_delivered: count("SELECT count(*) FROM outbox WHERE channel=? AND delivered_at>=? AND delivered_at<? AND state='sent' AND kind='reply'")?,
            replies_ambiguous: count("SELECT count(*) FROM outbox WHERE channel=? AND created>=? AND created<? AND state='ambiguous'")?,
            obligations_open: count("SELECT count(*) FROM obligations o JOIN threads t ON t.id=o.session_id WHERE t.channel=? AND o.created>=? AND o.created<? AND o.state IN ('open','deferred','awaiting_delivery')")?,
            obligations_answered: count("SELECT count(*) FROM obligations o JOIN threads t ON t.id=o.session_id WHERE t.channel=? AND o.updated>=? AND o.updated<? AND o.state='answered'")?,
            jobs_finished: count("SELECT count(*) FROM jobs j JOIN threads t ON t.id=j.session_id WHERE t.channel=? AND j.finished_at>=? AND j.finished_at<? AND j.status IN ('done','failed','interrupted','cancelled')")?,
        })
    }
    fn campaign_items_updated(&mut self, from: f64, to: f64) -> Result<i64> {
        Ok(self.0.query_row(
            "SELECT count(*) FROM work_items WHERE updated>=? AND updated<?",
            params![from, to],
            |r| r.get(0),
        )?)
    }
    fn keep_report(&mut self, report: &Report) -> Result<()> {
        self.0.execute("INSERT INTO reports(channel,day,timezone,data_json,markdown,created) VALUES(?,?,?,?,?,?)
             ON CONFLICT(channel,day) DO UPDATE SET timezone=excluded.timezone,data_json=excluded.data_json,markdown=excluded.markdown,created=excluded.created",
            params![report.channel, report.day, report.timezone, report.data, report.markdown, report.created])?;
        self.0.execute("INSERT INTO report_exports(channel,day) VALUES(?,?) ON CONFLICT(channel,day) DO UPDATE SET generation=generation+1,state='pending',error=''", params![report.channel, report.day])?;
        Ok(())
    }
    fn pending_exports(&mut self) -> Result<Vec<PendingExport>> {
        let exports = self.0.prepare("SELECT r.channel,r.day,r.markdown,e.generation FROM reports r JOIN report_exports e USING(channel,day) WHERE e.state='pending' ORDER BY r.day,r.channel")?
            .query_map([], |r| {
                Ok(PendingExport {
                    channel: r.get(0)?,
                    day: r.get(1)?,
                    markdown: r.get(2)?,
                    generation: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(exports)
    }
    fn pending_export(&mut self, channel: &str, day: &str) -> Result<Option<i64>> {
        Ok(self
            .0
            .query_row(
                "SELECT generation FROM report_exports WHERE channel=? AND day=? AND state='pending'",
                params![channel, day],
                |r| r.get(0),
            )
            .optional()?)
    }
    fn mark_exported(&mut self, channel: &str, day: &str, generation: i64) -> Result<usize> {
        Ok(self.0.execute(
            "UPDATE report_exports SET state='done' WHERE channel=? AND day=? AND generation=?",
            params![channel, day, generation],
        )?)
    }
    fn queue_report_post(
        &mut self,
        channel: &str,
        day: &str,
        idem_key: &str,
        session: &str,
        created: f64,
    ) -> Result<bool> {
        let exists: bool = self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM report_posts WHERE channel=? AND day=?)",
            params![channel, day],
            |r| r.get(0),
        )?;
        if exists {
            return Ok(false);
        }
        let markdown: String = self.0.query_row(
            "SELECT markdown FROM reports WHERE channel=? AND day=?",
            params![channel, day],
            |r| r.get(0),
        )?;
        self.0.execute("INSERT INTO outbox(idem_key,session_id,kind,channel,text,created) VALUES(?,?,'report',?,?,?)", params![idem_key, session, channel, markdown, created])?;
        self.0.execute(
            "INSERT INTO report_posts VALUES(?,?,?)",
            params![channel, day, self.0.last_insert_rowid()],
        )?;
        Ok(true)
    }
}
