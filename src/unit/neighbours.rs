//! What a thread's actor keeps beside its turns: the runtime row, thread
//! memory, worker-result snapshots, reply evidence, debriefs, progress notes
//! and the scheduling sweep.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{
    DebriefOrigin, DebriefPost, DebriefTurn, Debriefs, InboxEntry, LastReply, ProgressNote,
    ProgressState, ReplyEvidence, ResultSnapshots, Runtime, RuntimeStart, ThreadMemory,
    ThreadTurns,
};
use rusqlite::{params, OptionalExtension};
use serde_json::json;

impl Runtime for Sqlite<'_> {
    fn previous_slack_status(&mut self) -> Result<Option<String>> {
        Ok(self.0.query_row(
            "SELECT (SELECT slack_status FROM runtime WHERE id=1)",
            [],
            |r| r.get(0),
        )?)
    }
    fn start_runtime(&mut self, s: &RuntimeStart) -> Result<()> {
        self.0.execute("INSERT INTO runtime(id,pid,started_at,heartbeat_at,slack_status,observe_only,control_socket,config_fingerprint) VALUES(1,?,?,?,'starting',?,'',?) ON CONFLICT(id) DO UPDATE SET pid=excluded.pid,started_at=excluded.started_at,heartbeat_at=excluded.heartbeat_at,slack_status=excluded.slack_status,observe_only=excluded.observe_only,control_socket='',config_fingerprint=excluded.config_fingerprint",
            params![s.pid,s.started_at,s.started_at,s.observe_only,s.config_fingerprint])?;
        Ok(())
    }
    fn advertise_control(&mut self, endpoint: &str) -> Result<()> {
        self.0
            .execute("UPDATE runtime SET control_socket=? WHERE id=1", [endpoint])?;
        Ok(())
    }
    fn heartbeat(&mut self, now: f64) -> Result<()> {
        self.0
            .execute("UPDATE runtime SET heartbeat_at=? WHERE id=1", [now])?;
        Ok(())
    }
    fn stop_runtime(&mut self, now: f64) -> Result<()> {
        self.0.execute(
            "UPDATE runtime SET slack_status='stopped',heartbeat_at=? WHERE id=1",
            [now],
        )?;
        Ok(())
    }
}

impl ThreadMemory for Sqlite<'_> {
    fn latest_notes(&mut self, session: &str) -> Result<Option<(i64, String)>> {
        Ok(self.0.query_row("SELECT revision,data_json FROM notes WHERE session_id=? ORDER BY revision DESC LIMIT 1", [session], |r| Ok((r.get(0)?,r.get(1)?))).optional()?)
    }
    fn thread_decisions(&mut self, session: &str) -> Result<String> {
        Ok(self.0.query_row(
            "SELECT decisions_json FROM threads WHERE id=?",
            [session],
            |r| r.get(0),
        )?)
    }
    fn keep_decisions(&mut self, session: &str, decisions: &str) -> Result<()> {
        self.0.execute(
            "UPDATE threads SET decisions_json=? WHERE id=?",
            params![decisions, session],
        )?;
        Ok(())
    }
    fn post_queued(&mut self, session: &str, idem_key: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM outbox WHERE session_id=? AND idem_key=?)",
            params![session, idem_key],
            |r| r.get(0),
        )?)
    }
    fn write_parent_notes(
        &mut self,
        session: &str,
        revision: i64,
        data: &str,
        inbox: i64,
        now: f64,
    ) -> Result<()> {
        self.0.execute("INSERT INTO notes(session_id,revision,actor,data_json,source,created) VALUES(?,?,'parent',?,?,?)",params![session,revision,data,inbox.to_string(),now])?;
        self.0.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'parent','notes.write',?,?)",params![now,session,json!({"revision":revision,"inbox_id":inbox}).to_string()])?;
        Ok(())
    }
}

impl ResultSnapshots for Sqlite<'_> {
    fn job_group(&mut self, job: &str, session: &str) -> Result<Option<String>> {
        Ok(self
            .0
            .query_row(
                "SELECT join_group FROM jobs WHERE id=? AND session_id=?",
                [job, session],
                |r| r.get(0),
            )
            .optional()?)
    }
    fn group_results(&mut self, session: &str, group: &str, job: &str) -> Result<Vec<String>> {
        // A job stopped by a usage limit carries the limit's reset time (fridica#107).
        Ok(self.0.prepare("SELECT json_patch(json_object('id',j.id,'worker_id',j.worker_id,'machine',w.machine,'workspace',w.workspace,'role',w.role,
        'brief',j.brief,'job_status',j.status,'error',j.error,'result',json(j.result_json),'inbox_id',j.inbox_id,'reported',j.reported,'attempt',j.attempt),
        CASE WHEN j.retry_at IS NULL THEN '{}' ELSE json_object('rate_limit_resets_at',j.retry_at) END)
        FROM jobs j JOIN workers w ON w.id=j.worker_id WHERE j.session_id=? AND ((?!='' AND j.join_group=?) OR (?='' AND j.id=?)) ORDER BY j.queued_at,j.rowid")?
            .query_map([session,group,group,group,job], |r|r.get(0))?.collect::<rusqlite::Result<_>>()?)
    }
    fn inbox_entry(&mut self, inbox: i64, session: &str) -> Result<Option<InboxEntry>> {
        Ok(self
            .0
            .prepare_cached(
                "SELECT kind,ref,payload_json FROM thread_inbox WHERE id=? AND session_id=?",
            )?
            .query_row(params![inbox, session], |r| {
                Ok(InboxEntry {
                    kind: r.get(0)?,
                    reference: r.get(1)?,
                    payload: r.get(2)?,
                })
            })
            .optional()?)
    }
    fn message_origin(&mut self, event: &str) -> Result<Option<(String, bool)>> {
        Ok(self
            .0
            .query_row(
                "SELECT event_id,meta_json IS NOT NULL FROM messages WHERE event_id=?",
                [event],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }
    fn job_inbox(&mut self, job: &str, session: &str) -> Result<Option<i64>> {
        Ok(self
            .0
            .prepare_cached("SELECT inbox_id FROM jobs WHERE id=? AND session_id=?")?
            .query_row([job, session], |r| r.get(0))
            .optional()?
            .flatten())
    }
    fn deliverable_files(
        &mut self,
        session: &str,
        jobs: &[Option<String>],
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let mut statement = self.0.prepare_cached("SELECT a.path,a.blob FROM artifacts a JOIN jobs j ON j.id=a.job_id WHERE a.job_id=? AND a.session_id=? AND a.status='ready' AND a.blob IS NOT NULL AND j.deliverable IN ('markdown','figures_pdf') ORDER BY a.id")?;
        let mut files = Vec::new();
        for job in jobs {
            let rows =
                statement.query_map(params![job, session], |r| Ok((r.get(0)?, r.get(1)?)))?;
            for row in rows {
                files.push(row?);
            }
        }
        Ok(files)
    }
    fn link_answers(&mut self, post: i64, obligations: &[String]) -> Result<()> {
        let mut statement = self
            .0
            .prepare_cached("INSERT INTO obligation_posts VALUES(?,?)")?;
        for obligation in obligations {
            statement.execute(params![obligation, post])?;
        }
        Ok(())
    }
}

impl ReplyEvidence for Sqlite<'_> {
    fn last_reply(&mut self, session: &str) -> Result<Option<LastReply>> {
        Ok(self.0.query_row(
            "SELECT o.id,o.state,COALESCE(m.sender,'') FROM outbox o
         LEFT JOIN thread_inbox i ON i.session_id=o.session_id AND o.idem_key=CAST(i.id AS TEXT)||':reply' AND i.kind='message'
         LEFT JOIN messages m ON m.event_id=CASE WHEN o.trigger_event!='' THEN o.trigger_event ELSE i.ref END
         WHERE o.session_id=? AND o.kind='reply' ORDER BY o.id DESC LIMIT 1",
            [session],
            |r| Ok(LastReply { id: r.get(0)?, state: r.get(1)?, requester: r.get(2)? }),
        ).optional()?)
    }
    fn latest_messages(&mut self, session: &str) -> Result<Vec<(String, String)>> {
        Ok(self.0.prepare("SELECT sender,text FROM messages WHERE workspace||':'||channel||':'||root_ts=? ORDER BY CAST(ts AS REAL) DESC LIMIT 100")?
            .query_map([session],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?)
    }
    fn message_sender(&mut self, event: &str, session: &str) -> Result<Option<String>> {
        Ok(self.0.query_row("SELECT sender FROM messages WHERE event_id=? AND workspace||':'||channel||':'||root_ts=?",[event,session],|r|r.get(0)).optional()?)
    }
}

impl Debriefs for Sqlite<'_> {
    fn debrief_origin(&mut self, session: &str, after: &str) -> Result<Option<DebriefOrigin>> {
        Ok(self.0.query_row("SELECT t.version,t.turns,o.trigger_class FROM threads t JOIN outbox o ON o.session_id=t.id WHERE t.id=? AND t.status='complete' AND o.idem_key=?",params![session,after],|r|Ok(DebriefOrigin{version:r.get(0)?,turn:r.get(1)?,class:r.get(2)?})).optional()?)
    }
    fn queue_debrief(&mut self, session: &str, inbox: i64, payload: &str, now: f64) -> Result<()> {
        self.0.execute("INSERT INTO thread_inbox(session_id,kind,ref,payload_json,created) VALUES(?,'debrief',?,?,?)",params![session,inbox.to_string(),payload,now])?;
        Ok(())
    }
    fn debrief_due(
        &mut self,
        session: &str,
        version: Option<i64>,
        turn: i64,
        inbox: i64,
    ) -> Result<bool> {
        Ok(self.0.query_row("SELECT control='active' AND version=? AND debriefed_turn<? AND EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing') FROM threads WHERE id=?",params![version,turn,inbox,session],|r|r.get(0))?)
    }
    fn debrief_stale(&mut self, inbox: i64) -> Result<()> {
        self.0.execute(
            "UPDATE thread_inbox SET state='pending' WHERE id=? AND state='processing'",
            [inbox],
        )?;
        self.0.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [inbox],
        )?;
        Ok(())
    }
    fn debrief_posted(&mut self, p: &DebriefPost) -> Result<()> {
        self.0.execute(
            "UPDATE outbox SET trigger_class=? WHERE id=?",
            params![p.class, p.post],
        )?;
        self.0.execute(
            "UPDATE reply_reservations SET outbox_id=? WHERE inbox_id=? AND state='reserved'",
            params![p.post, p.inbox],
        )?;
        self.0.execute(
            "UPDATE threads SET debriefed_turn=?,updated=?,version=version+1 WHERE id=?",
            params![p.turn, p.now, p.session],
        )?;
        Ok(())
    }
    fn debrief_unavailable(&mut self, session: &str, inbox: i64, now: f64) -> Result<()> {
        self.0.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [inbox],
        )?;
        self.0.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','debrief.unavailable',?,?)",params![now,session,json!({"inbox_id":inbox}).to_string()])?;
        Ok(())
    }
    fn keep_debrief_turn(&mut self, t: &DebriefTurn) -> Result<()> {
        self.0.execute("INSERT INTO parent_turns(session_id,inbox_id,backend,call,action_json,response_json,context_json,created) VALUES(?,?,'adapter','debrief',?,?,?,?)",params![t.session,t.inbox,t.action,t.response,t.context,t.created])?;
        Ok(())
    }
}

impl ThreadTurns for Sqlite<'_> {
    fn ready_threads(&mut self, now: f64, limit: usize) -> Result<Vec<String>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(self
            .0
            .prepare(
                "SELECT session_id FROM thread_inbox WHERE state='pending' AND not_before<=?
                GROUP BY session_id ORDER BY MIN(id) LIMIT ?",
            )?
            .query_map(params![now, limit], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    fn inbox_done(&mut self, inbox: i64) -> Result<()> {
        self.0
            .execute("UPDATE thread_inbox SET state='done' WHERE id=?", [inbox])?;
        Ok(())
    }
    fn progress_state(&mut self, inbox: i64, session: &str) -> Result<ProgressState> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing'),control='active' FROM threads WHERE id=?",
            params![inbox, session], |r| Ok(ProgressState { processing: r.get(0)?, active: r.get(1)? }))?)
    }
    fn running_progress_note(
        &mut self,
        job: &str,
        attempt: i64,
        seq: i64,
        session: &str,
    ) -> Result<Option<ProgressNote>> {
        Ok(self.0.query_row(
            "SELECT p.text,j.worker_id FROM job_progress p JOIN jobs j ON j.id=p.job_id AND j.attempt=p.attempt
                 WHERE p.job_id=? AND p.attempt=? AND p.seq=? AND j.session_id=? AND j.status='running'",
            params![job, attempt, seq, session], |r| Ok(ProgressNote { text: r.get(0)?, worker: r.get(1)? })).optional()?)
    }
}
