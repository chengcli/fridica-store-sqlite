//! Thread controls, the worker stops they queue, and linked threads.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{
    ControlState, LinkedJob, LinkedMessage, LinkedThread, LinkedThreads, OpenAsk, ResumePoint,
    ThreadControls, WorkerStop, WorkerStops,
};
use rusqlite::{params, OptionalExtension};

impl ThreadControls for Sqlite<'_> {
    fn control_state(&mut self, session: &str) -> Result<ControlState> {
        Ok(self.0.query_row(
            "SELECT control,control_json FROM threads WHERE id=?",
            [session],
            |r| {
                Ok(ControlState {
                    control: r.get(0)?,
                    details: r.get(1)?,
                })
            },
        )?)
    }
    fn thread_channel(&mut self, session: &str) -> Result<(String, String)> {
        Ok(self.0.query_row(
            "SELECT workspace,channel FROM threads WHERE id=?",
            [session],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?)
    }
    fn has_live_workers(&mut self, session: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM workers WHERE session_id=? AND status!='stopped')",
            [session],
            |r| r.get(0),
        )?)
    }
    fn set_control(
        &mut self,
        session: &str,
        control: &str,
        details: &str,
        reason: &str,
        now: f64,
        reset_streaks: bool,
    ) -> Result<()> {
        self.0.execute(
            "UPDATE threads SET control=?,control_json=?,pause_reason=?,version=version+1,updated=?,
            wait_streak=CASE WHEN ? THEN 0 ELSE wait_streak END,
            no_progress=CASE WHEN ? THEN 0 ELSE no_progress END WHERE id=?",
            params![
                control,
                details,
                reason,
                now,
                reset_streaks,
                reset_streaks,
                session
            ],
        )?;
        Ok(())
    }
    fn restart_turns(&mut self, session: &str) -> Result<()> {
        self.0.execute("UPDATE threads SET status=CASE WHEN status IN ('blocked','working') THEN 'complete' ELSE status END,turns=0,debriefed_turn=0 WHERE id=?",[session])?;
        Ok(())
    }
    fn unblock(&mut self, session: &str) -> Result<()> {
        self.0.execute(
            "UPDATE threads SET status='complete' WHERE id=? AND status='blocked'",
            [session],
        )?;
        Ok(())
    }
    fn retry_failed_turn(&mut self, session: &str) -> Result<Option<i64>> {
        let last: Option<(i64, String)> = self
            .0
            .query_row(
                "SELECT inbox_id,error FROM parent_turns WHERE session_id=? AND call IN ('decide','repair') AND error!='parent_rate_limited' ORDER BY id DESC LIMIT 1",
                [session],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((inbox, error)) = last else {
            return Ok(None);
        };
        if !matches!(
            error.as_str(),
            "parent_unavailable" | "parent_invalid_after_repair"
        ) {
            return Ok(None);
        }
        let requeued = self.0.execute(
            "UPDATE thread_inbox SET state='pending',attempts=0,not_before=0 WHERE id=? AND session_id=? AND state='done'",
            params![inbox, session],
        )?;
        if requeued == 0 {
            return Ok(None);
        }
        self.unblock(session)?;
        Ok(Some(inbox))
    }
    fn release_worker_results(&mut self, session: &str) -> Result<()> {
        self.0.execute("UPDATE thread_inbox SET not_before=0 WHERE session_id=? AND kind IN ('worker_result','worker_interrupted') AND state='pending'",[session])?;
        Ok(())
    }
    fn wipe(&mut self, session: &str, now: f64) -> Result<()> {
        let c = self.0;
        c.execute("UPDATE messages SET text='',files_json='[]',attachments_json='[]' WHERE workspace||':'||channel||':'||root_ts=?", [session])?;
        c.execute(
            "UPDATE threads SET summary='',decisions_json='[]' WHERE id=?",
            [session],
        )?;
        c.execute("UPDATE thread_inbox SET payload_json='{\"text\":\"\"}' WHERE session_id=? AND kind='owner_instruction'", [session])?;
        // Retain IDs and history, but never execute work using wiped input or a
        // context snapshot captured before cleaning. New intake remains visible.
        c.execute("UPDATE thread_inbox SET state='dropped' WHERE session_id=? AND state IN ('pending','processing')", [session])?;
        c.execute("UPDATE reply_reservations SET state='released' WHERE session_id=? AND state='reserved' AND outbox_id IS NULL", [session])?;
        c.execute("UPDATE obligations SET state='owner_closed',state_json='{\"kind\":\"owner_closed\",\"reason\":\"Thread cleaned by owner\"}',updated=? WHERE session_id=? AND state IN ('open','deferred','awaiting_delivery')", params![now,session])?;
        Ok(())
    }
    fn owner_instruction(
        &mut self,
        session: &str,
        client_id: &str,
    ) -> Result<Option<(i64, String)>> {
        Ok(self
            .0
            .query_row(
                "SELECT id,json_extract(payload_json,'$.text') FROM thread_inbox \
             WHERE session_id=? AND kind='owner_instruction' AND ref=? ORDER BY id LIMIT 1",
                params![session, client_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }
    fn queue_owner_instruction(
        &mut self,
        session: &str,
        client_id: &str,
        payload: &str,
        now: f64,
    ) -> Result<i64> {
        self.0.execute("INSERT INTO thread_inbox(session_id,kind,ref,payload_json,created) VALUES(?,'owner_instruction',?,?,?)",params![session,client_id,payload,now])?;
        Ok(self.0.last_insert_rowid())
    }
    fn resume_point(&mut self, session: &str) -> Result<ResumePoint> {
        let latest:Option<(String,f64)>=self.0.query_row("SELECT event_id,CAST(ts AS REAL) FROM messages WHERE workspace||':'||channel||':'||root_ts=?
                    AND source!='self' AND meta_json IS NULL AND CAST(ts AS REAL)>COALESCE((SELECT MAX(CAST(ts AS REAL)) FROM messages WHERE workspace||':'||channel||':'||root_ts=? AND source='self'),0)
                    ORDER BY CAST(ts AS REAL) DESC LIMIT 1",params![session,session],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let newest:f64=self.0.query_row("SELECT COALESCE(MAX(CAST(ts AS REAL)),0) FROM messages WHERE workspace||':'||channel||':'||root_ts=?",[session],|r|r.get(0))?;
        Ok(ResumePoint { latest, newest })
    }
    fn reset_thread_at(&mut self, session: &str, boundary: f64) -> Result<()> {
        self.0.execute(
            "UPDATE threads SET reset_at=? WHERE id=?",
            params![boundary, session],
        )?;
        Ok(())
    }
    fn resume_message(
        &mut self,
        session: &str,
        event: &str,
        payload: &str,
        now: f64,
    ) -> Result<()> {
        let c = self.0;
        // A delegated request already has durable work or a result to
        // consume. Resume it without delegating the same ask again.
        let delegated:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN thread_inbox i ON i.id=j.inbox_id WHERE j.session_id=? AND i.kind='message' AND i.ref=? AND (j.reported=0 OR j.status IN ('queued','running')))",params![session,event],|r|r.get(0))?;
        if delegated {
            return Ok(());
        }
        let existing:Option<i64>=c.query_row("SELECT id FROM thread_inbox WHERE session_id=? AND kind='message' AND ref=? AND state IN ('pending','processing') ORDER BY id LIMIT 1",params![session,event],|r|r.get(0)).optional()?;
        if let Some(id) = existing {
            c.execute(
                "UPDATE thread_inbox SET payload_json=?,not_before=0 WHERE id=?",
                params![payload, id],
            )?;
            c.execute("UPDATE thread_inbox SET state='done' WHERE session_id=? AND kind='message' AND ref=? AND state='pending' AND id!=?",params![session,event,id])?;
        } else {
            c.execute("INSERT INTO thread_inbox(session_id,kind,ref,payload_json,created) VALUES(?,'message',?,?,?)",params![session,event,payload,now])?;
        }
        Ok(())
    }
    fn thread_driver(&mut self, session: &str) -> Result<String> {
        Ok(self
            .0
            .query_row("SELECT driver FROM threads WHERE id=?", [session], |r| {
                r.get(0)
            })?)
    }
    fn set_thread_driver(
        &mut self,
        session: &str,
        driver: &str,
        actor: &str,
        now: f64,
    ) -> Result<bool> {
        if !matches!(driver, "parent" | "external") {
            anyhow::bail!("unknown thread driver");
        }
        let current = self.thread_driver(session)?;
        if current == driver {
            return Ok(false);
        }
        self.0.execute(
            "UPDATE threads SET driver=?,version=version+1,updated=? WHERE id=?",
            params![driver, now, session],
        )?;
        self.audit_control(
            now,
            actor,
            "driver",
            session,
            &serde_json::json!({"from":current,"to":driver}).to_string(),
        )?;
        Ok(true)
    }
    fn audit_control(
        &mut self,
        now: f64,
        actor: &str,
        action: &str,
        session: &str,
        details: &str,
    ) -> Result<()> {
        self.0.execute(
            "INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,?,?,?,?)",
            params![now, actor, action, session, details],
        )?;
        Ok(())
    }
}

impl WorkerStops for Sqlite<'_> {
    fn closed_threads_with_live_workers(&mut self) -> Result<Vec<String>> {
        let sessions = self.0.prepare("SELECT DISTINCT t.id FROM threads t JOIN workers w ON w.session_id=t.id WHERE t.control IN ('closed','archived','cleaned') AND w.status!='stopped'")?
            .query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        Ok(sessions)
    }
    fn queue_worker_stops(
        &mut self,
        sessions: &[String],
        now: f64,
        include_stopped: bool,
    ) -> Result<()> {
        // Persist termination intent with the control effect, before
        // acknowledgement. Replay storage keeps the intent recoverable even if
        // worker status already became `stopped`.
        let mut statement = self.0.prepare_cached(
            "INSERT INTO replay_events(kind,time,payload_json,complete)
         SELECT 'thread_worker_stop',?,json_object('session',session_id,'worker',id),0 FROM workers w
         WHERE session_id=? AND (status!='stopped' OR ?) AND NOT EXISTS(
            SELECT 1 FROM replay_events e WHERE e.kind='thread_worker_stop' AND e.complete=0
            AND json_extract(e.payload_json,'$.worker')=w.id)",
        )?;
        for session in sessions {
            statement.execute(params![now, session, include_stopped])?;
        }
        Ok(())
    }
    fn worker_stop_pending(&mut self, session: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM replay_events WHERE kind='thread_worker_stop' AND complete=0 AND json_extract(payload_json,'$.session')=?)",
            [session], |r| r.get(0),
        )?)
    }
    fn pending_worker_stops(&mut self) -> Result<Vec<WorkerStop>> {
        let pending = self.0.prepare("SELECT seq,json_extract(payload_json,'$.worker') FROM replay_events WHERE kind='thread_worker_stop' AND complete=0 ORDER BY seq")?
            .query_map([], |r| Ok(WorkerStop { seq: r.get(0)?, worker: r.get(1)? }))?.collect::<rusqlite::Result<_>>()?;
        Ok(pending)
    }
}

impl LinkedThreads for Sqlite<'_> {
    fn linked_threads(
        &mut self,
        session: &str,
        limit: usize,
        window: f64,
    ) -> Result<Vec<LinkedThread>> {
        let c = self.0;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        // Why each thread is linked: shared items, and references either way.
        let rows: Vec<(String, String, bool, bool)> = c
            .prepare(
                "WITH mine AS (SELECT workspace,channel,item,repo,last_seen FROM item_links WHERE session_id=?1),
             shared AS (SELECT l.session_id AS id,group_concat(DISTINCT CASE WHEN l.repo='' THEN l.item ELSE l.repo||l.item END) AS items
                FROM item_links l JOIN mine m ON m.workspace=l.workspace AND m.channel=l.channel AND m.item=l.item
                WHERE l.session_id!=?1 AND (l.repo='' OR m.repo='' OR l.repo=m.repo) AND ABS(l.last_seen-m.last_seen)<=?3
                GROUP BY l.session_id),
             out AS (SELECT target AS id FROM thread_links WHERE session_id=?1),
             inc AS (SELECT session_id AS id FROM thread_links WHERE target=?1),
             ids AS (SELECT id FROM shared UNION SELECT id FROM out UNION SELECT id FROM inc)
             SELECT t.id,COALESCE((SELECT items FROM shared WHERE shared.id=t.id),''),
                EXISTS(SELECT 1 FROM out WHERE out.id=t.id),EXISTS(SELECT 1 FROM inc WHERE inc.id=t.id)
             FROM threads t JOIN ids ON ids.id=t.id
             WHERE t.control NOT IN ('archived','cleaned','closed') ORDER BY t.updated DESC,t.id LIMIT ?2",
            )?
            .query_map(params![session, limit, window], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        let mut linked = vec![];
        for (id, shared_items, referenced, references_this) in rows {
            let (root_ts, status, summary, decisions, control): (
                String,
                String,
                String,
                String,
                String,
            ) = c
                .prepare_cached(
                    "SELECT root_ts,status,summary,decisions_json,control FROM threads WHERE id=?",
                )?
                .query_row([&id], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                })?;
            let asks = c
                .prepare_cached(
                    "SELECT kind,summary,due FROM obligations WHERE session_id=? AND state IN ('open','deferred')
                 ORDER BY created,id LIMIT 3",
                )?
                .query_map([&id], |r| {
                    Ok(OpenAsk {
                        kind: r.get(0)?,
                        summary: r.get(1)?,
                        due: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?;
            let jobs = c
                .prepare_cached(
                    "SELECT j.id,j.status,w.role,w.machine,j.brief,COALESCE(json_extract(j.result_json,'$.summary'),''),j.error,
                    COALESCE((SELECT text FROM job_progress p WHERE p.job_id=j.id ORDER BY p.attempt DESC,p.seq DESC LIMIT 1),'')
                 FROM jobs j JOIN workers w ON w.id=j.worker_id WHERE j.session_id=? ORDER BY j.queued_at DESC,j.rowid DESC LIMIT 3",
                )?
                .query_map([&id], |r| {
                    Ok(LinkedJob {
                        id: r.get(0)?,
                        status: r.get(1)?,
                        role: r.get(2)?,
                        machine: r.get(3)?,
                        brief: r.get(4)?,
                        summary: r.get(5)?,
                        error: r.get(6)?,
                        progress: r.get(7)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?;
            let mut messages: Vec<LinkedMessage> = c
                .prepare_cached(
                    "SELECT ts,sender,text,meta_json IS NOT NULL FROM messages WHERE workspace||':'||channel||':'||root_ts=?
                 ORDER BY CAST(ts AS REAL) DESC,id DESC LIMIT 2",
                )?
                .query_map([&id], |r| {
                    Ok(LinkedMessage {
                        ts: r.get(0)?,
                        sender: r.get(1)?,
                        text: r.get(2)?,
                        from_agent: r.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?;
            messages.reverse();
            let root: String = c
                .query_row(
                    "SELECT text FROM messages WHERE workspace||':'||channel||':'||root_ts=? AND ts=root_ts LIMIT 1",
                    [&id],
                    |r| r.get(0),
                )
                .unwrap_or_default();
            linked.push(LinkedThread {
                id,
                shared_items,
                referenced,
                references_this,
                root_ts,
                status,
                control,
                summary,
                decisions,
                root,
                asks,
                jobs,
                messages,
            });
        }
        Ok(linked)
    }
}
