//! The thread actor: loading, fencing, settling and committing turns.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{
    Arrival, Fence, NewAsk, ObligationChange, ParentTurn, QueuedHandoff, Sessions, Settlement,
    TriageSettlement, TurnClose, TurnFailure, TurnInput, TurnJobs, TurnObligations, TurnRetry,
    Turns,
};
use rusqlite::params;

const MESSAGE_SQL: &str = "SELECT json_object('event_id',event_id,'ts',ts,'sender',sender,'text',text,'files',json(files_json),'meta',json(meta_json),'attachments',json(attachments_json)) FROM messages";

impl Turns for Sqlite<'_> {
    fn turn_input(&mut self, session: &str, id: i64) -> Result<TurnInput> {
        let tx = self.0;
        let thread:String=tx.query_row("SELECT json_object('id',id,'workspace',workspace,'channel',channel,'root_ts',root_ts,'control',control,'status',status,'version',version,'turns',turns,'wait_streak',wait_streak,'no_progress',no_progress,'summary',summary,'last_reply_hash',last_reply_hash,'reset_at',reset_at,'context',json(context_json)) FROM threads WHERE id=?",[session],|r|r.get(0))?;
        let (kind, reference, payload): (String, String, String) = tx.query_row(
            "SELECT kind,ref,payload_json FROM thread_inbox WHERE id=? AND state='processing'",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let message = if kind == "message" {
            Some(tx.query_row(
                &format!("{MESSAGE_SQL} WHERE event_id=?"),
                [&reference],
                |r| r.get(0),
            )?)
        } else {
            None
        };
        let from_peer = if kind == "obligation_due" {
            Some(tx.query_row("SELECT EXISTS(SELECT 1 FROM obligations o JOIN messages m ON m.event_id=json_extract(o.source_json,'$.event_id') WHERE o.id=? AND m.meta_json IS NOT NULL)",[&reference],|r|r.get(0))?)
        } else {
            None
        };
        let history:Vec<String>=tx.prepare(&format!("{MESSAGE_SQL} WHERE workspace||':'||channel||':'||root_ts=? ORDER BY CAST(ts AS REAL) DESC LIMIT 60"))?
            .query_map([session],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        let obligations:Vec<String>=tx.prepare("SELECT json_object('id',id,'kind',kind,'summary',summary,'due',due,'state',state,'disposition',json(state_json),'source',json(source_json),'deliveries',json((SELECT COALESCE(json_group_array(json_object('id',p.outbox_id,'state',o.state,'error',o.error)), '[]') FROM obligation_posts p JOIN outbox o ON o.id=p.outbox_id WHERE p.obligation_id=obligations.id))) FROM obligations WHERE session_id=? AND state IN ('open','deferred','awaiting_delivery') ORDER BY created,id")?
            .query_map([session],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        let review_required:bool=tx.query_row("SELECT COALESCE((SELECT error IN ('parent_unavailable','parent_invalid_after_repair') FROM parent_turns WHERE session_id=? AND call IN ('decide','repair') AND error!='parent_rate_limited' ORDER BY id DESC LIMIT 1),0)",[session],|r|r.get(0))?;
        Ok(TurnInput {
            thread,
            kind,
            reference,
            payload,
            message,
            from_peer,
            history,
            obligations,
            review_required,
        })
    }
    fn earlier_roots(
        &mut self,
        workspace: Option<&str>,
        channel: Option<&str>,
        root_ts: Option<&str>,
    ) -> Result<Vec<String>> {
        Ok(self.0.prepare("SELECT json_object('event_id',event_id,'ts',ts,'sender',sender,'text',text,'files',json(files_json),'from_agent',json(meta_json)) FROM messages WHERE workspace=? AND channel=? AND ts=root_ts AND CAST(ts AS REAL)<CAST(? AS REAL) ORDER BY CAST(ts AS REAL) DESC,id DESC LIMIT 10")?
            .query_map(params![workspace,channel,root_ts],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?)
    }
    fn turn_decisions(&mut self, session: &str) -> Result<(String, i64)> {
        Ok(self.0.query_row(
            "SELECT decisions_json,debriefed_turn FROM threads WHERE id=?",
            [session],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?)
    }
    fn turn_live(&mut self, session: &str, version: Option<i64>, id: i64) -> Result<bool> {
        Ok(self.0.query_row("SELECT control='active' AND version=? AND EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing') FROM threads WHERE id=?",params![version,id,session],|r|r.get(0))?)
    }
    fn settle_turn(&mut self, s: &Settlement) -> Result<bool> {
        let tx = self.0;
        let current: bool = tx.query_row(
            "SELECT version=? AND EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing') FROM threads WHERE id=?",
            params![s.version,s.id,s.session], |r| r.get(0),
        )?;
        if !current {
            tx.execute(
                "UPDATE thread_inbox SET state='pending' WHERE id=? AND state='processing'",
                [s.id],
            )?;
        } else if let Some(until) = s.until {
            tx.execute(
                "UPDATE thread_inbox SET state='pending',not_before=? WHERE id=?",
                params![until, s.id],
            )?;
        } else {
            if let Some(event) = &s.event {
                tx.execute(
                    "UPDATE messages SET verdict=? WHERE event_id=?",
                    params![s.verdict, event],
                )?;
            }
            tx.execute("UPDATE thread_inbox SET state='done' WHERE id=?", [s.id])?;
        }
        tx.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [s.id],
        )?;
        Ok(current)
    }
    fn inbox_attempts(&mut self, id: i64) -> Result<(i64, String)> {
        Ok(self.0.query_row(
            "SELECT attempts,state FROM thread_inbox WHERE id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?)
    }
    fn fail_turn(&mut self, f: &TurnFailure) -> Result<()> {
        let tx = self.0;
        tx.execute("UPDATE thread_inbox SET attempts=attempts+1,state=?,not_before=? WHERE id=? AND state='processing'",params![f.state,f.not_before,f.id])?;
        tx.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [f.id],
        )?;
        tx.execute("INSERT OR IGNORE INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,updated)
                        SELECT ?,?,'signal',?,?,'An inbox turn failed; review required',?,?,?
                        WHERE NOT EXISTS(SELECT 1 FROM thread_inbox i JOIN obligations o ON i.ref=o.id WHERE i.id=? AND i.kind='obligation_due' AND o.kind='signal')",params![f.signal,f.session,f.signal,f.source,f.now,f.now,f.now,f.id])?;
        tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','inbox.failed',?,?)",params![f.now,f.session,f.details])?;
        Ok(())
    }
    fn retry_turn(&mut self, retry: &TurnRetry) -> Result<bool> {
        let tx = self.0;
        let current:bool=tx.query_row("SELECT version=? AND EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing') FROM threads WHERE id=?",params![retry.version,retry.id,retry.session],|r|r.get(0))?;
        let mut insert = tx.prepare_cached("INSERT INTO parent_turns(session_id,inbox_id,backend,call,action_json,response_json,context_json,error,created) VALUES(?,?,'adapter',?,'{}',?,?,?,?)")?;
        for call in &retry.calls {
            insert.execute(params![
                retry.session,
                retry.id,
                call.call,
                call.response,
                call.context,
                call.error,
                call.created
            ])?;
        }
        if current {
            tx.execute(
                "UPDATE thread_inbox SET state='pending',not_before=? WHERE id=?",
                params![retry.retry_at, retry.id],
            )?;
            tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','parent.rate_limited',?,?)",params![retry.now,retry.session,retry.details])?;
        } else {
            tx.execute(
                "UPDATE thread_inbox SET state='pending' WHERE id=? AND state='processing'",
                [retry.id],
            )?;
        }
        tx.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [retry.id],
        )?;
        Ok(current)
    }
    fn settle_triage(&mut self, t: &TriageSettlement) -> Result<bool> {
        let tx = self.0;
        let current:bool=tx.query_row("SELECT control='active' AND version=? AND EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing') FROM threads WHERE id=?",params![t.version,t.id,t.session],|r|r.get(0))?;
        if !current {
            self.return_turn(t.id)?;
            return Ok(false);
        }
        let mut insert = tx.prepare_cached("INSERT INTO parent_turns(session_id,inbox_id,backend,call,action_json,response_json,context_json,created) VALUES(?,?,'adapter','triage','{}',?,?,?)")?;
        for call in &t.calls {
            insert.execute(params![
                t.session,
                t.id,
                call.response,
                call.context,
                call.created
            ])?;
        }
        tx.execute(
            "UPDATE messages SET verdict=? WHERE event_id=?",
            params![t.verdict, t.event],
        )?;
        tx.execute("UPDATE thread_inbox SET state='done' WHERE id=?", [t.id])?;
        tx.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [t.id],
        )?;
        tx.execute(
            "UPDATE threads SET version=version+1,updated=? WHERE id=?",
            params![t.now, t.session],
        )?;
        Ok(true)
    }
    fn recover_turns(&mut self) -> Result<usize> {
        Ok(self.0.execute(
            "UPDATE thread_inbox SET state='pending' WHERE state='processing'",
            [],
        )?)
    }
    fn fence_turn(&mut self, session: &str, id: i64) -> Result<Fence> {
        let tx = self.0;
        let version: i64 =
            tx.query_row("SELECT version FROM threads WHERE id=?", [session], |r| {
                r.get(0)
            })?;
        let active:bool=tx.query_row("SELECT control='active' AND EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND state='processing') FROM threads WHERE id=?",params![id,session],|r|r.get(0))?;
        Ok(Fence { version, active })
    }
    fn arrival(
        &mut self,
        session: &str,
        id: i64,
        seen: Option<f64>,
        owner: &str,
    ) -> Result<Arrival> {
        let tx = self.0;
        let arrived = match seen {
            Some(seen) => tx.query_row("SELECT EXISTS(SELECT 1 FROM messages WHERE workspace||':'||channel||':'||root_ts=? AND CAST(ts AS REAL)>? AND NOT (sender=? AND meta_json IS NOT NULL))",params![session,seen,owner],|r|r.get(0))?,
            None => false,
        };
        let reread: bool = tx.query_row(
            "SELECT COALESCE(json_extract(payload_json,'$.reread'),0) FROM thread_inbox WHERE id=?",
            [id],
            |r| r.get(0),
        )?;
        if arrived && !reread {
            tx.execute("UPDATE thread_inbox SET payload_json=json_set(payload_json,'$.reread',1) WHERE id=?",[id])?;
        }
        Ok(Arrival { arrived, reread })
    }
    fn return_turn(&mut self, id: i64) -> Result<()> {
        self.0.execute(
            "UPDATE thread_inbox SET state='pending' WHERE id=? AND state='processing'",
            [id],
        )?;
        self.0.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [id],
        )?;
        Ok(())
    }
    fn defer_turn(&mut self, id: i64, until: f64) -> Result<()> {
        self.0.execute(
            "UPDATE thread_inbox SET state='pending',not_before=? WHERE id=?",
            params![until, id],
        )?;
        self.0.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [id],
        )?;
        Ok(())
    }
    fn record_parent_calls(
        &mut self,
        session: &str,
        id: i64,
        action: &str,
        calls: &[ParentTurn],
        now: f64,
    ) -> Result<()> {
        let tx = self.0;
        for call in calls {
            tx.prepare_cached("INSERT INTO parent_turns(session_id,inbox_id,backend,call,action_json,response_json,context_json,error,created) VALUES(?,?,'adapter',?,?,?,?,?,?)")?
                .execute(params![session,id,call.call,action,call.response,call.context,call.error,call.created])?;
            if let Some(details) = &call.blocked {
                tx.prepare_cached("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','parent.blocked',?,?)")?
                    .execute(params![now,session,details])?;
            }
        }
        Ok(())
    }
    fn close_turn(&mut self, t: &TurnClose) -> Result<()> {
        self.0.execute("UPDATE threads SET status=?,turns=CASE WHEN EXISTS(SELECT 1 FROM outbox WHERE idem_key=?) THEN MAX(turns,?) ELSE turns END,
            wait_streak=?,no_progress=?,last_reply_hash=?,summary=CASE WHEN ?='' THEN summary ELSE ? END,updated=?,version=version+1 WHERE id=?",
            params![t.status,t.reply_key,t.turn,t.waiting,t.quiet,t.hash,t.summary,t.summary,t.now,t.session])?;
        Ok(())
    }
    fn finish_turn(&mut self, id: i64, event: Option<&str>, verdict: &str) -> Result<()> {
        if let Some(event) = event {
            self.0.execute(
                "UPDATE messages SET verdict=? WHERE event_id=?",
                params![verdict, event],
            )?;
        }
        self.0
            .execute("UPDATE thread_inbox SET state='done' WHERE id=?", [id])?;
        self.0.execute(
            "UPDATE reply_reservations SET state='released' WHERE inbox_id=? AND outbox_id IS NULL",
            [id],
        )?;
        Ok(())
    }
}

impl Sessions for Sqlite<'_> {
    fn last_unsolicited(
        &mut self,
        workspace: Option<&str>,
        channel: Option<&str>,
    ) -> Result<Option<f64>> {
        Ok(self.0.query_row(
            "SELECT (SELECT last_unsolicited FROM cooldowns WHERE workspace=? AND channel=?)",
            params![workspace, channel],
            |r| r.get(0),
        )?)
    }
    fn mark_unsolicited(
        &mut self,
        workspace: Option<&str>,
        channel: Option<&str>,
        now: f64,
    ) -> Result<()> {
        self.0.execute(
            "INSERT OR REPLACE INTO cooldowns(workspace,channel,last_unsolicited) VALUES(?,?,?)",
            params![workspace, channel, now],
        )?;
        Ok(())
    }
    fn patch_context(&mut self, session: &str, patch: &str) -> Result<()> {
        self.0.execute(
            "UPDATE threads SET context_json=json_patch(context_json,?) WHERE id=?",
            params![patch, session],
        )?;
        Ok(())
    }
    fn label_post(&mut self, post: i64, meta: &str, trigger: &str, report: bool) -> Result<()> {
        self.0.execute(
            "UPDATE outbox SET meta_json=?,trigger_event=? WHERE id=?",
            params![meta, trigger, post],
        )?;
        if report {
            self.0
                .execute("UPDATE outbox SET kind='report' WHERE id=?", [post])?;
        }
        Ok(())
    }
    fn handoff_class(&mut self, id: i64) -> Result<String> {
        Ok(self.0.query_row(
            "SELECT COALESCE((SELECT trigger_class FROM reply_reservations WHERE inbox_id=?),'peer')",
            [id],
            |r| r.get(0),
        )?)
    }
    fn queue_handoffs(&mut self, handoffs: &[QueuedHandoff], now: f64) -> Result<()> {
        let tx = self.0;
        for h in handoffs {
            let queued=tx.prepare_cached("INSERT OR IGNORE INTO thread_inbox(session_id,kind,ref,payload_json,created,dedup_key)
                    SELECT ?,'handoff',?,?,?,? WHERE EXISTS(SELECT 1 FROM threads WHERE id=? AND control='active')")?
                .execute(params![h.target,h.from,h.payload,now,h.dedup_key,h.target])?;
            tx.prepare_cached("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'parent','thread.handoff',?,?)")?
                .execute(params![now,h.target,if queued == 1 { &h.queued } else { &h.skipped }])?;
        }
        Ok(())
    }
}

impl TurnJobs for Sqlite<'_> {
    fn mark_reported(&mut self, session: &str, jobs: &[Option<String>]) -> Result<()> {
        let mut update = self
            .0
            .prepare_cached("UPDATE jobs SET reported=1 WHERE id=? AND session_id=?")?;
        for job in jobs {
            update.execute(params![job, session])?;
        }
        Ok(())
    }
    fn jobs_running(&mut self, session: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE session_id=? AND status IN ('queued','running'))",
            [session],
            |r| r.get(0),
        )?)
    }
}

impl TurnObligations for Sqlite<'_> {
    fn answer_for_handoff(
        &mut self,
        from: &str,
        obligations: &[String],
        post: i64,
        now: f64,
    ) -> Result<()> {
        let tx = self.0;
        for obligation in obligations {
            if tx.prepare_cached("UPDATE obligations SET state='awaiting_delivery',updated=? WHERE id=? AND session_id=? AND state IN ('open','deferred')")?
                .execute(params![now,obligation,from])?==1 {
                tx.prepare_cached("INSERT OR IGNORE INTO obligation_posts VALUES(?,?)")?
                    .execute(params![obligation, post])?;
            }
        }
        Ok(())
    }
    fn change_obligations(
        &mut self,
        session: &str,
        changes: &[ObligationChange],
        now: f64,
    ) -> Result<bool> {
        let mut update = self.0.prepare_cached("UPDATE obligations SET state=?,state_json=?,due=COALESCE(?,due),updated=? WHERE id=? AND session_id=? AND state IN ('open','deferred')")?;
        for c in changes {
            if update.execute(params![c.state, c.details, c.due, now, c.id, session])? != 1 {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn open_asks(&mut self, session: &str, asks: &[NewAsk], now: f64) -> Result<()> {
        let mut insert = self.0.prepare_cached("INSERT INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,updated) VALUES(?,?,'ask',?,?,?,?,?,?)")?;
        for ask in asks {
            insert.execute(params![
                ask.id,
                session,
                ask.id,
                ask.source,
                ask.summary,
                now,
                ask.due,
                now
            ])?;
        }
        Ok(())
    }
    fn open_streak_signal(
        &mut self,
        id: &str,
        session: &str,
        source: &str,
        now: f64,
    ) -> Result<()> {
        self.0.execute("INSERT OR IGNORE INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,updated) VALUES(?,?,'signal',?,?,'Conversation needs attention',?,?,?)",
            params![id,session,id,source,now,now,now])?;
        Ok(())
    }
}
