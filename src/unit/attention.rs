//! Messages arriving in threads, reply capacity and obligations.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{
    ArrivedMessage, Backfill, Disposal, HistoricalMention, Inbox, InboxItem, Mention, MentionQuery,
    Obligations, QueuedAnswer, RecentReply, Replies, ReservedReply, Route,
};
use rusqlite::{params, OptionalExtension};

impl Inbox for Sqlite<'_> {
    fn thread_waiting(&mut self, session: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM threads WHERE id=? AND status='waiting')",
            [session],
            |r| r.get(0),
        )?)
    }
    fn keep_message(&mut self, m: &ArrivedMessage) -> Result<bool> {
        let inserted=self.0.execute("INSERT OR IGNORE INTO messages(event_id,workspace,channel,ts,root_ts,thread_ts,sender,text,files_json,source,meta_json,received_at,attachments_json,mentions_owner) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![m.event_id,m.workspace,m.channel,m.ts,m.root_ts,m.thread_ts,m.sender,m.text,m.files,m.source,m.meta,m.received_at,m.attachments,m.mentions_owner])?;
        Ok(inserted != 0)
    }
    fn open_thread(
        &mut self,
        session: &str,
        workspace: &str,
        channel: &str,
        root_ts: &str,
        created: f64,
    ) -> Result<()> {
        self.0.execute("INSERT OR IGNORE INTO threads(id,workspace,channel,root_ts,created,updated,control_json) VALUES(?,?,?,?,?,?,'{\"kind\":\"active\"}')",
            params![session,workspace,channel,root_ts,created,created])?;
        Ok(())
    }
    fn queue_message(&mut self, session: &str, event_id: &str, created: f64) -> Result<i64> {
        self.0.execute(
            "INSERT INTO thread_inbox(session_id,kind,ref,created) VALUES(?,'message',?,?)",
            params![session, event_id, created],
        )?;
        Ok(self.0.last_insert_rowid())
    }
    fn claim_next(&mut self, session: &str, now: f64) -> Result<Option<InboxItem>> {
        let item:Option<InboxItem>=self.0.query_row("SELECT id,kind FROM thread_inbox WHERE session_id=? AND state='pending' AND not_before<=?
            AND NOT EXISTS(SELECT 1 FROM thread_inbox busy WHERE busy.session_id=thread_inbox.session_id AND busy.state='processing') ORDER BY id LIMIT 1",
            params![session,now],|r|Ok(InboxItem{id:r.get(0)?,kind:r.get(1)?})).optional()?;
        if let Some(item) = &item {
            self.0.execute(
                "UPDATE thread_inbox SET state='processing' WHERE id=?",
                [item.id],
            )?;
        }
        Ok(item)
    }
}

impl Replies for Sqlite<'_> {
    fn thread_active(&mut self, session: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT control='active' FROM threads WHERE id=?",
            [session],
            |r| r.get(0),
        )?)
    }
    fn inbox_open(&mut self, inbox: i64, session: &str) -> Result<bool> {
        Ok(self.0.query_row("SELECT EXISTS(SELECT 1 FROM thread_inbox WHERE id=? AND session_id=? AND state IN ('pending','processing'))",params![inbox,session],|r|r.get(0))?)
    }
    fn reservation_state(&mut self, inbox: i64) -> Result<Option<String>> {
        Ok(self
            .0
            .query_row(
                "SELECT state FROM reply_reservations WHERE inbox_id=?",
                [inbox],
                |r| r.get(0),
            )
            .optional()?)
    }
    fn recent_replies(&mut self, session: &str, now: f64) -> Result<Vec<RecentReply>> {
        Ok(self.0.prepare("SELECT r.trigger_class,CASE WHEN r.state='sent' THEN o.delivered_at ELSE MAX(r.reserved_at,?) END AS at
             FROM reply_reservations r LEFT JOIN outbox o ON o.id=r.outbox_id
             WHERE r.session_id=? AND r.state!='released' AND r.trigger_class!='owner'
             AND (r.state='reserved' OR o.delivered_at>?) ORDER BY at")?
            .query_map(params![now,session,now-3600.],|r|Ok(RecentReply{trigger:r.get(0)?,at:r.get(1)?}))?.collect::<rusqlite::Result<_>>()?)
    }
    fn defer_reply(&mut self, session: &str, inbox: i64, until: f64) -> Result<()> {
        self.0.execute(
            "UPDATE thread_inbox SET state='pending',not_before=? WHERE id=?",
            params![until, inbox],
        )?;
        self.0.execute(
            "UPDATE threads SET throttled_until=? WHERE id=?",
            params![until, session],
        )?;
        Ok(())
    }
    fn reserve_reply(
        &mut self,
        id: &str,
        session: &str,
        inbox: i64,
        trigger: &str,
        now: f64,
    ) -> Result<()> {
        self.0.execute("INSERT INTO reply_reservations(id,session_id,inbox_id,trigger_class,reserved_at) VALUES(?,?,?,?,?)
            ON CONFLICT(inbox_id) DO UPDATE SET state='reserved',reserved_at=excluded.reserved_at,trigger_class=excluded.trigger_class",
            params![id,session,inbox,trigger,now])?;
        Ok(())
    }
    fn reserved_reply(&mut self, inbox: i64, session: &str) -> Result<ReservedReply> {
        Ok(self.0.query_row("SELECT trigger_class,outbox_id FROM reply_reservations WHERE inbox_id=? AND session_id=? AND state='reserved'",
            params![inbox,session],|r|Ok(ReservedReply{trigger:r.get(0)?,post:r.get(1)?}))?)
    }
    fn thread_route(&mut self, session: &str) -> Result<Route> {
        Ok(self.0.query_row(
            "SELECT channel,root_ts FROM threads WHERE id=?",
            [session],
            |r| {
                Ok(Route {
                    channel: r.get(0)?,
                    root_ts: r.get(1)?,
                })
            },
        )?)
    }
    fn answer_queued(&mut self, answer: &QueuedAnswer) -> Result<Option<String>> {
        self.0.execute(
            "UPDATE outbox SET answers_json=?,trigger_class=? WHERE id=?",
            params![answer.answers, answer.trigger, answer.post],
        )?;
        for obligation in &answer.obligations {
            let changed=self.0.prepare_cached("UPDATE obligations SET state='awaiting_delivery',updated=? WHERE id=? AND session_id=? AND state IN ('open','deferred')")?
                .execute(params![answer.time,obligation,answer.session])?;
            if changed != 1 {
                return Ok(Some(obligation.clone()));
            }
            self.0
                .prepare_cached("INSERT OR IGNORE INTO obligation_posts VALUES(?,?)")?
                .execute(params![obligation, answer.post])?;
        }
        self.0.execute(
            "UPDATE reply_reservations SET outbox_id=? WHERE inbox_id=?",
            params![answer.post, answer.inbox],
        )?;
        self.0.execute(
            "UPDATE thread_inbox SET state='done' WHERE id=?",
            [answer.inbox],
        )?;
        Ok(None)
    }
}

impl Obligations for Sqlite<'_> {
    fn open_mention(&mut self, m: &Mention) -> Result<()> {
        self.0.execute("INSERT OR IGNORE INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,updated) VALUES(?,?,'mention',?,?,'Owner mentioned',?,?,?)",
            params![m.id,m.session,m.dedup_key,m.source,m.created,m.due,m.created])?;
        Ok(())
    }
    fn open_signal(&mut self, id: &str, session: &str, now: f64) -> Result<bool> {
        let changed=self.0.execute("INSERT OR IGNORE INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,updated)
            VALUES(?,?,'signal',?,'{}','Conversation needs attention',?,?,?)",
            params![id,session,id,now,now,now])?;
        Ok(changed == 1)
    }
    fn obligation_state(&mut self, id: &str) -> Result<String> {
        Ok(self
            .0
            .query_row("SELECT state FROM obligations WHERE id=?", [id], |r| {
                r.get(0)
            })?)
    }
    fn dispose(&mut self, d: &Disposal) -> Result<bool> {
        if self.0.execute("UPDATE obligations SET state=?,state_json=?,due=COALESCE(?,due),updated=? WHERE id=? AND state IN ('open','deferred','awaiting_delivery')",
            params![d.state,d.details,d.due,d.time,d.id])?!=1 {return Ok(false);}
        self.0.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,?,'obligation.disposition',?,?)", params![d.time,d.actor,d.id,d.details])?;
        Ok(true)
    }
    fn queue_due(&mut self, now: f64) -> Result<usize> {
        Ok(self.0.execute(
            "INSERT OR IGNORE INTO thread_inbox(session_id,kind,ref,payload_json,created,dedup_key)
            SELECT session_id,'obligation_due',id,'{}',?, 'due:'||id||':'||due FROM obligations
            WHERE state IN ('open','deferred') AND due<=?",
            params![now, now],
        )?)
    }
    fn historical_mentions(&mut self, q: &MentionQuery) -> Result<Vec<HistoricalMention>> {
        let mut query=self.0.prepare("SELECT m.event_id,t.id,m.workspace,m.channel,m.ts,m.received_at FROM messages m JOIN threads t ON t.workspace=m.workspace AND t.channel=m.channel AND t.root_ts=m.root_ts
            WHERE m.workspace=? AND m.channel IN (SELECT value FROM json_each(?))
            AND CAST(m.ts AS REAL)>=? AND CAST(m.ts AS REAL)<? AND m.sender!=? AND m.source!='self'
            AND instr(m.text,?)>0 AND CAST(m.ts AS REAL)>t.reset_at AND t.control NOT IN ('closed','archived','cleaned')
            AND NOT EXISTS(SELECT 1 FROM obligations o WHERE o.dedup_key='mention:'||m.workspace||':'||m.channel||':'||m.ts)
            ORDER BY CAST(m.ts AS REAL),m.event_id LIMIT 1001")?;
        let selected = query.query_map(
            params![
                q.workspace,
                q.channels,
                q.since,
                q.until,
                q.owner,
                q.mention
            ],
            |r| {
                Ok(HistoricalMention {
                    event_id: r.get(0)?,
                    session: r.get(1)?,
                    workspace: r.get(2)?,
                    channel: r.get(3)?,
                    ts: r.get(4)?,
                    received_at: r.get(5)?,
                })
            },
        )?;
        Ok(selected.collect::<rusqlite::Result<_>>()?)
    }
    fn apply_backfill(&mut self, b: &Backfill) -> Result<()> {
        for o in &b.obligations {
            self.0.prepare_cached("INSERT INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,state,state_json,updated) VALUES(?,?,'mention',?,?,'Historical mention; answer status requires owner review',?,?,'deferred',?,?)")?
                .execute(params![o.id,o.session,o.dedup_key,o.source,o.created,o.due,o.state,o.updated])?;
            self.0
                .prepare_cached("UPDATE messages SET mentions_owner=1 WHERE event_id=?")?
                .execute([&o.event_id])?;
        }
        self.0.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,?,'obligations.backfill',?,?)",params![b.time,b.actor,b.client_id,b.result])?;
        Ok(())
    }
}
