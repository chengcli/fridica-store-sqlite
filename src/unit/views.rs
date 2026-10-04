//! The read models behind the control API and the dashboard, and the owner's
//! notes edits.
use super::Sqlite;
use anyhow::{bail, Result};
use fridica_core::store::{Cell, MessageFiles, OwnerNotes, Row, Status, Views};
use rusqlite::{params, types::ValueRef, Connection, OptionalExtension, Params};
use serde_json::json;

const THREAD:&str="SELECT id,workspace,channel,root_ts,status,control,pause_reason,turns,wait_streak,no_progress,last_reply_hash,reset_at,summary,decisions_json,context_json,debriefed_turn,last_unsolicited,created,updated,version,control_json AS control_detail_json,throttled_until FROM threads";
const WORKER:&str="SELECT id,session_id,machine,workspace,backend,role,ephemeral,backend_session_id,status,summary,last_result_json,slot,created,updated FROM workers";
const JOB:&str="SELECT id,worker_id,session_id,brief,join_group,inbox_id,deliverable,fetch_repo,fetch_ref,status,attempt,reported,result_json,error,queued_at,started_at,finished_at,work_item_id,target_sha,target_tree FROM jobs";
const OUTBOX:&str="SELECT id,idem_key,session_id,kind,channel,thread_ts,text,meta_json,filename,\"after\",state,attempts,retry_at,sent_ts,error,created,blob IS NOT NULL AS has_file,delivered_at,trigger_event,trigger_class,answers_json FROM outbox";

fn rows(c: &Connection, sql: &str, params: impl Params) -> Result<Vec<Row>> {
    let mut statement = c.prepare(sql)?;
    let columns = statement
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut rows = statement.query(params)?;
    let mut result = vec![];
    while let Some(row) = rows.next()? {
        let mut cells = Vec::with_capacity(columns.len());
        for (i, name) in columns.iter().enumerate() {
            let cell = match row.get_ref(i)? {
                ValueRef::Null => Cell::Null,
                ValueRef::Integer(n) => Cell::Integer(n),
                ValueRef::Real(n) => Cell::Real(n),
                ValueRef::Text(t) => Cell::Text(std::str::from_utf8(t)?.to_owned()),
                ValueRef::Blob(_) => bail!("binary fields are not control views"),
            };
            cells.push((name.clone(), cell));
        }
        result.push(Row(cells));
    }
    Ok(result)
}

/// ` WHERE column IN (?,…)` for a non-empty filter.
fn filter(column: &str, values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }
    format!(" WHERE {column} IN ({})", vec!["?"; values.len()].join(","))
}

impl Views for Sqlite<'_> {
    fn status(&mut self) -> Result<Status> {
        let c = self.0;
        let runtime = rows(
            c,
            "SELECT started_at,slack_status FROM runtime WHERE id=1",
            [],
        )?
        .pop();
        let count = |sql: &str| c.query_row(sql, [], |r| r.get::<_, i64>(0));
        Ok(Status {
            runtime,
            pending_approvals: count("SELECT count(*) FROM approvals WHERE status='pending'")?,
            running_jobs: count("SELECT count(*) FROM jobs WHERE status='running'")?,
            queued_jobs: count("SELECT count(*) FROM jobs WHERE status='queued'")?,
            problem_posts: count(
                "SELECT count(*) FROM outbox WHERE state IN ('failed','ambiguous','blocked')",
            )?,
        })
    }
    fn threads(&mut self, controls: &[String], limit: usize) -> Result<Vec<Row>> {
        let clause = filter("control", controls);
        rows(
            self.0,
            &format!("{THREAD}{clause} ORDER BY updated DESC,id LIMIT {limit}"),
            rusqlite::params_from_iter(controls),
        )
    }
    fn threads_needing_attention(&mut self) -> Result<Vec<Row>> {
        rows(self.0,
            &format!("{THREAD} WHERE control='paused' OR (control='active' AND status='blocked') ORDER BY updated DESC,id"), [])
    }
    fn thread(&mut self, id: &str) -> Result<Option<Row>> {
        Ok(rows(self.0, &format!("{THREAD} WHERE id=?"), [id])?.pop())
    }
    fn thread_messages(&mut self, id: &str, limit: i64) -> Result<Vec<Row>> {
        let mut messages = rows(
            self.0,
            "SELECT event_id,ts,thread_ts,sender,text,source,meta_json FROM messages \
            WHERE workspace||':'||channel||':'||root_ts=? ORDER BY CAST(ts AS REAL) DESC LIMIT ?",
            params![id, limit],
        )?;
        messages.reverse();
        Ok(messages)
    }
    fn thread_notes(&mut self, id: &str) -> Result<Option<Row>> {
        Ok(rows(
            self.0,
            "SELECT revision,data_json FROM notes WHERE session_id=? ORDER BY revision DESC LIMIT 1",
            [id],
        )?
        .pop())
    }
    fn owner_instructions(&mut self, id: &str) -> Result<Vec<Row>> {
        rows(
            self.0,
            "SELECT id,ref,created,state,payload_json FROM thread_inbox \
            WHERE session_id=? AND kind='owner_instruction' ORDER BY id DESC LIMIT 20",
            [id],
        )
    }
    fn thread_workers(&mut self, id: &str) -> Result<Vec<Row>> {
        rows(
            self.0,
            &format!("{WORKER} WHERE session_id=? ORDER BY created,rowid"),
            [id],
        )
    }
    fn thread_jobs(&mut self, id: &str) -> Result<Vec<Row>> {
        rows(
            self.0,
            &format!(
                "{JOB} WHERE session_id=? ORDER BY \
            (SELECT created FROM workers WHERE workers.id=jobs.worker_id),\
            (SELECT rowid FROM workers WHERE workers.id=jobs.worker_id),queued_at,rowid"
            ),
            [id],
        )
    }
    fn thread_posts(&mut self, id: &str) -> Result<Vec<Row>> {
        rows(
            self.0,
            &format!("{OUTBOX} WHERE session_id=? ORDER BY id"),
            [id],
        )
    }
    fn workers(&mut self, statuses: &[String], limit: usize) -> Result<Vec<Row>> {
        let clause = filter("status", statuses);
        rows(
            self.0,
            &format!("{WORKER}{clause} ORDER BY updated DESC,id LIMIT {limit}"),
            rusqlite::params_from_iter(statuses),
        )
    }
    fn active_jobs(&mut self, limit: usize) -> Result<Vec<Row>> {
        rows(
            self.0,
            &format!(
                "{JOB} WHERE status IN ('running','queued') ORDER BY \
            CASE status WHEN 'running' THEN 0 ELSE 1 END, \
            CASE status WHEN 'running' THEN started_at ELSE queued_at END,rowid LIMIT {limit}"
            ),
            [],
        )
    }
    fn all_jobs(&mut self, limit: usize) -> Result<Vec<Row>> {
        rows(
            self.0,
            &format!(
                "{JOB} ORDER BY CASE WHEN status IN ('running','queued') THEN 0 ELSE 1 END, \
                queued_at DESC,rowid DESC LIMIT {limit}"
            ),
            [],
        )
    }
    fn approvals(&mut self, statuses: &[String], limit: usize) -> Result<Vec<Row>> {
        let clause = filter("status", statuses);
        rows(
            self.0,
            &format!(
                "SELECT id,worker_id,job_id,session_id,backend_request_id,kind,summary,\
                detail_json,status,scope,decided_by,created,decided_at,expires_at FROM approvals\
                {clause} ORDER BY created DESC,id LIMIT {limit}"
            ),
            rusqlite::params_from_iter(statuses),
        )
    }
    fn posts(&mut self, states: &[String], limit: usize) -> Result<Vec<Row>> {
        let clause = filter("state", states);
        rows(
            self.0,
            &format!("{OUTBOX}{clause} ORDER BY id DESC LIMIT {limit}"),
            rusqlite::params_from_iter(states),
        )
    }
    fn activity(&mut self, limit: usize) -> Result<Vec<Row>> {
        rows(self.0, &format!("SELECT id,time,actor,action,target,details_json FROM audit ORDER BY id DESC LIMIT {limit}"), [])
    }
    fn obligations(&mut self, limit: usize) -> Result<Vec<Row>> {
        rows(
            self.0,
            &format!(
                "SELECT id,session_id,kind,summary,created,due,state,\
            state_json AS disposition_json,updated FROM obligations ORDER BY created DESC,id LIMIT {limit}"
            ),
            [],
        )
    }
    fn busy_machines(&mut self) -> Result<Vec<Row>> {
        rows(
            self.0,
            "SELECT machine,count(*) AS busy FROM workers w JOIN jobs j ON j.worker_id=w.id \
            WHERE j.status IN ('queued','running') GROUP BY machine",
            [],
        )
    }
    fn worker_machines(&mut self) -> Result<Vec<Row>> {
        rows(self.0, "SELECT id,machine FROM workers", [])
    }
    fn thread_files(&mut self, id: &str) -> Result<Option<Vec<MessageFiles>>> {
        let c = self.0;
        let rows: Vec<MessageFiles> = c
            .prepare("SELECT ts,sender,attachments_json FROM messages WHERE workspace||':'||channel||':'||root_ts=? ORDER BY CAST(ts AS REAL),id")?
            .query_map([id], |r| Ok(MessageFiles { ts: r.get(0)?, sender: r.get(1)?, attachments: r.get(2)? }))?
            .collect::<rusqlite::Result<_>>()?;
        let known: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM threads WHERE id=?)",
            [id],
            |r| r.get(0),
        )?;
        Ok(known.then_some(rows))
    }
    fn attachments_mentioning(&mut self, file: &str) -> Result<Vec<String>> {
        Ok(self
            .0
            .prepare("SELECT attachments_json FROM messages WHERE instr(attachments_json,?)>0 ORDER BY id DESC")?
            .query_map([format!("\"{file}\"")], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    fn latest_thread_in(&mut self, workspace: &str, channel: &str) -> Result<Option<String>> {
        Ok(self.0.query_row(
            "SELECT id FROM threads WHERE workspace=? AND channel=? ORDER BY updated DESC, rowid DESC LIMIT 1",
            params![workspace, channel],
            |r| r.get::<_, String>(0),
        )
        .optional()?)
    }
    fn thread_exists(&mut self, id: &str) -> Result<bool> {
        Ok(self
            .0
            .query_row("SELECT 1 FROM threads WHERE id=?", [id], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .is_some())
    }
    fn approval_exists(&mut self, id: &str) -> Result<bool> {
        Ok(self
            .0
            .query_row("SELECT 1 FROM approvals WHERE id=?", [id], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .is_some())
    }
}

impl OwnerNotes for Sqlite<'_> {
    fn notes_revision(&mut self, session: &str) -> Result<i64> {
        Ok(self.0.query_row(
            "SELECT COALESCE(MAX(revision),0) FROM notes WHERE session_id=?",
            [session],
            |r| r.get(0),
        )?)
    }
    fn write_owner_notes(
        &mut self,
        session: &str,
        revision: i64,
        actor: &str,
        data: &str,
        now: f64,
    ) -> Result<()> {
        self.0.execute(
            "INSERT INTO notes(session_id,revision,actor,data_json,source,created) VALUES(?,?,?,?,'control',?)",
            params![session, revision, actor, data, now],
        )?;
        self.0.execute(
            "INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,?,'notes.write',?,?)",
            params![now, actor, session, json!({"revision": revision}).to_string()],
        )?;
        Ok(())
    }
}
