//! The read models behind the control API and the dashboard, and the owner's
//! notes edits.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{
    ActivityView, ApprovalView, InstructionView, JobView, MachineLoad, MessageFiles, MessageView,
    NotesView, ObligationView, OwnerNotes, PostView, RuntimeStatus, Status, ThreadView, Views,
    WorkerMachine, WorkerView,
};
use rusqlite::{params, Connection, OptionalExtension, Params, Row};
use serde_json::json;

const THREAD:&str="SELECT id,workspace,channel,root_ts,status,control,pause_reason,turns,wait_streak,no_progress,last_reply_hash,reset_at,summary,decisions_json,context_json,debriefed_turn,last_unsolicited,created,updated,version,control_json,throttled_until,driver FROM threads";
const WORKER:&str="SELECT id,session_id,machine,workspace,backend,role,ephemeral,backend_session_id,status,summary,last_result_json,slot,created,updated FROM workers";
const JOB:&str="SELECT id,worker_id,session_id,brief,join_group,inbox_id,deliverable,fetch_repo,fetch_ref,status,attempt,reported,result_json,error,queued_at,started_at,finished_at,work_item_id,target_sha,target_tree,COALESCE((SELECT role FROM workers WHERE workers.id=jobs.worker_id),''),tags_json FROM jobs";
const OUTBOX:&str="SELECT id,idem_key,session_id,kind,channel,thread_ts,text,meta_json,filename,\"after\",state,attempts,retry_at,sent_ts,error,created,blob IS NOT NULL,delivered_at,trigger_event,trigger_class,answers_json FROM outbox";

fn thread(r: &Row<'_>) -> rusqlite::Result<ThreadView> {
    Ok(ThreadView {
        id: r.get(0)?,
        workspace: r.get(1)?,
        channel: r.get(2)?,
        root_ts: r.get(3)?,
        status: r.get(4)?,
        control: r.get(5)?,
        pause_reason: r.get(6)?,
        turns: r.get(7)?,
        wait_streak: r.get(8)?,
        no_progress: r.get(9)?,
        last_reply_hash: r.get(10)?,
        reset_at: r.get(11)?,
        summary: r.get(12)?,
        decisions_json: r.get(13)?,
        context_json: r.get(14)?,
        debriefed_turn: r.get(15)?,
        last_unsolicited: r.get(16)?,
        created: r.get(17)?,
        updated: r.get(18)?,
        version: r.get(19)?,
        control_detail_json: r.get(20)?,
        throttled_until: r.get(21)?,
        driver: r.get(22)?,
    })
}
fn worker(r: &Row<'_>) -> rusqlite::Result<WorkerView> {
    Ok(WorkerView {
        id: r.get(0)?,
        session_id: r.get(1)?,
        machine: r.get(2)?,
        workspace: r.get(3)?,
        backend: r.get(4)?,
        role: r.get(5)?,
        ephemeral: r.get::<_, i64>(6)? != 0,
        backend_session_id: r.get(7)?,
        status: r.get(8)?,
        summary: r.get(9)?,
        last_result_json: r.get(10)?,
        slot: r.get(11)?,
        created: r.get(12)?,
        updated: r.get(13)?,
    })
}
fn job(r: &Row<'_>) -> rusqlite::Result<JobView> {
    Ok(JobView {
        id: r.get(0)?,
        worker_id: r.get(1)?,
        session_id: r.get(2)?,
        brief: r.get(3)?,
        join_group: r.get(4)?,
        inbox_id: r.get(5)?,
        deliverable: r.get(6)?,
        fetch_repo: r.get(7)?,
        fetch_ref: r.get(8)?,
        status: r.get(9)?,
        attempt: r.get(10)?,
        reported: r.get::<_, i64>(11)? != 0,
        result_json: r.get(12)?,
        error: r.get(13)?,
        queued_at: r.get(14)?,
        started_at: r.get(15)?,
        finished_at: r.get(16)?,
        work_item_id: r.get(17)?,
        target_sha: r.get(18)?,
        target_tree: r.get(19)?,
        role: r.get(20)?,
        tags: serde_json::from_str(&r.get::<_, String>(21)?).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(21, rusqlite::types::Type::Text, Box::new(e))
        })?,
    })
}
fn post(r: &Row<'_>) -> rusqlite::Result<PostView> {
    Ok(PostView {
        id: r.get(0)?,
        idem_key: r.get(1)?,
        session_id: r.get(2)?,
        kind: r.get(3)?,
        channel: r.get(4)?,
        thread_ts: r.get(5)?,
        text: r.get(6)?,
        meta_json: r.get(7)?,
        filename: r.get(8)?,
        after: r.get(9)?,
        state: r.get(10)?,
        attempts: r.get(11)?,
        retry_at: r.get(12)?,
        sent_ts: r.get(13)?,
        error: r.get(14)?,
        created: r.get(15)?,
        has_file: r.get(16)?,
        delivered_at: r.get(17)?,
        trigger_event: r.get(18)?,
        trigger_class: r.get(19)?,
        answers_json: r.get(20)?,
    })
}

/// Every row of `sql` as `read` makes it.
fn rows<T>(
    c: &Connection,
    sql: &str,
    params: impl Params,
    read: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    Ok(c.prepare(sql)?
        .query_map(params, read)?
        .collect::<rusqlite::Result<_>>()?)
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
        let runtime = c
            .query_row(
                "SELECT started_at,slack_status FROM runtime WHERE id=1",
                [],
                |r| {
                    Ok(RuntimeStatus {
                        started_at: r.get(0)?,
                        slack_status: r.get(1)?,
                    })
                },
            )
            .optional()?;
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
    fn threads(&mut self, controls: &[String], limit: usize) -> Result<Vec<ThreadView>> {
        let clause = filter("control", controls);
        rows(
            self.0,
            &format!("{THREAD}{clause} ORDER BY updated DESC,id LIMIT {limit}"),
            rusqlite::params_from_iter(controls),
            thread,
        )
    }
    fn threads_needing_attention(&mut self) -> Result<Vec<ThreadView>> {
        rows(self.0,
            &format!("{THREAD} WHERE control='paused' OR (control='active' AND status='blocked') ORDER BY updated DESC,id"), [], thread)
    }
    fn thread(&mut self, id: &str) -> Result<Option<ThreadView>> {
        Ok(rows(self.0, &format!("{THREAD} WHERE id=?"), [id], thread)?.pop())
    }
    fn thread_messages(&mut self, id: &str, limit: i64) -> Result<Vec<MessageView>> {
        let mut messages = rows(
            self.0,
            "SELECT event_id,ts,thread_ts,sender,text,source,meta_json FROM messages \
            WHERE workspace||':'||channel||':'||root_ts=? ORDER BY CAST(ts AS REAL) DESC LIMIT ?",
            params![id, limit],
            |r| {
                Ok(MessageView {
                    event_id: r.get(0)?,
                    ts: r.get(1)?,
                    thread_ts: r.get(2)?,
                    sender: r.get(3)?,
                    text: r.get(4)?,
                    source: r.get(5)?,
                    meta_json: r.get(6)?,
                })
            },
        )?;
        messages.reverse();
        Ok(messages)
    }
    fn thread_notes(&mut self, id: &str) -> Result<Option<NotesView>> {
        Ok(rows(
            self.0,
            "SELECT revision,data_json FROM notes WHERE session_id=? ORDER BY revision DESC LIMIT 1",
            [id],
            |r| {
                Ok(NotesView {
                    revision: r.get(0)?,
                    data_json: r.get(1)?,
                })
            },
        )?
        .pop())
    }
    fn owner_instructions(&mut self, id: &str) -> Result<Vec<InstructionView>> {
        rows(
            self.0,
            "SELECT id,ref,created,state,payload_json FROM thread_inbox \
            WHERE session_id=? AND kind='owner_instruction' ORDER BY id DESC LIMIT 20",
            [id],
            |r| {
                Ok(InstructionView {
                    id: r.get(0)?,
                    reference: r.get(1)?,
                    created: r.get(2)?,
                    state: r.get(3)?,
                    payload_json: r.get(4)?,
                })
            },
        )
    }
    fn thread_workers(&mut self, id: &str) -> Result<Vec<WorkerView>> {
        rows(
            self.0,
            &format!("{WORKER} WHERE session_id=? ORDER BY created,rowid"),
            [id],
            worker,
        )
    }
    fn thread_jobs(&mut self, id: &str) -> Result<Vec<JobView>> {
        rows(
            self.0,
            &format!(
                "{JOB} WHERE session_id=? ORDER BY \
            (SELECT created FROM workers WHERE workers.id=jobs.worker_id),\
            (SELECT rowid FROM workers WHERE workers.id=jobs.worker_id),queued_at,rowid"
            ),
            [id],
            job,
        )
    }
    fn thread_posts(&mut self, id: &str) -> Result<Vec<PostView>> {
        rows(
            self.0,
            &format!("{OUTBOX} WHERE session_id=? ORDER BY id"),
            [id],
            post,
        )
    }
    fn workers(&mut self, statuses: &[String], limit: usize) -> Result<Vec<WorkerView>> {
        let clause = filter("status", statuses);
        rows(
            self.0,
            &format!("{WORKER}{clause} ORDER BY updated DESC,id LIMIT {limit}"),
            rusqlite::params_from_iter(statuses),
            worker,
        )
    }
    fn active_jobs(&mut self, limit: usize) -> Result<Vec<JobView>> {
        rows(
            self.0,
            &format!(
                "{JOB} WHERE status IN ('running','queued') ORDER BY \
            CASE status WHEN 'running' THEN 0 ELSE 1 END, \
            CASE status WHEN 'running' THEN started_at ELSE queued_at END,rowid LIMIT {limit}"
            ),
            [],
            job,
        )
    }
    fn all_jobs(&mut self, limit: usize) -> Result<Vec<JobView>> {
        rows(
            self.0,
            &format!(
                "{JOB} ORDER BY CASE WHEN status IN ('running','queued') THEN 0 ELSE 1 END, \
                queued_at DESC,rowid DESC LIMIT {limit}"
            ),
            [],
            job,
        )
    }
    fn approvals(&mut self, statuses: &[String], limit: usize) -> Result<Vec<ApprovalView>> {
        let clause = filter("status", statuses);
        rows(
            self.0,
            &format!(
                "SELECT id,worker_id,job_id,session_id,backend_request_id,kind,summary,\
                detail_json,status,scope,decided_by,created,decided_at,expires_at FROM approvals\
                {clause} ORDER BY created DESC,id LIMIT {limit}"
            ),
            rusqlite::params_from_iter(statuses),
            |r| {
                Ok(ApprovalView {
                    id: r.get(0)?,
                    worker_id: r.get(1)?,
                    job_id: r.get(2)?,
                    session_id: r.get(3)?,
                    backend_request_id: r.get(4)?,
                    kind: r.get(5)?,
                    summary: r.get(6)?,
                    detail_json: r.get(7)?,
                    status: r.get(8)?,
                    scope: r.get(9)?,
                    decided_by: r.get(10)?,
                    created: r.get(11)?,
                    decided_at: r.get(12)?,
                    expires_at: r.get(13)?,
                })
            },
        )
    }
    fn posts(&mut self, states: &[String], limit: usize) -> Result<Vec<PostView>> {
        let clause = filter("state", states);
        rows(
            self.0,
            &format!("{OUTBOX}{clause} ORDER BY id DESC LIMIT {limit}"),
            rusqlite::params_from_iter(states),
            post,
        )
    }
    fn activity(&mut self, limit: usize) -> Result<Vec<ActivityView>> {
        rows(
            self.0,
            &format!("SELECT id,time,actor,action,target,details_json FROM audit ORDER BY id DESC LIMIT {limit}"),
            [],
            |r| {
                Ok(ActivityView {
                    id: r.get(0)?,
                    time: r.get(1)?,
                    actor: r.get(2)?,
                    action: r.get(3)?,
                    target: r.get(4)?,
                    details_json: r.get(5)?,
                })
            },
        )
    }
    fn obligations(&mut self, limit: usize) -> Result<Vec<ObligationView>> {
        rows(
            self.0,
            &format!(
                "SELECT id,session_id,kind,summary,created,due,state,\
            state_json,updated FROM obligations ORDER BY created DESC,id LIMIT {limit}"
            ),
            [],
            |r| {
                Ok(ObligationView {
                    id: r.get(0)?,
                    session_id: r.get(1)?,
                    kind: r.get(2)?,
                    summary: r.get(3)?,
                    created: r.get(4)?,
                    due: r.get(5)?,
                    state: r.get(6)?,
                    disposition_json: r.get(7)?,
                    updated: r.get(8)?,
                })
            },
        )
    }
    fn busy_machines(&mut self) -> Result<Vec<MachineLoad>> {
        rows(
            self.0,
            "SELECT machine,count(*) FROM workers w JOIN jobs j ON j.worker_id=w.id \
            WHERE j.status IN ('queued','running') GROUP BY machine",
            [],
            |r| {
                Ok(MachineLoad {
                    machine: r.get(0)?,
                    busy: r.get(1)?,
                })
            },
        )
    }
    fn worker_machines(&mut self) -> Result<Vec<WorkerMachine>> {
        rows(self.0, "SELECT id,machine FROM workers", [], |r| {
            Ok(WorkerMachine {
                id: r.get(0)?,
                machine: r.get(1)?,
            })
        })
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
