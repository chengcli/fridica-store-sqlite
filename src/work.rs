//! Durable job admission and completion. Backend I/O never runs on this thread.
use super::Store;
use anyhow::{bail, Result};
use fridica_core::{
    config::{registry::Registry, Limits},
    worker::{CollectedArtifact, Failure, Job, Outcome, WorkerFailure, WorkerRecord},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
const JOB:&str="SELECT json_object('id',id,'worker_id',worker_id,'session_id',session_id,'brief',brief,'join_group',join_group,'inbox_id',inbox_id,'deliverable',deliverable,'fetch_repo',fetch_repo,'fetch_ref',fetch_ref,'files',json(files_json),'context',context,'snapshot',json(snapshot_json),'fork_from_worker',fork_from_worker,'status',status,'attempt',attempt,'work_item_id',work_item_id,'target_sha',target_sha,'target_tree',target_tree,'retry_of',retry_of,'clearance',clearance) FROM jobs";
const WORKER:&str="SELECT json_object('id',id,'session_id',session_id,'machine',machine,'workspace',workspace,'backend',backend,'role',role,'ephemeral',json(CASE WHEN ephemeral THEN 'true' ELSE 'false' END),'backend_session_id',backend_session_id,'status',status,'slot',slot,'updated',updated) FROM workers";
fn job(c: &Connection, id: &str) -> Result<Job> {
    let raw: String = c.query_row(&format!("{JOB} WHERE id=?"), [id], |r| r.get(0))?;
    Ok(serde_json::from_str(&raw)?)
}
fn worker(c: &Connection, id: &str) -> Result<WorkerRecord> {
    let raw: String = c.query_row(&format!("{WORKER} WHERE id=?"), [id], |r| r.get(0))?;
    Ok(serde_json::from_str(&raw)?)
}
pub async fn get_job(store: &Store, id: String) -> Result<Job> {
    store.call(move |c| job(c, &id)).await
}
pub async fn get_worker(store: &Store, id: String) -> Result<WorkerRecord> {
    store.call(move |c| worker(c, &id)).await
}
pub fn add_worker_tx(c: &Connection, w: &WorkerRecord, now: f64) -> Result<()> {
    if w.id.is_empty() || !now.is_finite() {
        bail!("invalid worker identity/time");
    }
    c.execute("INSERT INTO workers(id,session_id,machine,workspace,backend,role,ephemeral,backend_session_id,status,slot,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        params![w.id,w.session_id,w.machine,w.workspace,w.backend,w.role,w.ephemeral,w.backend_session_id,w.status,i64::try_from(w.slot)?,now,now])?;
    Ok(())
}
pub async fn add_worker(store: &Store, w: WorkerRecord, now: f64) -> Result<()> {
    store.call(move |c| add_worker_tx(c, &w, now)).await
}
pub fn enqueue_tx(c: &Connection, j: &Job, now: f64) -> Result<()> {
    if j.id.is_empty() || j.brief.trim().is_empty() || !now.is_finite() || j.clearance != "worker" {
        bail!("invalid job identity/brief/time");
    }
    let w = worker(c, &j.worker_id)?;
    if w.session_id != j.session_id || w.status == "stopped" {
        bail!("worker unavailable in this thread");
    }
    let snapshot = j.snapshot.as_ref().map(serde_json::to_string).transpose()?;
    c.execute("INSERT INTO jobs(id,worker_id,session_id,brief,join_group,inbox_id,deliverable,fetch_repo,fetch_ref,files_json,context,snapshot_json,fork_from_worker,work_item_id,target_sha,target_tree,queued_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![j.id,j.worker_id,j.session_id,j.brief,j.join_group,j.inbox_id,j.deliverable,j.fetch_repo,j.fetch_ref,serde_json::to_string(&j.files)?,j.context.as_str(),snapshot,j.fork_from_worker,j.work_item_id,j.target_sha,j.target_tree,now])?;
    Ok(())
}
pub async fn enqueue(store: &Store, j: Job, now: f64) -> Result<()> {
    store.call(move |c| enqueue_tx(c, &j, now)).await
}
/// Context and placement load are read inside the actor's snapshot transaction.
pub fn context_tx(c: &Connection, session: &str) -> Result<serde_json::Value> {
    let workers: Vec<String> = c
        .prepare(&format!("{WORKER} WHERE session_id=? ORDER BY id"))?
        .query_map([session], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let workers: Vec<WorkerRecord> = workers
        .iter()
        .map(|s| serde_json::from_str(s))
        .collect::<std::result::Result<_, _>>()?;
    let busy: BTreeMap<String,i64> = c.prepare("SELECT w.machine,count(*) FROM jobs j JOIN workers w ON w.id=j.worker_id WHERE j.status IN ('queued','running') GROUP BY w.machine")?
        .query_map([], |r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
    let mut context = json!({"workers":workers,"busy":busy,"jobs":super::worker_controls::jobs_tx(c,session)?,"controls":super::worker_controls::recent_tx(c,session)?,
        "results":results_tx(c,session)?});
    let elsewhere = elsewhere_tx(c, session)?;
    if !elsewhere.is_empty() {
        context["elsewhere"] = json!(elsewhere);
    }
    // The latest progress note of each running job (#105), for "how is it going".
    let progress: Vec<String> = c.prepare("SELECT json_object('job_id',j.id,'worker_id',j.worker_id,'note',p.text,'at',p.created)
        FROM jobs j JOIN job_progress p ON p.job_id=j.id AND p.attempt=j.attempt
        WHERE j.session_id=? AND j.status='running' AND p.seq=(SELECT MAX(seq) FROM job_progress WHERE job_id=j.id AND attempt=j.attempt)
        ORDER BY j.queued_at,j.rowid")?
        .query_map([session], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !progress.is_empty() {
        context["progress"] = progress
            .iter()
            .map(|r| serde_json::from_str(r))
            .collect::<std::result::Result<Vec<serde_json::Value>, _>>()?
            .into();
    }
    Ok(context)
}
/// This thread's finished jobs, oldest first: what earlier workers found, for
/// the parent and for the context a forked worker inherits.
fn results_tx(c: &Connection, session: &str) -> Result<Vec<serde_json::Value>> {
    let rows: Vec<String> = c.prepare("SELECT json_object('job_id',id,'worker_id',worker_id,'status',status,
            'summary',COALESCE(substr(json_extract(result_json,'$.summary'),1,600),''),'error',substr(error,1,200))
        FROM jobs WHERE session_id=? AND status IN ('done','failed','interrupted') ORDER BY finished_at DESC,rowid DESC LIMIT 10")?
        .query_map([session], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    rows.iter()
        .rev()
        .map(|r| Ok(serde_json::from_str(r)?))
        .collect()
}
/// The snapshot a worker's previous job ran with, and that job's id, so a
/// resumed session is told only what changed since. Jobs that never ran are
/// skipped.
pub async fn previous_snapshot(
    store: &Store,
    worker_id: String,
    job_id: String,
) -> Result<Option<(String, fridica_core::fork::ContextBundle)>> {
    store
        .call(move |c| {
            let row: Option<(String, String)> = c
                .query_row(
                    "SELECT id,snapshot_json FROM jobs WHERE worker_id=? AND id!=? AND attempt>0 AND snapshot_json IS NOT NULL ORDER BY started_at DESC,rowid DESC LIMIT 1",
                    params![worker_id, job_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            Ok(match row {
                Some((id, raw)) => Some((id, serde_json::from_str(&raw)?)),
                None => None,
            })
        })
        .await
}
/// Files attached in this thread by others, newest first, that a delegation
/// may hand to a worker. Fridica's own uploads are left out.
pub fn files_tx(c: &Connection, session: &str) -> Result<Vec<serde_json::Value>> {
    let rows: Vec<(String, String, String)> = c
        .prepare("SELECT attachments_json,ts,sender FROM messages WHERE workspace||':'||channel||':'||root_ts=? AND attachments_json!='[]' ORDER BY CAST(ts AS REAL) DESC LIMIT 100")?
        .query_map([session], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut files = vec![];
    let mut seen = std::collections::BTreeSet::new();
    for (attachments, ts, sender) in rows {
        let attachments: Vec<serde_json::Value> =
            serde_json::from_str(&attachments).unwrap_or_default();
        for a in attachments {
            let Some(id) = a["id"].as_str().filter(|id| !id.is_empty()) else {
                continue;
            };
            if !seen.insert(id.to_owned()) {
                continue;
            }
            let own: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM outbox WHERE kind='upload' AND sent_ts=?)",
                [id],
                |r| r.get(0),
            )?;
            if own {
                continue;
            }
            files.push(json!({"id":id,"name":a["name"],"mimetype":a["mimetype"],"size":a["size"],"ts":ts,"sender":sender}));
            if files.len() >= 20 {
                return Ok(files);
            }
        }
    }
    Ok(files)
}
/// The latest jobs of other threads in the same channel, so a parent can cite
/// or wait for work already running or done instead of starting it again.
/// Other channels are left out: their work may be private to them.
fn elsewhere_tx(c: &Connection, session: &str) -> Result<Vec<serde_json::Value>> {
    let Some((channel, _)) = session.rsplit_once(':') else {
        return Ok(vec![]);
    };
    let prefix = format!("{channel}:");
    let rows: Vec<String> = c.prepare("SELECT json_object('thread',session_id,'brief',substr(brief,1,300),'status',status,
            'summary',COALESCE(substr(json_extract(result_json,'$.summary'),1,300),''),'queued_at',queued_at)
        FROM jobs WHERE substr(session_id,1,length(?1))=?1 AND session_id!=?2 ORDER BY queued_at DESC,rowid DESC LIMIT 10")?
        .query_map(params![prefix, session], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    rows.iter().map(|r| Ok(serde_json::from_str(r)?)).collect()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub queued: Vec<Job>,
    pub running: Vec<Job>,
    pub workers: Vec<WorkerRecord>,
}
pub async fn snapshot(store: &Store) -> Result<Snapshot> {
    store
        .call(|c| {
            let tx = c.transaction()?;
            let read = |sql: String| -> Result<Vec<String>> {
                Ok(tx
                    .prepare(&sql)?
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?)
            };
            let queued = read(format!(
                "{JOB} WHERE status='queued' ORDER BY queued_at,rowid"
            ))?
            .iter()
            .map(|v| serde_json::from_str(v))
            .collect::<std::result::Result<_, _>>()?;
            let running = read(format!(
                "{JOB} WHERE status='running' ORDER BY started_at,rowid"
            ))?
            .iter()
            .map(|v| serde_json::from_str(v))
            .collect::<std::result::Result<_, _>>()?;
            let workers = read(format!("{WORKER} ORDER BY updated DESC,rowid"))?
                .iter()
                .map(|v| serde_json::from_str(v))
                .collect::<std::result::Result<_, _>>()?;
            tx.commit()?;
            Ok(Snapshot {
                queued,
                running,
                workers,
            })
        })
        .await
}
fn notify(c: &Connection, j: &Job, now: f64) -> Result<()> {
    c.execute("INSERT OR IGNORE INTO thread_inbox(session_id,kind,ref,created,dedup_key) VALUES(?,'worker_result',?,?,?)",
        params![j.session_id,j.id,now,format!("worker-result:{}",j.id)])?;
    Ok(())
}
fn cancel(c: &Connection, j: &Job, error: &str, now: f64) -> Result<()> {
    c.execute(
        "UPDATE jobs SET status='cancelled',error=?,finished_at=? WHERE id=? AND status='queued'",
        params![error, now, j.id],
    )?;
    notify(c, j, now)
}
/// Candidate slot selection is advisory. Recheck all durable limits at
/// admission, against the configured `machines` and `limits`.
pub async fn claim(
    store: &Store,
    id: String,
    slot: usize,
    machines: Registry,
    limits: Limits,
    now: f64,
) -> Result<Option<(Job, WorkerRecord)>> {
    store.call(move|c|{
        let tx=c.transaction()?;let mut j=job(&tx,&id)?;if j.status!="queued" || j.clearance!="worker"{return Ok(None);}
        let mut w=worker(&tx,&j.worker_id)?;
        let machine=machines.get(&w.machine);
        let active:bool=tx.query_row("SELECT control='active' FROM threads WHERE id=?",[&j.session_id],|r|r.get(0))?;
        if w.status=="stopped" || machine.is_none(){cancel(&tx,&j,if w.status=="stopped"{"worker stopped"}else{"machine no longer configured"},now)?;tx.commit()?;return Ok(None);}
        let m=machine.unwrap();
        if !j.work_item_id.is_empty(){
            let head:Option<(String,String)>=tx.query_row("SELECT head_sha,head_tree FROM work_items WHERE id=?",[&j.work_item_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            if head!=Some((j.target_sha.clone(),j.target_tree.clone())){cancel(&tx,&j,"work item head changed",now)?;tx.commit()?;return Ok(None);}
        }
        if !active || super::worker_controls::pending_tx(&tx,&j.session_id)? {return Ok(None);}
        if slot==0 || slot>m.max_jobs || (w.slot>0 && w.slot<=m.max_jobs && w.slot!=slot){bail!("invalid or changed sticky slot");}
        let (total,machine_count,worker_count,occupied):(i64,i64,i64,i64)=tx.query_row(
            "SELECT COUNT(*),COALESCE(SUM(w.machine=?),0),COALESCE(SUM(j.worker_id=?),0),COALESCE(SUM(w.machine=? AND w.slot=?),0) FROM jobs j JOIN workers w ON w.id=j.worker_id WHERE j.status='running'",
            params![w.machine,w.id,w.machine,i64::try_from(slot)?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        if total as u64>=limits.max_jobs as u64 || machine_count as u64>=m.max_jobs as u64 || worker_count>0 || occupied>0{return Ok(None);}
        tx.execute("UPDATE jobs SET status='running',started_at=?,finished_at=0,attempt=attempt+1 WHERE id=?",params![now,id])?;
        if w.updated!=0. && now-w.updated>limits.session_timeout && j.retry_of.is_empty() {
            w.backend_session_id.clear();
        }
        tx.execute("UPDATE workers SET status='running',slot=?,backend_session_id=? WHERE id=?",params![i64::try_from(slot)?,w.backend_session_id,w.id])?;
        j.status="running".into();j.attempt+=1;w.slot=slot;
        tx.commit()?;Ok(Some((j,w)))
    }).await
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Completion {
    pub outcome: std::result::Result<Outcome, WorkerFailure>,
    pub artifacts: Vec<CollectedArtifact>,
    pub interrupted: bool,
    pub stopped: bool,
    pub allow_retry: bool,
}
#[derive(Debug, PartialEq)]
pub enum Completed {
    Finished,
    Retried,
    Stale,
}
/// Record a progress note of a running job attempt and queue it for the
/// job's thread (#105). A note for an attempt that is no longer running is
/// dropped: its result is, or soon will be, the thread's news.
pub async fn progress(
    store: &Store,
    job_id: String,
    attempt: u32,
    text: String,
    now: f64,
) -> Result<bool> {
    store.call(move|c|{
        let tx=c.transaction()?;
        let session:Option<String>=tx.query_row("SELECT session_id FROM jobs WHERE id=? AND attempt=? AND status='running'",params![job_id,attempt],|r|r.get(0)).optional()?;
        let Some(session)=session else {return Ok(false)};
        let seq:i64=tx.query_row("SELECT COALESCE(MAX(seq),0)+1 FROM job_progress WHERE job_id=? AND attempt=?",params![job_id,attempt],|r|r.get(0))?;
        tx.execute("INSERT INTO job_progress(job_id,attempt,seq,text,created) VALUES(?,?,?,?,?)",params![job_id,attempt,seq,text,now])?;
        tx.execute("INSERT OR IGNORE INTO thread_inbox(session_id,kind,ref,payload_json,created,dedup_key) VALUES(?,'worker_progress',?,?,?,?)",
            params![session,job_id,json!({"attempt":attempt,"seq":seq}).to_string(),now,format!("worker-progress:{job_id}:{attempt}:{seq}")])?;
        tx.commit()?;
        Ok(true)
    }).await
}
pub async fn complete(
    store: &Store,
    id: String,
    attempt: u32,
    completion: Completion,
    now: f64,
) -> Result<Completed> {
    store.call(move|c|{
        let tx=c.transaction()?;let j=job(&tx,&id)?;
        tx.execute("INSERT INTO replay_events(kind,time,payload_json) VALUES('worker_completion',?,?)",
            params![now,json!({"job_id":id,"attempt":attempt,"completion":completion}).to_string()])?;
        tx.execute("UPDATE replay_events SET complete=1 WHERE kind='worker_call' AND json_extract(payload_json,'$.request.job_id')=? AND json_extract(payload_json,'$.request.attempt')=?",params![id,attempt])?;
        if j.status!="running" || j.attempt!=attempt{tx.commit()?;return Ok(Completed::Stale);}
        let w=worker(&tx,&j.worker_id)?;
        let stopped=completion.stopped || w.status=="stopped";
        let (mut status,result,session,error)=match &completion.outcome {
            Ok(o)=>("done",Some(&o.result),o.backend_session_id.as_str(),String::new()),
            Err(e)=>(match e.kind {Failure::Cancelled=>"cancelled",Failure::Interrupted=>"interrupted",_=>"failed"},None,e.backend_session_id.as_str(),safe_code(&e.code)),
        };
        let interrupted=completion.interrupted || super::worker_controls::interrupt_pending_tx(&tx,&id,attempt)?;
        if interrupted && status!="cancelled" {status="interrupted";}
        if stopped && status=="done"{status="interrupted";}
        let resume=if session.is_empty(){w.backend_session_id.as_str()}else{session};
        let control:String=tx.query_row("SELECT control FROM threads WHERE id=?",[&j.session_id],|r|r.get(0))?;
        let retry=completion.allow_retry && !stopped && !interrupted && j.attempt==1 && !resume.is_empty() && control=="active" &&
            // A usage limit (Failure::RateLimited) is never retried at once into
            // the same limit (#107); the parent sees its reset time instead.
            matches!(&completion.outcome,Err(e) if e.kind==Failure::Execution);
        let retry_at=match &completion.outcome {Err(WorkerFailure{kind:Failure::RateLimited{retry_at},..})=>retry_at.map(|t|t as f64),_=>None};
        let result_json=result.map(serde_json::to_string).transpose()?;
        let summary=result.map(|r|r.summary.as_str()).unwrap_or("");
        let retire=stopped || (w.ephemeral && !retry);
        tx.execute("UPDATE workers SET status=?,updated=?,backend_session_id=CASE WHEN ?='' THEN backend_session_id ELSE ? END,
            summary=CASE WHEN ?='' THEN summary ELSE ? END,last_result_json=COALESCE(?,last_result_json) WHERE id=?",
            params![if retire{"stopped"}else{"idle"},now,session,session,summary,summary,result_json,w.id])?;
        tx.execute("UPDATE approvals SET status='cancelled',decided_by='system',decided_at=? WHERE job_id=? AND status='pending'",params![now,id])?;
        if retry {
            tx.execute("UPDATE jobs SET status='queued',error=?,retry_of=id WHERE id=?",params![error,id])?;
            tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','job.retry',?,?)",params![now,id,json!({"attempt":attempt,"same_session":true}).to_string()])?;
            tx.commit()?;return Ok(Completed::Retried);
        }
        tx.execute("UPDATE jobs SET status=?,result_json=?,error=?,finished_at=?,retry_at=? WHERE id=?",params![status,result_json,error,now,retry_at,id])?;
        for (index,a) in completion.artifacts.iter().enumerate(){
            tx.execute("INSERT INTO artifacts(id,job_id,session_id,machine,path,kind,caption,size,blob,status,error) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                params![format!("artifact:{id}:{index}"),id,j.session_id,w.machine,a.reference.path,a.reference.kind,a.reference.caption,
                i64::try_from(a.data.as_ref().map_or(0,Vec::len))?,a.data,if a.data.is_some(){"ready"}else{"rejected"},safe_code(&a.error)])?;
        }
        notify(&tx,&j,now)?;
        tx.execute("UPDATE threads SET updated=?,version=version+1 WHERE id=?",params![now,j.session_id])?;
        tx.commit()?;Ok(Completed::Finished)
    }).await
}
fn safe_code(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    if s.len() <= 100
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        s.into()
    } else {
        "worker_error".into()
    }
}
pub async fn stop(store: &Store, worker_id: String, now: f64) -> Result<()> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            stop_tx(&tx, &worker_id, "owner", now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}

pub fn stop_tx(c: &Connection, worker_id: &str, actor: &str, now: f64) -> Result<()> {
    let queued: Vec<String> = c
        .prepare(
            "SELECT id FROM jobs WHERE worker_id=? AND status='queued' ORDER BY queued_at,rowid",
        )?
        .query_map([&worker_id], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for id in queued {
        cancel(c, &job(c, &id)?, "worker stopped", now)?;
    }
    c.execute(
        "UPDATE workers SET status='stopped',updated=? WHERE id=?",
        params![now, worker_id],
    )?;
    c.execute("UPDATE threads SET version=version+1,updated=? WHERE id=(SELECT session_id FROM workers WHERE id=?)",params![now,worker_id])?;
    c.execute(
        "INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,?,'worker.stop',?,'{}')",
        params![now, actor, worker_id],
    )?;
    c.execute("UPDATE approvals SET status='cancelled',decided_by='system',decided_at=? WHERE worker_id=? AND status='pending'",params![now,worker_id])?;
    Ok(())
}
/// Startup only, after acquiring the daemon lock and before creating any backend.
pub async fn recover(store: &Store, now: f64) -> Result<usize> {
    store.call(move|c|{
    let tx=c.transaction()?;
    let ids:Vec<String>=tx.prepare("SELECT id FROM jobs WHERE status='running' ORDER BY started_at,rowid")?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
    for id in &ids{
        let j=job(&tx,id)?;
        tx.execute("UPDATE jobs SET status='interrupted',error='daemon_restarted',finished_at=? WHERE id=?",params![now,id])?;
        tx.execute("UPDATE workers SET status=CASE WHEN status='stopped' OR ephemeral THEN 'stopped' ELSE 'idle' END,updated=? WHERE id=?",params![now,j.worker_id])?;
        notify(&tx,&j,now)?;
    }
    tx.execute("UPDATE approvals SET status='cancelled',decided_by='system',decided_at=? WHERE status='pending'",[now])?;
    tx.commit()?;Ok(ids.len())
}).await
}
pub async fn busy_by_machine(store: &Store) -> Result<BTreeMap<String, i64>> {
    store.call(|c|{
    Ok(c.prepare("SELECT w.machine,COUNT(*) FROM jobs j JOIN workers w ON w.id=j.worker_id WHERE j.status IN ('queued','running') GROUP BY w.machine")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?)
}).await
}
