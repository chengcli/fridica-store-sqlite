//! Scoped-fetch intent and completion share the same durable attempt fence as
//! worker execution. A stopped/paused/stale job never consumes fetched context.
use super::Store;
use anyhow::{bail, Result};
use fridica_core::worker::Job;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
fn active(c: &Connection, job: &Job) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN workers w ON w.id=j.worker_id JOIN threads t ON t.id=j.session_id WHERE j.id=? AND j.worker_id=? AND j.session_id=? AND j.attempt=? AND j.fetch_repo=? AND j.fetch_ref=? AND j.status='running' AND w.status='running' AND t.control='active' AND (j.work_item_id='' OR EXISTS(SELECT 1 FROM work_items i WHERE i.id=j.work_item_id AND i.head_sha=j.target_sha AND i.head_tree=j.target_tree)))",params![job.id,job.worker_id,job.session_id,job.attempt,job.fetch_repo,job.fetch_ref],|r|r.get(0))?)
}
pub async fn begin(store: &Store, job: Job, request: Value, now: f64) -> Result<Option<i64>> {
    if !now.is_finite() {
        bail!("invalid fetch time");
    }
    store.call(move |c|{
        let tx=c.transaction()?;
        if !active(&tx,&job)?{return Ok(None);}
        tx.execute("INSERT INTO replay_events(kind,time,payload_json,complete) VALUES('repo_fetch',?,?,0)",params![now,json!({"job_id":job.id,"attempt":job.attempt,"request":request}).to_string()])?;
        let seq=tx.last_insert_rowid();
        tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'policy','repo.fetch.started',?,?)",params![now,job.id,json!({"repo":job.fetch_repo,"ref":job.fetch_ref,"attempt":job.attempt}).to_string()])?;
        tx.commit()?;Ok(Some(seq))
    }).await
}
pub async fn finish(store: &Store, job: Job, seq: i64, result: Value, now: f64) -> Result<bool> {
    if !now.is_finite() {
        bail!("invalid fetch time");
    }
    store.call(move |c|{
        let tx=c.transaction()?;let accepted=active(&tx,&job)?;
        let updated=tx.execute("UPDATE replay_events SET payload_json=json_set(payload_json,'$.result',json(?),'$.accepted',json(?),'$.finished_at',?),complete=1 WHERE seq=? AND kind='repo_fetch' AND complete=0 AND json_extract(payload_json,'$.job_id')=? AND json_extract(payload_json,'$.attempt')=?",params![result.to_string(),if accepted{"true"}else{"false"},now,seq,job.id,job.attempt])?;
        if updated!=1 {bail!("fetch completion is stale or already recorded");}
        let success=result.get("commit").is_some();
        let action=if success && accepted {"repo.fetch"}else if success {"repo.fetch.stale"}else{"repo.fetch.failed"};
        tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'policy',?,?,?)",params![now,action,job.id,json!({"repo":job.fetch_repo,"ref":job.fetch_ref,"attempt":job.attempt,"outcome":result}).to_string()])?;
        tx.commit()?;Ok(accepted)
    }).await
}
