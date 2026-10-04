//! Parent controls commit with the actor, before process effects. The immutable
//! intent names an exact interrupt attempt; acknowledgements are separate events.
use super::{work, Store};
use anyhow::{Context, Result};
use fridica_core::parent::{ParentRequest, WorkerControl, WorkerOperation};
use fridica_core::store::PendingWorkerControl;
pub use fridica_core::store::WorkerControlIntent as Intent;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

pub fn jobs_tx(c: &Connection, session: &str) -> Result<Vec<Value>> {
    let rows: Vec<String> = c.prepare("SELECT json_object('id',id,'worker_id',worker_id,'status',status,'attempt',attempt) FROM jobs WHERE session_id=? AND status IN ('queued','running') ORDER BY id")?
        .query_map([session], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    rows.into_iter()
        .map(|r| Ok(serde_json::from_str(&r)?))
        .collect()
}

pub fn pending_tx(c: &Connection, session: &str) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM replay_events WHERE kind='parent_worker_control' AND complete=0 AND json_extract(payload_json,'$.session')=?)",[session],|r|r.get(0))?)
}

pub fn recent_tx(c: &Connection, session: &str) -> Result<Vec<Value>> {
    let rows:Vec<String>=c.prepare("SELECT json_object('id',e.seq,'created',e.time,'request',json(e.payload_json),'complete',json(CASE WHEN e.complete THEN 'true' ELSE 'false' END),'outcome',(SELECT json_extract(r.payload_json,'$.outcome') FROM replay_events r WHERE r.kind='parent_worker_control_result' AND json_extract(r.payload_json,'$.intent')=e.seq ORDER BY r.seq DESC LIMIT 1)) FROM replay_events e WHERE e.kind='parent_worker_control' AND json_extract(e.payload_json,'$.session')=? ORDER BY e.seq DESC LIMIT 20")?
        .query_map([session],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
    rows.into_iter()
        .map(|raw| Ok(serde_json::from_str(&raw)?))
        .collect()
}

pub fn interrupt_pending_tx(c: &Connection, job: &str, attempt: u32) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM replay_events WHERE kind='parent_worker_control' AND complete=0 AND json_extract(payload_json,'$.op')='interrupt' AND json_extract(payload_json,'$.job')=? AND json_extract(payload_json,'$.attempt')=?)",params![job,attempt],|r|r.get(0))?)
}

/// Admission does not change thread.version, so compare targeted active jobs too.
pub fn current_tx(
    c: &Connection,
    request: &ParentRequest,
    controls: &[WorkerControl],
) -> Result<bool> {
    if controls.is_empty() {
        return Ok(true);
    }
    let session = request.session["id"]
        .as_str()
        .context("missing control session")?;
    let actual = jobs_tx(c, session)?;
    let expected = request.session["work"]["jobs"]
        .as_array()
        .context("missing job snapshot")?;
    for control in controls {
        let filter = |j: &&Value| j["worker_id"] == control.worker_id;
        if actual.iter().filter(filter).collect::<Vec<_>>()
            != expected.iter().filter(filter).collect::<Vec<_>>()
        {
            return Ok(false);
        }
        let belongs: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM workers WHERE id=? AND session_id=?)",
            params![control.worker_id, session],
            |r| r.get(0),
        )?;
        if !belongs {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn enqueue_tx(
    c: &Connection,
    request: &ParentRequest,
    controls: &[WorkerControl],
    now: f64,
) -> Result<()> {
    let session = request.session["id"]
        .as_str()
        .context("missing control session")?;
    for control in controls {
        let running = request.session["work"]["jobs"]
            .as_array()
            .context("missing job snapshot")?
            .iter()
            .find(|j| j["worker_id"] == control.worker_id && j["status"] == "running");
        let intent = Intent {
            session: session.into(),
            inbox: request.inbox_id,
            worker: control.worker_id.clone(),
            op: control.op,
            job: running.and_then(|j| j["id"].as_str()).map(str::to_owned),
            attempt: running
                .and_then(|j| j["attempt"].as_u64())
                .map(u32::try_from)
                .transpose()?,
        };
        c.execute("INSERT INTO replay_events(kind,time,payload_json,complete) VALUES('parent_worker_control',?,?,0)",params![now,serde_json::to_string(&intent)?])?;
        let seq = c.last_insert_rowid();
        match intent.op {
            WorkerOperation::Stop => work::stop_tx(c, &intent.worker, "parent", now)?,
            WorkerOperation::Interrupt => {
                c.execute("UPDATE approvals SET status='cancelled',decided_by='system',decided_at=? WHERE job_id=? AND status='pending'",params![now,intent.job])?;
                c.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'parent','worker.interrupt',?,?)",params![now,intent.worker,json!({"intent":seq,"job":intent.job,"attempt":intent.attempt}).to_string()])?;
            }
        }
    }
    Ok(())
}

pub async fn pending(store: &Store) -> Result<Vec<(i64, Intent)>> {
    store
        .call(|c| {
            Ok(pending_intents_tx(c)?
                .into_iter()
                .map(|p| (p.seq, p.intent))
                .collect())
        })
        .await
}

/// Up to 128 pending controls, oldest first.
pub fn pending_intents_tx(c: &Connection) -> Result<Vec<PendingWorkerControl>> {
    {
        let rows: Vec<(i64,String)>=c.prepare("SELECT seq,payload_json FROM replay_events WHERE kind='parent_worker_control' AND complete=0 ORDER BY seq LIMIT 128")?
            .query_map([],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        rows.into_iter()
            .map(|(id, raw)| {
                Ok(PendingWorkerControl {
                    seq: id,
                    intent: serde_json::from_str(&raw)?,
                })
            })
            .collect()
    }
}

pub async fn complete(store: &Store, seq: i64, outcome: &'static str, now: f64) -> Result<()> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            complete_tx(&tx, seq, outcome, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}

/// Record that control `seq` was carried out with `outcome`.
pub fn complete_tx(tx: &Connection, seq: i64, outcome: &str, now: f64) -> Result<()> {
    {
        if tx.execute("UPDATE replay_events SET complete=1 WHERE seq=? AND kind='parent_worker_control' AND complete=0",[seq])?==1 {
            let raw:String=tx.query_row("SELECT payload_json FROM replay_events WHERE seq=?",[seq],|r|r.get(0))?;
            let intent:Intent=serde_json::from_str(&raw)?;
            let details=json!({"intent":seq,"outcome":outcome,"job":intent.job,"attempt":intent.attempt});
            tx.execute("INSERT INTO replay_events(kind,time,payload_json) VALUES('parent_worker_control_result',?,?)",params![now,details.to_string()])?;
            tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'parent','worker.control_reconciled',?,?)",params![now,intent.worker,details.to_string()])?;
        }
        Ok(())
    }
}
