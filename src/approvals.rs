//! Approval state and audit are committed before callers observe decisions.
use super::Store;
use anyhow::{bail, Result};
pub use fridica_core::store::{ApprovalStart as Started, NewApproval as NewRequest, Settlement};
use fridica_core::worker::{Approval, ApprovalDecision};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;

const SELECT: &str = "SELECT id,worker_id,job_id,session_id,backend_request_id,kind,summary,detail_json,status,scope,decided_by,created,decided_at,expires_at FROM approvals";
fn read(r: &rusqlite::Row<'_>) -> rusqlite::Result<Approval> {
    let raw: String = r.get(7)?;
    let detail = serde_json::from_str(&raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(Approval {
        id: r.get(0)?,
        worker_id: r.get(1)?,
        job_id: r.get(2)?,
        session_id: r.get(3)?,
        backend_request_id: r.get(4)?,
        kind: r.get(5)?,
        summary: r.get(6)?,
        detail,
        status: r.get(8)?,
        scope: r.get(9)?,
        decided_by: r.get(10)?,
        created: r.get(11)?,
        decided_at: r.get(12)?,
        expires_at: r.get(13)?,
    })
}
pub async fn get(store: &Store, id: String) -> Result<Option<Approval>> {
    store.call(move |c| get_tx(c, &id)).await
}
pub fn get_tx(c: &Connection, id: &str) -> Result<Option<Approval>> {
    Ok(c.query_row(&format!("{SELECT} WHERE id=?"), [id], read)
        .optional()?)
}
pub async fn pending(store: &Store, limit: usize) -> Result<Vec<Approval>> {
    store.call(move |c| pending_tx(c, limit)).await
}
pub fn pending_tx(c: &Connection, limit: usize) -> Result<Vec<Approval>> {
    Ok(c.prepare(&format!(
        "{SELECT} WHERE status='pending' ORDER BY created DESC,rowid DESC LIMIT ?"
    ))?
    .query_map([limit.min(1000) as i64], read)?
    .collect::<rusqlite::Result<_>>()?)
}
fn audit(
    c: &Connection,
    now: f64,
    actor: &str,
    action: &str,
    id: &str,
    details: serde_json::Value,
) -> Result<()> {
    c.execute(
        "INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,?,?,?,?)",
        params![now, actor, action, id, details.to_string()],
    )?;
    Ok(())
}
pub async fn begin(store: &Store, input: NewRequest) -> Result<Started> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let started = begin_tx(&tx, &input)?;
            tx.commit()?;
            Ok(started)
        })
        .await
}
pub fn begin_tx(tx: &Connection, input: &NewRequest) -> Result<Started> {
    let NewRequest {
        id,
        worker: w,
        job: j,
        request: r,
        automatic,
        now,
        expires_at,
    } = input.clone();
    if id.is_empty() || !now.is_finite() || !expires_at.is_finite() || expires_at <= now {
        bail!("invalid approval identity/deadline");
    }
    {
        let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN workers w ON w.id=j.worker_id JOIN threads t ON t.id=j.session_id WHERE j.id=? AND j.worker_id=? AND j.session_id=? AND j.status='running' AND j.attempt=? AND w.status IN ('running','awaiting_approval') AND w.session_id=j.session_id AND w.machine=? AND w.workspace=? AND w.backend=? AND t.control='active')",params![j.id,w.id,j.session_id,j.attempt,w.machine,w.workspace,w.backend],|r|r.get(0))?;
        if !valid || j.worker_id != w.id || w.session_id != j.session_id {
            return Ok(Started::Immediate(ApprovalDecision::Deny));
        }
        if let Some(decision) = automatic {
            audit(
                tx,
                now,
                "policy",
                if decision == ApprovalDecision::Deny {
                    "approval.deny"
                } else {
                    "approval.allow"
                },
                &w.id,
                json!({"kind":r.kind,"job_id":j.id,"attempt":j.attempt}),
            )?;
            return Ok(Started::Immediate(decision));
        }
        tx.execute("INSERT INTO approvals(id,worker_id,job_id,session_id,backend_request_id,kind,summary,detail_json,created,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?)",params![id,w.id,j.id,j.session_id,r.backend_request_id,r.kind,r.summary,r.detail.to_string(),now,expires_at])?;
        tx.execute(
            "UPDATE workers SET status='awaiting_approval',updated=? WHERE id=?",
            params![now, w.id],
        )?;
        // Existing audit storage carries the job-attempt fence, without changing
        // the legacy approval detail object or inventing a new schema version.
        audit(
            tx,
            now,
            "system",
            "approval.requested",
            &id,
            json!({"job_id":j.id,"attempt":j.attempt}),
        )?;
        Ok(Started::Pending)
    }
}
fn restore(c: &Connection, worker: &str, now: f64) -> Result<()> {
    c.execute("UPDATE workers SET status='running',updated=? WHERE id=? AND status='awaiting_approval' AND EXISTS(SELECT 1 FROM jobs WHERE worker_id=workers.id AND status='running') AND NOT EXISTS(SELECT 1 FROM approvals WHERE worker_id=workers.id AND status='pending')",params![now,worker])?;
    Ok(())
}
/// A late decision expires or cancels the request and returns false. Neither a
/// terminal request nor an old job attempt can be approved by a delayed control.
pub async fn settle(
    store: &Store,
    id: String,
    settlement: Settlement,
    actor: String,
    now: f64,
) -> Result<bool> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let accepted = settle_tx(&tx, &id, settlement, &actor, now)?;
            tx.commit()?;
            Ok(accepted)
        })
        .await
}
pub fn settle_tx(
    tx: &Connection,
    id: &str,
    settlement: Settlement,
    actor: &str,
    now: f64,
) -> Result<bool> {
    if !now.is_finite() {
        bail!("invalid approval time");
    }
    {
        let Some(a) = tx
            .query_row(&format!("{SELECT} WHERE id=?"), [id], read)
            .optional()?
        else {
            return Ok(false);
        };
        if a.status != "pending" {
            return Ok(false);
        }
        let live:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN workers w ON w.id=j.worker_id JOIN threads t ON t.id=j.session_id WHERE j.id=? AND j.worker_id=? AND j.session_id=? AND j.status='running' AND w.status IN ('running','awaiting_approval') AND t.control='active' AND j.attempt=(SELECT json_extract(details_json,'$.attempt') FROM audit WHERE action='approval.requested' AND target=? ORDER BY id DESC LIMIT 1))",params![a.job_id,a.worker_id,a.session_id,id],|r|r.get(0))?;
        let accepted = live && now < a.expires_at;
        let (status, scope, who) = match settlement {
            Settlement::Decide(_) if !live => ("cancelled", "once", "system"),
            Settlement::Decide(_) if now >= a.expires_at => ("expired", "once", "timeout"),
            Settlement::Decide(ApprovalDecision::Once) => ("approved", "once", actor),
            Settlement::Decide(ApprovalDecision::Session) => ("approved", "session", actor),
            Settlement::Decide(ApprovalDecision::Deny) => ("denied", "once", actor),
            Settlement::Expire => ("expired", "once", actor),
            Settlement::Cancel => ("cancelled", "once", actor),
        };
        tx.execute(
            "UPDATE approvals SET status=?,scope=?,decided_by=?,decided_at=? WHERE id=?",
            params![status, scope, who, now, id],
        )?;
        audit(
            tx,
            now,
            who,
            &format!("approval.{status}"),
            id,
            json!({"scope":scope}),
        )?;
        restore(tx, &a.worker_id, now)?;
        Ok(!matches!(settlement, Settlement::Decide(_)) || accepted)
    }
}
pub async fn cancel_worker(store: &Store, worker: String, now: f64) -> Result<()> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            cancel_worker_tx(&tx, &worker, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}
pub fn cancel_worker_tx(tx: &Connection, worker: &str, now: f64) -> Result<()> {
    if !now.is_finite() {
        bail!("invalid approval time");
    }
    {
        let ids = tx
            .prepare("SELECT id FROM approvals WHERE worker_id=? AND status='pending'")?
            .query_map([worker], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for id in ids {
            tx.execute("UPDATE approvals SET status='cancelled',decided_by='interrupted',decided_at=? WHERE id=?",params![now,id])?;
            audit(
                tx,
                now,
                "interrupted",
                "approval.cancelled",
                &id,
                json!({"scope":"once"}),
            )?;
        }
        restore(tx, worker, now)?;
        Ok(())
    }
}

/// Reload invalidates pending grants before the new policy becomes visible.
pub async fn cancel_all(store: &Store, now: f64) -> Result<()> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            cancel_all_tx(&tx, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}
pub fn cancel_all_tx(tx: &Connection, now: f64) -> Result<()> {
    if !now.is_finite() {
        bail!("invalid approval time");
    }
    {
        let rows = tx
            .prepare("SELECT id,worker_id FROM approvals WHERE status='pending'")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        tx.execute("UPDATE approvals SET status='cancelled',decided_by='reconfigured',decided_at=? WHERE status='pending'",[now])?;
        for (id, worker) in rows {
            audit(
                tx,
                now,
                "reconfigured",
                "approval.cancelled",
                &id,
                json!({"scope":"once"}),
            )?;
            restore(tx, &worker, now)?;
        }
        Ok(())
    }
}
