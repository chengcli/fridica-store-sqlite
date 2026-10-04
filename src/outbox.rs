//! Ordered, durable outbox. Delivery attempts are fenced by their attempt count,
//! so a late response from an older attempt cannot acknowledge an operator retry.
use super::Store;
use anyhow::{bail, Context, Result};
use fridica_core::{
    delivery::{ClaimedPost, DeliveryOutcome, Post},
    store::PostOutcome,
    Authority,
};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde_json::{json, Value};

const READY: &str = "SELECT o.* FROM outbox o WHERE o.state='pending' AND o.retry_at<=?
 AND (o.after='' OR EXISTS(SELECT 1 FROM outbox p WHERE p.idem_key=o.after AND p.state='sent'))
 AND NOT EXISTS(SELECT 1 FROM outbox e WHERE e.channel=o.channel AND e.id<o.id
   AND COALESCE(e.thread_ts,e.idem_key)=COALESCE(o.thread_ts,o.idem_key)
   AND e.state IN ('pending','sending')) ORDER BY o.id LIMIT 1";
const DEPENDENTS: &str = "WITH RECURSIVE dependents(key) AS (
 SELECT idem_key FROM outbox WHERE id=? UNION
 SELECT o.idem_key FROM outbox o JOIN dependents d ON o.after=d.key)";

fn post(row: &Row<'_>) -> rusqlite::Result<ClaimedPost> {
    let meta: Option<String> = row.get("meta_json")?;
    let meta = meta
        .map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;
    Ok(ClaimedPost {
        id: row.get("id")?,
        attempt: row.get("attempts")?,
        post: Post {
            idem_key: row.get("idem_key")?,
            session_id: row.get("session_id")?,
            kind: row.get("kind")?,
            channel: row.get("channel")?,
            thread_ts: row.get("thread_ts")?,
            text: row.get("text")?,
            meta,
            filename: row.get("filename")?,
            blob: row.get("blob")?,
            after: row.get("after")?,
        },
    })
}

pub fn enqueue_tx(c: &Connection, item: &Post, now: f64) -> Result<i64> {
    if !now.is_finite() || item.idem_key.is_empty() || item.channel.is_empty() {
        bail!("invalid outbox item");
    }
    if !matches!(
        item.kind.as_str(),
        "reply"
            | "notice"
            | "report"
            | "debrief_root"
            | "upload"
            | "approval_notice"
            // An external driver's posts (fridica#130).
            | "study_claim"
            | "study_result"
            | "study_root"
            | "driver_report"
    ) {
        bail!("unknown outbox kind");
    }
    if let Some(existing) = c
        .query_row(
            "SELECT * FROM outbox WHERE idem_key=?",
            [&item.idem_key],
            post,
        )
        .optional()?
    {
        if existing.post != *item {
            bail!("outbox idempotency key reused with different content");
        }
        return Ok(existing.id);
    }
    let prerequisite: Option<String> = if item.after.is_empty() {
        None
    } else {
        Some(
            c.query_row(
                "SELECT state FROM outbox WHERE idem_key=?",
                [&item.after],
                |r| r.get(0),
            )
            .context("outbox prerequisite must already be queued")?,
        )
    };
    // A dependency created after its predecessor failed must also be visible as
    // blocked. Merely waiting in 'pending' would leave it stranded forever.
    let blocked =
        prerequisite.is_some_and(|s| matches!(s.as_str(), "failed" | "ambiguous" | "blocked"));
    c.execute("INSERT INTO outbox(idem_key,session_id,kind,channel,thread_ts,text,meta_json,filename,blob,after,state,error,created)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", params![item.idem_key,item.session_id,item.kind,item.channel,item.thread_ts,item.text,
        item.meta.as_ref().map(serde_json::to_string).transpose()?,item.filename,item.blob,item.after,
        if blocked {"blocked"} else {"pending"},if blocked {"waiting for a post that failed"} else {""},now])?;
    Ok(c.last_insert_rowid())
}

pub async fn enqueue(store: &Store, item: Post, now: f64) -> Result<i64> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let id = enqueue_tx(&tx, &item, now)?;
            tx.commit()?;
            Ok(id)
        })
        .await
}

pub async fn ready(store: &Store, now: f64, limit: usize) -> Result<Vec<i64>> {
    if !now.is_finite() {
        bail!("invalid delivery time");
    }
    store.call(move |c| ready_tx(c, now, limit)).await
}

pub fn ready_tx(c: &Connection, now: f64, limit: usize) -> Result<Vec<i64>> {
    if !now.is_finite() {
        bail!("invalid delivery time");
    }
    let sql = READY.replace("LIMIT 1", "LIMIT ?");
    Ok(c.prepare(&sql)?
        .query_map(params![now, limit.min(100) as i64], |r| r.get("id"))?
        .collect::<rusqlite::Result<_>>()?)
}

pub async fn claim(store: &Store, now: f64) -> Result<Option<ClaimedPost>> {
    claim_matching(store, now, None).await
}

pub async fn claim_id(store: &Store, now: f64, id: i64) -> Result<Option<ClaimedPost>> {
    claim_matching(store, now, Some(id)).await
}

async fn claim_matching(store: &Store, now: f64, id: Option<i64>) -> Result<Option<ClaimedPost>> {
    if !now.is_finite() {
        bail!("invalid delivery time");
    }
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let item = claim_tx(&tx, now, id)?;
            tx.commit()?;
            Ok(item)
        })
        .await
}

/// Claim the next ready post, or post `id` when it is ready.
pub fn claim_tx(tx: &Connection, now: f64, id: Option<i64>) -> Result<Option<ClaimedPost>> {
    if !now.is_finite() {
        bail!("invalid delivery time");
    }
    let item = if let Some(id) = id {
        let sql = READY.replace("ORDER BY o.id", "AND o.id=? ORDER BY o.id");
        tx.query_row(&sql, params![now, id], post).optional()?
    } else {
        tx.query_row(READY, [now], post).optional()?
    };
    let Some(mut item) = item else {
        return Ok(None);
    };
    tx.execute(
        "UPDATE outbox SET state='sending',attempts=attempts+1 WHERE id=?",
        [item.id],
    )?;
    item.attempt += 1;
    tx.execute(
        "INSERT INTO replay_events(kind,time,payload_json,complete) VALUES('delivery_call',?,?,0)",
        params![now, serde_json::to_string(&item)?],
    )?;
    Ok(Some(item))
}

pub fn fail_tx(c: &Connection, id: i64, state: &str, error: &str) -> Result<()> {
    c.execute(
        "UPDATE outbox SET state=?,error=? WHERE id=?",
        params![state, error, id],
    )?;
    c.execute(
        &format!(
            "{DEPENDENTS} UPDATE outbox SET state='blocked',error='waiting for a post that failed'
        WHERE state='pending' AND idem_key IN (SELECT key FROM dependents) AND id!=?"
        ),
        params![id, id],
    )?;
    if state == "failed" {
        c.execute(
            "UPDATE reply_reservations SET state='released' WHERE outbox_id=? AND state='reserved'",
            [id],
        )?;
    }
    Ok(())
}

/// A refused reply or report gives the parent one turn to rewrite it: a
/// `post_refused` inbox item naming the post and the rule it broke (fridica#119).
/// The rewrite's own refusal queues nothing more, so a text that cannot pass
/// costs one extra turn, not a loop. Uploads and notices are not retried.
fn refused_tx(c: &Connection, claim: &ClaimedPost, code: &str, now: f64) -> Result<()> {
    if !matches!(claim.post.kind.as_str(), "reply" | "report") {
        return Ok(());
    }
    let inbox: i64 = claim
        .post
        .idem_key
        .split(':')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let (class, source): (String, Option<String>) = c.query_row(
        "SELECT (SELECT trigger_class FROM outbox WHERE id=?),(SELECT kind FROM thread_inbox WHERE id=?)",
        params![claim.id, inbox],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if source.as_deref() == Some("post_refused") {
        return Ok(());
    }
    let payload = json!({"outbox_id":claim.id,"code":code,"post_kind":claim.post.kind,"turn":claim.post.meta.as_ref().map(|m|m["turn"].clone()).unwrap_or(Value::Null),"class":class});
    c.execute(
        "INSERT OR IGNORE INTO thread_inbox(session_id,kind,ref,payload_json,created,dedup_key)
         SELECT ?,'post_refused',?,?,?,? WHERE EXISTS(SELECT 1 FROM threads WHERE id=? AND control='active')",
        params![claim.post.session_id, claim.id.to_string(), payload.to_string(), now, format!("post-refused:{}", claim.id), claim.post.session_id],
    )?;
    Ok(())
}

/// Common confirmation effects, also used by the attention adapter.
pub fn confirm_tx(c: &Connection, id: i64, reference: &str, now: f64) -> Result<()> {
    if reference.is_empty() || !now.is_finite() {
        bail!("delivery needs a timestamp");
    }
    if c.execute("UPDATE outbox SET state='sent',sent_ts=?,delivered_at=?,error='' WHERE id=? AND state='sending'",params![reference,now,id])?!=1 {
        bail!("post was not being sent");
    }
    c.execute(
        "UPDATE reply_reservations SET state='sent' WHERE outbox_id=?",
        [id],
    )?;
    c.execute(
        "UPDATE obligations SET state='answered',updated=? WHERE state='awaiting_delivery'
        AND id IN (SELECT obligation_id FROM obligation_posts WHERE outbox_id=?)
        AND NOT EXISTS(SELECT 1 FROM obligation_posts p JOIN outbox o ON p.outbox_id=o.id
            WHERE p.obligation_id=obligations.id AND o.state NOT IN ('sent','failed'))",
        params![now, id],
    )?;
    Ok(())
}

fn safe_code(code: &str) -> String {
    if code.is_empty()
        || code.len() > 100
        || !code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-+".contains(&b))
    {
        "adapter_error".into()
    } else {
        code.into()
    }
}

pub async fn complete(
    store: &Store,
    claim: ClaimedPost,
    outcome: DeliveryOutcome,
    owner: String,
    now: f64,
) -> Result<bool> {
    if !now.is_finite() {
        bail!("invalid delivery time");
    }
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let done = complete_tx(&tx, &claim, &outcome, &owner, now)?;
            tx.commit()?;
            match done {
                PostOutcome::Stale => bail!("stale delivery attempt or changed payload"),
                PostOutcome::Sent => Ok(true),
                PostOutcome::Unsent => Ok(false),
            }
        })
        .await
}

/// Record a claimed delivery attempt's outcome. A stale attempt's result is
/// recorded as late, and the caller commits that before it reports the error.
pub fn complete_tx(
    tx: &Connection,
    claim: &ClaimedPost,
    outcome: &DeliveryOutcome,
    owner: &str,
    now: f64,
) -> Result<PostOutcome> {
    if !now.is_finite() {
        bail!("invalid delivery time");
    }
    {
        let current = tx
            .query_row(
                "SELECT * FROM outbox WHERE id=? AND state='sending' AND attempts=?",
                params![claim.id, claim.attempt],
                post,
            )
            .optional()?;
        let raw = serde_json::to_value(outcome)?;
        if current.as_ref().is_none_or(|item| item.post != claim.post) {
            tx.execute(
                "INSERT INTO replay_events(kind,time,payload_json) VALUES('delivery_late',?,?)",
                params![now, json!({"claim":claim,"result":raw}).to_string()],
            )?;
            return Ok(PostOutcome::Stale);
        }
        tx.execute("UPDATE replay_events SET complete=1 WHERE kind='delivery_call' AND json_extract(payload_json,'$.id')=? AND json_extract(payload_json,'$.attempt')=?",
            params![claim.id,claim.attempt])?;
        let mut sent = false;
        let record = match outcome.clone() {
            DeliveryOutcome::Sent { reference } if !reference.is_empty() => {
                confirm_tx(tx, claim.id, &reference, now)?;
                let p = &claim.post;
                if p.kind != "upload" {
                    let workspace = p
                        .session_id
                        .split(':')
                        .next()
                        .filter(|v| !v.is_empty())
                        .context("post missing workspace identity")?;
                    let root = p.thread_ts.as_ref().unwrap_or(&reference);
                    // Slack may have delivered its own echo before the API result.
                    // Update that existing history row rather than duplicating it.
                    tx.execute("INSERT INTO messages(event_id,workspace,channel,ts,root_ts,thread_ts,sender,text,source,meta_json,received_at)
                        VALUES(?,?,?,?,?,?,?,?,'self',?,?) ON CONFLICT(workspace,channel,ts) DO UPDATE SET source='self',meta_json=excluded.meta_json",
                        params![format!("self:{}:{reference}",p.channel),workspace,p.channel,reference,root,p.thread_ts,owner,p.text,
                        p.meta.as_ref().map(serde_json::to_string).transpose()?,now])?;
                }
                sent = true;
                json!({"outcome":"sent","reference":reference})
            }
            DeliveryOutcome::RateLimited { retry_after } if retry_after.is_finite() => {
                let base: u32 = tx.query_row(
                    "SELECT retry_base FROM outbox WHERE id=?",
                    [claim.id],
                    |r| r.get(0),
                )?;
                if claim.attempt.saturating_sub(base) >= 5 {
                    fail_tx(tx, claim.id, "failed", "rate limited 5 times")?;
                } else {
                    tx.execute("UPDATE outbox SET state='pending',retry_at=?,error='rate_limited' WHERE id=?",params![now+retry_after.clamp(1.,3600.),claim.id])?;
                }
                json!({"outcome":"rate_limited","retry_after":retry_after})
            }
            DeliveryOutcome::Rejected { code } => {
                let code = safe_code(&code);
                fail_tx(tx, claim.id, "failed", &code)?;
                refused_tx(tx, claim, &code, now)?;
                json!({"outcome":"rejected","code":code})
            }
            DeliveryOutcome::Ambiguous { code } => {
                let code = safe_code(&code);
                fail_tx(tx, claim.id, "ambiguous", &code)?;
                json!({"outcome":"ambiguous","code":code})
            }
            _ => {
                fail_tx(tx, claim.id, "ambiguous", "invalid_adapter_result")?;
                json!({"outcome":"ambiguous","code":"invalid_adapter_result"})
            }
        };
        tx.execute("INSERT INTO replay_events(kind,time,payload_json) VALUES('delivery',?,?)",
            params![now,json!({"outbox_id":claim.id,"attempt":claim.attempt,"result":record,"raw_result":raw}).to_string()])?;
        Ok(if sent {
            PostOutcome::Sent
        } else {
            PostOutcome::Unsent
        })
    }
}

/// Call once before starting delivery tasks, while holding the store's daemon lock.
pub async fn recover(store: &Store, now: f64) -> Result<usize> {
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let count = recover_tx(&tx, now)?;
            tx.commit()?;
            Ok(count)
        })
        .await
}

pub fn recover_tx(tx: &Connection, now: f64) -> Result<usize> {
    {
        let ids: Vec<i64> = tx
            .prepare("SELECT id FROM outbox WHERE state='sending' ORDER BY id")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for id in &ids {
            fail_tx(tx, *id, "ambiguous", "daemon_stopped_during_send")?;
            tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','outbox.ambiguous',?,'{}')",params![now,id.to_string()])?;
        }
        Ok(ids.len())
    }
}

pub async fn requeue(store: &Store, id: i64, actor: Authority, now: f64) -> Result<bool> {
    if actor != Authority::Owner {
        bail!("only the owner may retry an uncertain or failed post");
    }
    store
        .call(move |c| {
            let tx = c.transaction()?;
            let retried = requeue_tx(&tx, id, actor, now)?;
            tx.commit()?;
            Ok(retried)
        })
        .await
}

pub fn requeue_tx(tx: &Connection, id: i64, actor: Authority, now: f64) -> Result<bool> {
    if actor != Authority::Owner {
        bail!("only the owner may retry an uncertain or failed post");
    }
    {
        if tx.execute("UPDATE outbox SET state='pending',retry_at=0,retry_base=attempts,error='' WHERE id=? AND state IN ('failed','ambiguous')",[id])?==0 {return Ok(false);}
        // Attempt numbers never reset: they fence delayed completions.
        tx.execute(&format!("{DEPENDENTS} UPDATE outbox SET state='pending',error='' WHERE state='blocked' AND idem_key IN (SELECT key FROM dependents)"),[id])?;
        tx.execute("UPDATE reply_reservations SET state='reserved',reserved_at=? WHERE outbox_id=? AND state='released'",params![now,id])?;
        tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'owner','outbox.retry',?,'{}')",params![now,id.to_string()])?;
        Ok(true)
    }
}
