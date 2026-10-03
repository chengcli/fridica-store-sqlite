//! The configuration-edit journal. The file and SQLite cannot share a
//! transaction: record the fingerprints before replacement and leave the
//! request pending until runtime snapshots agree. Checking an intent against a
//! loaded configuration is the host's (fridica's `threads::configuration`).
use super::Store;
use anyhow::{bail, Result};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub path: PathBuf,
    pub before: String,
    pub after: String,
}
pub async fn pending(store: &Store) -> Result<Option<(i64, Intent)>> {
    store.call(|c| {
        let rows: Vec<(i64, String)> = c.prepare("SELECT seq,payload_json FROM replay_events WHERE kind='configuration_edit' AND complete=0 ORDER BY seq LIMIT 2")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        if rows.len() > 1 { bail!("multiple pending configuration edits"); }
        rows.into_iter().next().map(|(id, raw)| Ok((id, serde_json::from_str(&raw)?))).transpose()
    }).await
}
/// Record `intent` as the one pending edit, then run `commit` (which replaces
/// the file) on the store thread, so a cancelled caller cannot leave the file
/// replaced without its intent recorded.
pub async fn replace<F>(store: &Store, intent: Intent, now: f64, commit: F) -> Result<()>
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    if !now.is_finite() {
        bail!("invalid configuration edit time");
    }
    store.call(move |c| {
        let tx = c.transaction()?;
        let pending: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM replay_events WHERE kind='configuration_edit' AND complete=0)", [], |r| r.get(0))?;
        if pending { bail!("configuration edit awaits reconciliation"); }
        tx.execute("INSERT INTO replay_events(kind,time,payload_json,complete) VALUES('configuration_edit',?,?,0)", params![now,serde_json::to_string(&intent)?])?;
        tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'owner','configuration_edit',?,?)", params![now,intent.path.to_string_lossy(),json!({"before":intent.before,"after":intent.after}).to_string()])?;
        tx.commit()?;
        // This closure survives cancellation of its caller. Never acknowledge the
        // intent here: adapters still need rebuilding, even if rename succeeds.
        commit()
    }).await
}
pub async fn complete(store: &Store, id: i64, applied: bool, now: f64) -> Result<()> {
    store.call(move |c| {
        let tx = c.transaction()?;
        if tx.execute("UPDATE replay_events SET complete=1 WHERE seq=? AND kind='configuration_edit' AND complete=0", [id])? == 1 {
            tx.execute("UPDATE runtime SET config_fingerprint=(SELECT json_extract(payload_json,?) FROM replay_events WHERE seq=?) WHERE id=1", params![if applied {"$.after"} else {"$.before"},id])?;
            let details = json!({"call":id,"outcome":if applied {"applied"} else {"not_applied"}}).to_string();
            tx.execute("INSERT INTO replay_events(kind,time,payload_json) VALUES('configuration_result',?,?)", params![now,details])?;
            tx.execute("INSERT INTO audit(time,actor,action,target,details_json) VALUES(?,'system','configuration_reconciled',?,?)", params![now,id.to_string(),details])?;
        }
        tx.commit()?;
        Ok(())
    }).await
}
