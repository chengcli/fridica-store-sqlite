//! The replay ledger and health events.
use super::Sqlite;
use anyhow::Result;
use fridica_core::store::{Event, Health, Ledger};
use rusqlite::params;

impl Ledger for Sqlite<'_> {
    fn record(&mut self, kind: &str, time: f64, payload: &str, complete: bool) -> Result<i64> {
        self.0.execute(
            "INSERT INTO replay_events(kind,time,payload_json,complete) VALUES(?,?,?,?)",
            params![kind, time, payload, complete],
        )?;
        Ok(self.0.last_insert_rowid())
    }
    fn complete(&mut self, seq: i64, complete: bool) -> Result<()> {
        self.0.execute(
            "UPDATE replay_events SET complete=? WHERE seq=?",
            params![complete, seq],
        )?;
        Ok(())
    }
    fn last_seq(&mut self) -> Result<i64> {
        Ok(self
            .0
            .query_row("SELECT COALESCE(MAX(seq),0) FROM replay_events", [], |r| {
                r.get(0)
            })?)
    }
    fn events_after(&mut self, seq: i64, limit: usize) -> Result<Vec<Event>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut statement = self.0.prepare_cached(
            "SELECT seq,kind,time,payload_json,complete FROM replay_events WHERE seq>? ORDER BY seq LIMIT ?",
        )?;
        let events = statement
            .query_map(params![seq, limit], |r| {
                Ok(Event {
                    seq: r.get(0)?,
                    kind: r.get(1)?,
                    time: r.get(2)?,
                    payload: r.get(3)?,
                    complete: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(events)
    }
}

impl Health for Sqlite<'_> {
    fn note(&mut self, kind: &str, details: &str, created: f64) -> Result<i64> {
        self.0.execute(
            "INSERT INTO health_events(kind,details_json,created) VALUES(?,?,?)",
            params![kind, details, created],
        )?;
        Ok(self.0.last_insert_rowid())
    }
    fn note_unless_since(
        &mut self,
        kind: &str,
        details: &str,
        created: f64,
        since: f64,
    ) -> Result<bool> {
        Ok(self.0.execute(
            "INSERT INTO health_events(kind,details_json,created) SELECT ?1,?2,?3 WHERE NOT EXISTS(SELECT 1 FROM health_events WHERE kind=?1 AND created>?4)",
            params![kind, details, created, since],
        )? > 0)
    }
    fn note_unless_noted(
        &mut self,
        kind: &str,
        details: &str,
        created: f64,
        fields: &[&str],
    ) -> Result<bool> {
        let mut same = String::new();
        for field in fields {
            anyhow::ensure!(
                !field.is_empty() && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "health detail field must be a plain name: {field}"
            );
            same.push_str(&format!(
                " AND json_extract(details_json,'$.{field}')=json_extract(?2,'$.{field}')"
            ));
        }
        Ok(self.0.execute(
            &format!("INSERT INTO health_events(kind,details_json,created) SELECT ?1,?2,?3 WHERE NOT EXISTS(SELECT 1 FROM health_events WHERE kind=?1{same})"),
            params![kind, details, created],
        )? > 0)
    }
    fn count_between(&mut self, from: f64, to: f64) -> Result<i64> {
        Ok(self.0.query_row(
            "SELECT count(*) FROM health_events WHERE created>=? AND created<?",
            params![from, to],
            |r| r.get(0),
        )?)
    }
}
