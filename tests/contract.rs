//! The storage contract (`fridica_core::store`) as fridica-store-sqlite keeps it.
//! Stage 3 of fridica#117 turns these into a suite any backend can run.
use fridica_core::store::{transact, ChannelActivity, Report, Store as _};
use fridica_store_sqlite::Store;

async fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.sqlite3")).await.unwrap();
    (dir, store)
}

#[tokio::test]
async fn the_ledger_appends_in_order_and_completes_calls() {
    let (_dir, store) = store().await;
    let (call, result) = store
        .transact(|u| {
            assert_eq!(u.last_seq()?, 0);
            let call = u.record("x_call", 1.0, r#"{"b":1,"a":2}"#, false)?;
            let result = u.record("x_result", 2.0, "{}", true)?;
            u.complete(call, true)?;
            Ok((call, result))
        })
        .await
        .unwrap();
    assert!(result > call);
    let events = store
        .transact(move |u| u.events_after(call - 1, 10))
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    // Payloads are kept byte for byte.
    assert_eq!(
        (
            events[0].kind.as_str(),
            events[0].payload.as_str(),
            events[0].complete
        ),
        ("x_call", r#"{"b":1,"a":2}"#, true)
    );
    assert_eq!(store.transact(|u| u.last_seq()).await.unwrap(), result);
    assert_eq!(
        store
            .transact(move |u| u.events_after(result, 10))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        store
            .transact(move |u| u.events_after(0, 1))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_failed_unit_of_work_leaves_nothing_behind() {
    let (_dir, store) = store().await;
    let error = store
        .transact(|u| -> anyhow::Result<()> {
            u.record("x", 1.0, "{}", true)?;
            u.note("y", "{}", 1.0)?;
            anyhow::bail!("refused")
        })
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "refused");
    let dynamic: &dyn fridica_core::store::Store = &store;
    let (seq, health) = transact(dynamic, |u| Ok((u.last_seq()?, u.count_between(0.0, 9.0)?)))
        .await
        .unwrap();
    assert_eq!((seq, health), (0, 0));
}

#[tokio::test]
async fn health_events_are_deduplicated_by_time_or_by_detail() {
    let (_dir, store) = store().await;
    let recorded = store
        .transact(|u| {
            Ok([
                u.note_unless_since("deny", "{}", 100.0, 0.0)?,
                u.note_unless_since("deny", "{}", 200.0, 50.0)?,
                u.note_unless_since("deny", "{}", 300.0, 150.0)?,
                u.note_unless_noted(
                    "skip",
                    r#"{"channel":"C","root":"1"}"#,
                    1.0,
                    &["channel", "root"],
                )?,
                u.note_unless_noted(
                    "skip",
                    r#"{"channel":"C","root":"1","code":"x"}"#,
                    2.0,
                    &["channel", "root"],
                )?,
                u.note_unless_noted(
                    "skip",
                    r#"{"channel":"C","root":"2"}"#,
                    3.0,
                    &["channel", "root"],
                )?,
                // A missing field never matches, as in SQL.
                u.note_unless_noted("drop", "{}", 4.0, &["event_id"])?,
                u.note_unless_noted("drop", "{}", 5.0, &["event_id"])?,
            ])
        })
        .await
        .unwrap();
    assert_eq!(recorded, [true, false, true, true, false, true, true, true]);
    assert_eq!(
        store
            .transact(|u| u.count_between(100.0, 300.0))
            .await
            .unwrap(),
        1
    );
    assert!(store
        .transact(|u| u.note_unless_noted("x", "{}", 1.0, &["a'b"]))
        .await
        .is_err());
}

#[tokio::test]
async fn an_empty_window_counts_no_activity() {
    let (_dir, store) = store().await;
    let (activity, campaign) = store
        .transact(|u| {
            Ok((
                u.channel_activity("C", 0.0, 9.0)?,
                u.campaign_items_updated(0.0, 9.0)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(activity, ChannelActivity::default());
    assert_eq!(campaign, 0);
}

fn report(day: &str, markdown: &str, created: f64) -> Report {
    Report {
        channel: "C".into(),
        day: day.into(),
        timezone: "UTC".into(),
        data: r#"{"b":1,"a":2}"#.into(),
        markdown: markdown.into(),
        created,
    }
}

#[tokio::test]
async fn a_kept_report_is_exported_once_per_generation() {
    let (_dir, store) = store().await;
    let pending = store
        .transact(|u| {
            u.keep_report(&report("2026-09-27", "second day", 1.0))?;
            u.keep_report(&report("2026-09-26", "old", 1.0))?;
            u.keep_report(&report("2026-09-26", "first day", 2.0))?;
            u.pending_exports()
        })
        .await
        .unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|e| (e.day.as_str(), e.markdown.as_str()))
            .collect::<Vec<_>>(),
        [("2026-09-26", "first day"), ("2026-09-27", "second day")]
    );
    let first = pending[0].generation;
    assert!(first > pending[1].generation);
    let marked = store
        .transact(move |u| {
            assert_eq!(u.pending_export("C", "2026-09-26")?, Some(first));
            Ok((
                u.mark_exported("C", "2026-09-26", first - 1)?,
                u.mark_exported("C", "2026-09-26", first)?,
                u.pending_export("C", "2026-09-26")?,
                u.pending_export("C", "2026-01-01")?,
                u.pending_exports()?.len(),
            ))
        })
        .await
        .unwrap();
    assert_eq!(marked, (0, 1, None, None, 1));
    // Keeping it again queues a new generation.
    let again = store
        .transact(|u| {
            u.keep_report(&report("2026-09-26", "again", 3.0))?;
            u.pending_export("C", "2026-09-26")
        })
        .await
        .unwrap();
    assert_eq!(again, Some(first + 1));
}

#[tokio::test]
async fn a_report_is_queued_for_posting_once() {
    let (_dir, store) = store().await;
    assert!(store
        .transact(|u| u.queue_report_post("C", "2026-09-26", "daily:C:2026-09-26", "S", 1.0))
        .await
        .is_err());
    let queued = store
        .transact(|u| {
            u.keep_report(&report("2026-09-26", "day", 1.0))?;
            Ok([
                u.queue_report_post("C", "2026-09-26", "daily:C:2026-09-26", "S", 1.0)?,
                u.queue_report_post("C", "2026-09-26", "daily:C:2026-09-26", "S", 2.0)?,
            ])
        })
        .await
        .unwrap();
    assert_eq!(queued, [true, false]);
}
