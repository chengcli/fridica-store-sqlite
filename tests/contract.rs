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
#[tokio::test]
async fn views_read_threads_messages_and_files() {
    use fridica_core::store::{Cell, Row};
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,control,created,updated) VALUES
                    ('T:C:1','T','C','1','active',1,5),('T:C:2','T','C','2','paused',1,7.5);
                 INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at,attachments_json) VALUES
                    ('e1','T','C','1','1','U','one','slack',1,'[{\"id\":\"F1\"}]'),
                    ('e2','T','C','2.5','1','U','two','slack',2,'[]'),
                    ('e3','T','C','10','1','U','three','slack',3,'[{\"id\":\"F12\"}]');",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (all, paused, attention, one, missing, messages, status) = store
        .transact(|u| {
            Ok((
                u.threads(&[], 10)?,
                u.threads(&["paused".into()], 10)?,
                u.threads_needing_attention()?,
                u.thread("T:C:1")?,
                u.thread("T:C:9")?,
                u.thread_messages("T:C:1", 2)?,
                u.status()?,
            ))
        })
        .await
        .unwrap();
    let id = |row: &Row| row.0[0].1.clone();
    assert_eq!(
        all.iter().map(id).collect::<Vec<_>>(),
        [Cell::Text("T:C:2".into()), Cell::Text("T:C:1".into())]
    );
    assert_eq!(paused.len(), 1);
    assert_eq!(attention, paused);
    // Columns keep their names, order and stored types; JSON stays text.
    let one = one.unwrap();
    assert_eq!(one.0[0].0, "id");
    let column = |row: &Row, name: &str| row.0.iter().find(|(n, _)| n == name).unwrap().1.clone();
    assert_eq!(column(&one, "turns"), Cell::Integer(0));
    assert_eq!(column(&one, "updated"), Cell::Real(5.0));
    assert_eq!(column(&one, "decisions_json"), Cell::Text("[]".into()));
    assert_eq!(column(&one, "control_detail_json"), Cell::Text("{}".into()));
    assert_eq!(missing, None);
    // The last messages, oldest first.
    assert_eq!(
        messages
            .iter()
            .map(|m| column(m, "text"))
            .collect::<Vec<_>>(),
        [Cell::Text("two".into()), Cell::Text("three".into())]
    );
    assert_eq!(column(&messages[0], "meta_json"), Cell::Null);
    assert_eq!(status.runtime, None);
    assert_eq!(status.pending_approvals, 0);
    let (files, unknown, mentioning, latest, exists, approval) = store
        .transact(|u| {
            Ok((
                u.thread_files("T:C:1")?,
                u.thread_files("T:C:9")?,
                u.attachments_mentioning("F1")?,
                u.latest_thread_in("T", "C")?,
                (u.thread_exists("T:C:1")?, u.thread_exists("T:C:9")?),
                u.approval_exists("A1")?,
            ))
        })
        .await
        .unwrap();
    let files = files.unwrap();
    assert_eq!(
        files.iter().map(|f| f.ts.as_str()).collect::<Vec<_>>(),
        ["1", "2.5", "10"]
    );
    assert_eq!(files[0].attachments, r#"[{"id":"F1"}]"#);
    assert_eq!(unknown, None);
    // Only the exact file ID, not one it prefixes.
    assert_eq!(mentioning, [r#"[{"id":"F1"}]"#]);
    assert_eq!(latest.as_deref(), Some("T:C:2"));
    assert_eq!(exists, (true, false));
    assert!(!approval);
}

#[tokio::test]
async fn owner_notes_are_revised_and_audited() {
    let (_dir, store) = store().await;
    let revisions = store
        .transact(|u| {
            let before = u.notes_revision("T:C:1")?;
            u.write_owner_notes("T:C:1", before + 1, "U1", r#"{"b":1,"a":2}"#, 3.0)?;
            Ok((before, u.notes_revision("T:C:1")?))
        })
        .await
        .unwrap();
    assert_eq!(revisions, (0, 1));
    let (notes, activity) = store
        .transact(|u| Ok((u.thread_notes("T:C:1")?, u.activity(10)?)))
        .await
        .unwrap();
    let notes = serde_json::to_string(&notes.unwrap()).unwrap();
    assert!(notes.contains(r#"{\"b\":1,\"a\":2}"#), "{notes}");
    let activity = serde_json::to_string(&activity).unwrap();
    assert!(
        activity.contains("notes.write") && activity.contains(r#"{\"revision\":1}"#),
        "{activity}"
    );
    // A revision is written once.
    assert!(store
        .transact(|u| u.write_owner_notes("T:C:1", 1, "U1", "{}", 4.0))
        .await
        .is_err());
}
#[tokio::test]
async fn slack_names_are_kept_as_recorded() {
    let (_dir, store) = store().await;
    let names = store.transact(|u| u.slack_names()).await.unwrap();
    assert_eq!(names, fridica_core::store::RecordedNames::default());
    store
        .call(|c| {
            c.execute("INSERT INTO meta VALUES('slack_workspace_name','scix')", [])?;
            c.execute(
                "INSERT INTO meta VALUES('slack_channel_names','{\"C1\":\"room\"}')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (names, users) = store
        .transact(|u| {
            u.keep_user_names(r#"{"U2":"Bo","U1":"Ada"}"#)?;
            u.keep_user_names(r#"{"U1":"Ada"}"#)?;
            Ok((u.slack_names()?, u.user_names()?))
        })
        .await
        .unwrap();
    assert_eq!(names.workspace.as_deref(), Some("scix"));
    assert_eq!(names.channels.as_deref(), Some(r#"{"C1":"room"}"#));
    assert_eq!(names.users.as_deref(), Some(r#"{"U1":"Ada"}"#));
    assert_eq!(users, names.users);
}

#[tokio::test]
async fn the_ledger_finds_intake_senders_and_repeats() {
    let (_dir, store) = store().await;
    let (first, senders, repeat, elsewhere, later) = store
        .transact(|u| {
            let intake = |event: &str, sender: &str| {
                format!(r#"{{"message":{{"event_id":"{event}","sender":"{sender}"}}}}"#)
            };
            let first = u.record("intake", 5.0, &intake("e1", "U1"), true)?;
            u.record("other", 5.0, r#"{"message":{"sender":"U9"}}"#, true)?;
            u.record("intake", 5.0, r#"{"message":{}}"#, true)?;
            u.record("intake", 6.0, &intake("e2", "U2"), true)?;
            let again = u.record("intake", 5.0, &intake("e1", "U1"), true)?;
            Ok((
                first,
                u.intake_senders_after(0)?,
                u.has_recent_intake("e1", again, 5.0)?,
                u.has_recent_intake("e1", again, 6.0)?,
                u.has_recent_intake("e1", first, 5.0)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(senders, ["U1", "U2", "U1"]);
    assert!(repeat && !elsewhere && !later);
    let after = store
        .transact(move |u| u.intake_senders_after(first))
        .await
        .unwrap();
    assert_eq!(after, ["U2", "U1"]);
}

#[tokio::test]
async fn the_feed_reads_posts_jobs_and_messages() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO outbox(id,idem_key,session_id,kind,channel,created) VALUES(7,'k','T:C:1.0','reply','C',1.0)", [])?;
            c.execute("INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at) VALUES('e1','T','C','1.0','1.0','U','hi','socket',2.5)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let found = store
        .transact(|u| {
            Ok((
                u.outbox_post(7)?,
                u.outbox_post(8)?,
                u.job_session("job-1")?,
                u.message_received_at("e1")?,
                u.message_received_at("e2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        found,
        (
            Some(fridica_core::store::OutboxPost {
                kind: "reply".into(),
                session: "T:C:1.0".into()
            }),
            None,
            None,
            Some(2.5),
            None
        )
    );
}

#[tokio::test]
async fn a_github_pause_keeps_the_later_end() {
    let (_dir, store) = store().await;
    let until = store
        .transact(|u| {
            let before = u.github_paused_until()?;
            u.pause_github(20.5)?;
            u.pause_github(10.0)?;
            Ok((before, u.github_paused_until()?))
        })
        .await
        .unwrap();
    assert_eq!(until, (None, Some("20.5".to_string())));
}

#[tokio::test]
async fn thread_controls_record_their_effects() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,status,control,created,updated) VALUES
                    ('T:C:1','T','C','1','blocked','active',1,5);
                 INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at) VALUES
                    ('e1','T','C','1','1','U','one','slack',1),
                    ('e2','T','C','2','1','B','reply','self',2),
                    ('e3','T','C','3','1','U','three','slack',3);
                 INSERT INTO workers(id,session_id,machine,workspace,backend,status,created,updated) VALUES
                    ('W1','T:C:1','m','/w','claude','idle',1,1);
                 INSERT INTO thread_inbox(session_id,kind,ref,state,created) VALUES
                    ('T:C:1','message','e3','pending',3),('T:C:1','message','e3','pending',3);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (state, channel, live, point) = store
        .transact(|u| {
            u.set_control("T:C:1", "paused", r#"{"b":1,"a":2}"#, "why", 6.0, false)?;
            Ok((
                u.control_state("T:C:1")?,
                u.thread_channel("T:C:1")?,
                u.has_live_workers("T:C:1")?,
                u.resume_point("T:C:1")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(state.control, "paused");
    assert_eq!(state.details, r#"{"b":1,"a":2}"#);
    assert_eq!(channel, ("T".into(), "C".into()));
    assert!(live);
    // The latest message from someone else since Fridica last posted.
    assert_eq!(point.latest, Some(("e3".into(), 3.0)));
    assert_eq!(point.newest, 3.0);
    // An unknown thread is an error.
    assert!(store.transact(|u| u.control_state("T:C:9")).await.is_err());
    assert!(store.transact(|u| u.thread_channel("T:C:9")).await.is_err());

    // Resuming reuses the first unfinished entry and finishes the others.
    let inbox = store
        .transact(|u| {
            u.restart_turns("T:C:1")?;
            u.reset_thread_at("T:C:1", 2.5)?;
            u.resume_message("T:C:1", "e3", r#"{"resumed":true}"#, 7.0)?;
            u.resume_message("T:C:1", "e1", r#"{"resumed":true}"#, 7.0)?;
            u.audit_control(7.0, r#""owner""#, "thread.resume", "T:C:1", r#""resume""#)?;
            Ok(())
        })
        .await;
    inbox.unwrap();
    type Inbox = Vec<(String, String, String)>;
    let (rows, thread): (Inbox, (String, f64, i64)) = store
        .call(|c| {
            let rows = c
                .prepare("SELECT ref,state,payload_json FROM thread_inbox ORDER BY id")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<rusqlite::Result<_>>()?;
            let thread = c.query_row(
                "SELECT status,reset_at,turns FROM threads WHERE id='T:C:1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            Ok((rows, thread))
        })
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            ("e3".into(), "pending".into(), r#"{"resumed":true}"#.into()),
            ("e3".into(), "done".into(), "{}".into()),
            ("e1".into(), "pending".into(), r#"{"resumed":true}"#.into()),
        ]
    );
    assert_eq!(thread, ("complete".into(), 2.5, 0));
    let activity = store.transact(|u| u.activity(10)).await.unwrap();
    assert!(serde_json::to_string(&activity)
        .unwrap()
        .contains("thread.resume"));

    // Owner instructions are found by client ID; cleaning wipes their text.
    let (id, found, missing) = store
        .transact(|u| {
            let id = u.queue_owner_instruction("T:C:1", "client-1", r#"{"text":"go"}"#, 8.0)?;
            Ok((
                id,
                u.owner_instruction("T:C:1", "client-1")?,
                u.owner_instruction("T:C:1", "client-2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(found, Some((id, "go".into())));
    assert_eq!(missing, None);
    let (found, text, states) = store
        .transact(|u| {
            u.wipe("T:C:1", 9.0)?;
            u.release_worker_results("T:C:1")?;
            u.unblock("T:C:1")?;
            Ok((
                u.owner_instruction("T:C:1", "client-1")?,
                u.thread_messages("T:C:1", 1)?,
                u.thread("T:C:1")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(found, Some((id, String::new())));
    assert!(serde_json::to_string(&text)
        .unwrap()
        .contains(r#"["text",{"Text":""}]"#));
    assert!(states.is_some());
    let dropped: i64 = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT count(*) FROM thread_inbox WHERE state='dropped'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(dropped, 3);
}

#[tokio::test]
async fn worker_stops_are_queued_once_per_worker() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,control,created,updated) VALUES
                    ('T:C:1','T','C','1','closed',1,5),('T:C:2','T','C','2','active',1,5);
                 INSERT INTO workers(id,session_id,machine,workspace,backend,status,created,updated) VALUES
                    ('W1','T:C:1','m','/w','claude','idle',1,1),
                    ('W2','T:C:1','m','/w','claude','stopped',1,1),
                    ('W3','T:C:2','m','/w','claude','idle',1,1);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (closed, before, pending, again, other) = store
        .transact(|u| {
            let closed = u.closed_threads_with_live_workers()?;
            let before = u.worker_stop_pending("T:C:1")?;
            u.queue_worker_stops(&closed, 2.0, false)?;
            let pending = u.pending_worker_stops()?;
            // A pending stop is not queued twice; stopped workers only on request.
            u.queue_worker_stops(&closed, 3.0, true)?;
            Ok((
                closed,
                before,
                pending,
                u.pending_worker_stops()?,
                u.worker_stop_pending("T:C:2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(closed, ["T:C:1"]);
    assert!(!before);
    assert_eq!(
        pending
            .iter()
            .map(|s| s.worker.as_str())
            .collect::<Vec<_>>(),
        ["W1"]
    );
    assert_eq!(
        again.iter().map(|s| s.worker.as_str()).collect::<Vec<_>>(),
        ["W1", "W2"]
    );
    assert!(!other);
    let (pending, after) = store
        .transact(move |u| {
            for stop in &again {
                u.complete(stop.seq, true)?;
            }
            Ok((u.worker_stop_pending("T:C:1")?, u.pending_worker_stops()?))
        })
        .await
        .unwrap();
    assert!(!pending);
    assert!(after.is_empty());
    let events = store.transact(|u| u.events_after(0, 10)).await.unwrap();
    assert_eq!(events[0].kind, "thread_worker_stop");
    assert_eq!(events[0].payload, r#"{"session":"T:C:1","worker":"W1"}"#);
}

#[tokio::test]
async fn linked_threads_are_read_with_their_state() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,status,control,summary,decisions_json,created,updated) VALUES
                    ('T:C:1','T','C','1','idle','active','',  '[]',1,1),
                    ('T:C:2','T','C','2','idle','active','two','[\"d\"]',1,4),
                    ('T:C:3','T','C','3','idle','active','',  '[]',1,3),
                    ('T:C:4','T','C','4','idle','closed','',  '[]',1,9),
                    ('T:C:5','T','C','5','idle','active','',  '[]',1,2);
                 INSERT INTO item_links(workspace,channel,item,session_id,repo,first_seen,last_seen) VALUES
                    ('T','C','#7','T:C:1','o/r',1,1),('T','C','#7','T:C:2','',1,2),
                    ('T','C','#7','T:C:4','',1,2),('T','C','#7','T:C:5','',1,100);
                 INSERT INTO thread_links(session_id,target,created) VALUES('T:C:1','T:C:3',1),('T:C:2','T:C:1',1);
                 INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at,meta_json) VALUES
                    ('e1','T','C','2','2','U','root','slack',1,NULL),
                    ('e2','T','C','3','2','U','middle','slack',1,NULL),
                    ('e3','T','C','4','2','B','last','self',1,'{}');
                 INSERT INTO obligations(id,session_id,kind,dedup_key,source_json,summary,created,due,updated) VALUES
                    ('O1','T:C:2','ask','k1','{}','answer',1,5,1);
                 INSERT INTO workers(id,session_id,machine,workspace,backend,role,created,updated) VALUES
                    ('W1','T:C:2','m','/w','claude','coder',1,1);
                 INSERT INTO jobs(id,worker_id,session_id,brief,status,queued_at,result_json) VALUES
                    ('J1','W1','T:C:2','fix it','done',1,'{\"summary\":\"fixed\"}');",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let linked = store
        .transact(|u| u.linked_threads("T:C:1", 4, 10.0))
        .await
        .unwrap();
    // Open threads only, most recently updated first; a stale shared item
    // links nothing.
    assert_eq!(
        linked.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
        ["T:C:2", "T:C:3"]
    );
    let two = &linked[0];
    assert_eq!(two.shared_items, "#7");
    assert!(!two.referenced && two.references_this);
    assert!(linked[1].referenced && !linked[1].references_this);
    assert_eq!(
        (
            two.root_ts.as_str(),
            two.summary.as_str(),
            two.decisions.as_str()
        ),
        ("2", "two", r#"["d"]"#)
    );
    assert_eq!(two.root, "root");
    assert_eq!(linked[1].root, "");
    assert_eq!(two.asks.len(), 1);
    assert_eq!(two.asks[0].summary, "answer");
    assert_eq!(two.jobs.len(), 1);
    assert_eq!(
        (two.jobs[0].role.as_str(), two.jobs[0].summary.as_str()),
        ("coder", "fixed")
    );
    assert_eq!(
        two.messages
            .iter()
            .map(|m| (m.text.as_str(), m.from_agent))
            .collect::<Vec<_>>(),
        [("middle", false), ("last", true)]
    );
    let limited = store
        .transact(|u| u.linked_threads("T:C:1", 1, 10.0))
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
}
