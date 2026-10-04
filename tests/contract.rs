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
async fn slack_identity_is_kept_as_reported() {
    let (_dir, store) = store().await;
    let names = store
        .transact(|u| {
            u.keep_identity(&fridica_core::store::SlackIdentity {
                scopes: "old".into(),
                channels: "{}".into(),
                workspace: "old".into(),
            })?;
            u.keep_identity(&fridica_core::store::SlackIdentity {
                scopes: "chat:write,files:read".into(),
                channels: r#"{"C1":"room"}"#.into(),
                workspace: "scix".into(),
            })?;
            u.slack_names()
        })
        .await
        .unwrap();
    assert_eq!(names.workspace.as_deref(), Some("scix"));
    assert_eq!(names.channels.as_deref(), Some(r#"{"C1":"room"}"#));
    let scopes: String = store
        .call(|c| {
            Ok(
                c.query_row("SELECT value FROM meta WHERE key='slack_scopes'", [], |r| {
                    r.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(scopes, "chat:write,files:read");
}

#[tokio::test]
async fn catch_up_keeps_watermarks_and_truncated_passes() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at) VALUES('e1','T','C','10.5','10.5','U','a','socket',11.0),('e2','T','C','20.5','10.5','U','b','socket',21.0),('e3','T','D','30.5','30.5','U','c','socket',31.0)", [])?;
            c.execute("INSERT INTO threads(id,workspace,channel,root_ts,created,updated) VALUES('T:C:1','T','C','1',1.0,5.0),('T:C:2','T','C','2',1.0,50.0),('T:D:3','T','D','3',1.0,60.0)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let (before, latest, earlier, none, roots, runs) = store
        .transact(|u| {
            Ok((
                u.catchup_mark("T", "C")?,
                u.latest_message_ts("T", "C", None)?,
                u.latest_message_ts("T", "C", Some(21.0))?,
                u.latest_message_ts("T", "E", None)?,
                u.recent_thread_roots("T", "C", 10.0)?,
                u.truncated_passes("T", "C")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        (before, latest, earlier, none),
        (None, Some(20.5), Some(10.5), None)
    );
    assert_eq!(roots, ["2"]);
    assert_eq!(runs, None);
    let (mark, runs) = store
        .transact(|u| {
            u.keep_watermark(&fridica_core::store::Watermark {
                workspace: "T".into(),
                channel: "C".into(),
                mark: 7.25,
                pinned: true,
                truncated_passes: 1,
            })?;
            Ok((u.catchup_mark("T", "C")?, u.truncated_passes("T", "C")?))
        })
        .await
        .unwrap();
    assert_eq!((mark, runs.as_deref()), (Some(7.25), Some("1")));
    let stored: (String, bool) = store
        .call(|c| {
            Ok((
                c.query_row("SELECT value FROM meta WHERE key='catchup:T:C'", [], |r| {
                    r.get(0)
                })?,
                c.query_row(
                    "SELECT pinned FROM channel_watermarks WHERE workspace='T' AND channel='C'",
                    [],
                    |r| r.get(0),
                )?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(stored, ("7.250000".into(), true));
}

#[tokio::test]
async fn the_socket_status_is_kept_in_meta_and_the_runtime_row() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO runtime(id,pid,started_at,heartbeat_at,slack_status,observe_only) VALUES(1,1,1.0,1.0,'',0)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let status = store
        .transact(|u| {
            let before = u.socket_status()?;
            u.keep_socket_status("connected")?;
            Ok((before, u.socket_status()?))
        })
        .await
        .unwrap();
    assert_eq!(status, (None, Some("connected".into())));
    let runtime: String = store
        .call(|c| {
            Ok(
                c.query_row("SELECT slack_status FROM runtime WHERE id=1", [], |r| {
                    r.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(runtime, "connected");
}

#[tokio::test]
async fn file_lookups_find_own_uploads_and_thread_attachments() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO outbox(idem_key,session_id,kind,channel,thread_ts,filename,sent_ts,created) VALUES('a','T:C:1.0','upload','C','1.0','plot.png','F1',1.0),('b','T:C:1.0','upload','C','1.0','table.csv','',1.0),('c','T:C:1.0','reply','C','1.0','other.txt','',1.0)", [])?;
            c.execute("INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at,attachments_json) VALUES('e1','T','C','1.0','1.0','U','a','socket',1.0,'[{\"id\":\"F9\"}]'),('e2','T','C','2.0','2.0','U','b','socket',2.0,'[]')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let (own, files, none) = store
        .transact(|u| {
            let files = [
                ("F1", "anything"),
                ("F2", "table.csv"),
                ("F3", "other.txt"),
                ("F4", "plot.png"),
            ]
            .map(|(a, b)| (a.to_string(), b.to_string()));
            Ok((
                u.own_uploads("C", "1.0", &files)?,
                u.session_attachments("T:C:1.0")?,
                u.session_attachments("T:C:9.0")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(own, ["F1", "F2"]);
    assert_eq!(files, [r#"[{"id":"F9"}]"#]);
    assert!(none.is_empty());
}

#[tokio::test]
async fn the_ledger_finds_the_latest_attachment_context() {
    let (_dir, store) = store().await;
    let found = store
        .transact(|u| {
            u.record(
                "parent_attachment_result",
                1.0,
                r#"{"key":"k","context":{"n":1}}"#,
                true,
            )?;
            u.record(
                "parent_attachment_result",
                2.0,
                r#"{"key":"k","context":{"n":2}}"#,
                true,
            )?;
            u.record(
                "parent_attachment_result",
                3.0,
                r#"{"key":"k","context":{"n":3}}"#,
                false,
            )?;
            u.record(
                "parent_attachment_result",
                4.0,
                r#"{"key":"j","context":{"n":4}}"#,
                true,
            )?;
            Ok((u.attachment_context("k")?, u.attachment_context("x")?))
        })
        .await
        .unwrap();
    assert_eq!(found, (Some(r#"{"n":2}"#.to_string()), None));
}

#[tokio::test]
async fn the_supervisor_interrupts_and_fingerprints_workers() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO threads(id,workspace,channel,root_ts,created,updated) VALUES('T:C:1','T','C','1',1.0,1.0)", [])?;
            c.execute("INSERT INTO workers(id,session_id,machine,workspace,backend,backend_session_id,created,updated) VALUES('w1','T:C:1','m','/w','claude','s1',1.0,1.0)", [])?;
            c.execute("INSERT INTO approvals(id,worker_id,job_id,session_id,kind,summary,created) VALUES('a1','w1','j1','T:C:1','tool','x',1.0),('a2','w2','j2','T:C:1','tool','y',1.0)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let fingerprints = store
        .transact(|u| {
            u.interrupt_worker("w1", 9.0)?;
            let before = u.instructions_fingerprint("w1")?;
            u.begin_instructions("w1", "f1", false)?;
            let kept = u.instructions_fingerprint("w1")?;
            u.begin_instructions("w1", "f2", true)?;
            Ok((before, kept, u.instructions_fingerprint("w1")?))
        })
        .await
        .unwrap();
    assert_eq!(fingerprints, (None, Some("f1".into()), Some("f2".into())));
    let rows = store
        .call(|c| {
            let approvals = c
                .prepare("SELECT id,status,decided_by,decided_at FROM approvals ORDER BY id")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
                .collect::<rusqlite::Result<Vec<(String, String, String, f64)>>>()?;
            let audit: (String, String, String) =
                c.query_row("SELECT actor,action,target FROM audit", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
            let session: String = c.query_row(
                "SELECT backend_session_id FROM workers WHERE id='w1'",
                [],
                |r| r.get(0),
            )?;
            Ok((approvals, audit, session))
        })
        .await
        .unwrap();
    assert_eq!(
        rows.0,
        [
            ("a1".into(), "cancelled".into(), "system".into(), 9.0),
            ("a2".into(), "pending".into(), String::new(), 0.0)
        ]
    );
    assert_eq!(
        rows.1,
        ("owner".into(), "worker.interrupt".into(), "w1".into())
    );
    assert_eq!(rows.2, "");
}
