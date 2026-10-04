//! The storage contract (`fridica_core::store`) as fridica-store-sqlite keeps it.
//! Stage 3 of fridica#117 turns these into a suite any backend can run.
use fridica_core::store::{transact, Store as _};
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
fn arrived(event: &str, ts: &str, text: &str) -> fridica_core::store::ArrivedMessage {
    fridica_core::store::ArrivedMessage {
        event_id: event.into(),
        workspace: "T".into(),
        channel: "C".into(),
        ts: ts.into(),
        root_ts: "1.0".into(),
        thread_ts: (ts != "1.0").then(|| "1.0".into()),
        sender: "U2".into(),
        text: text.into(),
        files: "[]".into(),
        source: "socket".into(),
        meta: Some(r#"{"b":1,"a":2}"#.into()),
        received_at: 2.0,
        attachments: "[]".into(),
        mentions_owner: text.contains("<@U1>"),
    }
}

#[tokio::test]
async fn intake_keeps_messages_once_and_claims_one_item_at_a_time() {
    let (_dir, store) = store().await;
    let (new, again, waiting, first, second, claimed, busy) = store
        .transact(|u| {
            let new = u.keep_message(&arrived("e1", "1.0", "hi"))?;
            let again = u.keep_message(&arrived("e1", "1.0", "hi"))?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 9.0)?;
            let waiting = u.thread_waiting("T:C:1.0")?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            let second = u.queue_message("T:C:1.0", "e2", 3.0)?;
            let claimed = u.claim_next("T:C:1.0", 5.0)?;
            let busy = u.claim_next("T:C:1.0", 5.0)?;
            Ok((new, again, waiting, first, second, claimed, busy))
        })
        .await
        .unwrap();
    assert!(new && !again && !waiting && second > first);
    assert_eq!(
        claimed,
        Some(fridica_core::store::InboxItem {
            id: first,
            kind: "message".into()
        })
    );
    assert_eq!(busy, None);
    let (meta, created) = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT (SELECT meta_json FROM messages WHERE event_id='e1'),(SELECT created FROM threads WHERE id='T:C:1.0')",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!((meta.as_str(), created), (r#"{"b":1,"a":2}"#, 2.0));
}

#[tokio::test]
async fn a_reserved_reply_is_answered_once_its_obligations_are_open() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO outbox(id,idem_key,session_id,kind,channel,created) VALUES(7,'k','T:C:1.0','reply','C',1.0)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let found = store
        .transact(|u| {
            assert!(u.thread_active("T:C:1.0").is_err());
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let inbox = u.queue_message("T:C:1.0", "e1", 2.0)?;
            assert!(u.thread_active("T:C:1.0")? && u.inbox_open(inbox, "T:C:1.0")?);
            assert!(!u.inbox_open(inbox, "T:C:2.0")?);
            assert_eq!(u.reservation_state(inbox)?, None);
            assert!(u.reserved_reply(inbox, "T:C:1.0").is_err());
            u.reserve_reply("r1", "T:C:1.0", inbox, "peer", 3.0)?;
            u.open_mention(&fridica_core::store::Mention {
                id: "o1".into(),
                session: "T:C:1.0".into(),
                dedup_key: "mention:T:C:1.0".into(),
                source: r#"{"event_id":"e1"}"#.into(),
                created: 2.0,
                due: 9.0,
            })?;
            let answer = fridica_core::store::QueuedAnswer {
                post: 7,
                session: "T:C:1.0".into(),
                inbox,
                trigger: "peer".into(),
                obligations: vec!["o1".into()],
                answers: r#"["o1"]"#.into(),
                time: 4.0,
            };
            let before = (
                u.reservation_state(inbox)?,
                u.reserved_reply(inbox, "T:C:1.0")?,
                u.recent_replies("T:C:1.0", 5.0)?,
                u.thread_route("T:C:1.0")?,
            );
            let answered = u.answer_queued(&answer)?;
            let reserved = u.reserved_reply(inbox, "T:C:1.0")?;
            let again = u.answer_queued(&answer)?;
            u.defer_reply("T:C:1.0", inbox, 50.0)?;
            Ok((before, answered, reserved, again, u.obligation_state("o1")?))
        })
        .await
        .unwrap();
    let ((state, reserved, recent, route), answered, after, again, obligation) = found;
    assert_eq!(state.as_deref(), Some("reserved"));
    assert_eq!(
        reserved,
        fridica_core::store::ReservedReply {
            trigger: "peer".into(),
            post: None
        }
    );
    assert_eq!(
        recent,
        [fridica_core::store::RecentReply {
            trigger: "peer".into(),
            at: 5.0
        }]
    );
    assert_eq!(
        route,
        fridica_core::store::Route {
            channel: "C".into(),
            root_ts: "1.0".into()
        }
    );
    assert_eq!((answered, after.post), (None, Some(7)));
    assert_eq!(again.as_deref(), Some("o1"));
    assert_eq!(obligation, "awaiting_delivery");
    let rows = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT (SELECT answers_json FROM outbox WHERE id=7),(SELECT count(*) FROM obligation_posts),(SELECT state||':'||not_before FROM thread_inbox),(SELECT throttled_until FROM threads)",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?, r.get::<_, f64>(3)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(rows, (r#"["o1"]"#.into(), 1, "pending:50.0".into(), 50.0));
}

#[tokio::test]
async fn obligations_are_signalled_disposed_and_queued_when_due() {
    let (_dir, store) = store().await;
    let (opened, reopened, queued, again, disposed, closed, state) = store
        .transact(|u| {
            assert!(u.obligation_state("s1").is_err());
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let opened = u.open_signal("s1", "T:C:1.0", 3.0)?;
            let reopened = u.open_signal("s1", "T:C:1.0", 4.0)?;
            let queued = u.queue_due(5.0)?;
            let again = u.queue_due(6.0)?;
            let disposal = fridica_core::store::Disposal {
                id: "s1".into(),
                state: "declined".into(),
                details: r#"{"kind":"declined","reason":"no"}"#.into(),
                due: None,
                actor: r#""owner""#.into(),
                time: 7.0,
            };
            let disposed = u.dispose(&disposal)?;
            let closed = u.dispose(&disposal)?;
            Ok((
                opened,
                reopened,
                queued,
                again,
                disposed,
                closed,
                u.obligation_state("s1")?,
            ))
        })
        .await
        .unwrap();
    assert!(opened && !reopened && disposed && !closed);
    assert_eq!((queued, again, state.as_str()), (1, 0, "declined"));
    let audit = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT count(*),max(details_json),max(actor) FROM audit WHERE action='obligation.disposition'",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        audit,
        (
            1,
            r#"{"kind":"declined","reason":"no"}"#.into(),
            r#""owner""#.into()
        )
    );
}

#[tokio::test]
async fn a_historical_review_opens_deferred_mentions_and_is_found_by_client_id() {
    let (_dir, store) = store().await;
    let (found, after, prior, missing) = store
        .transact(|u| {
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            u.keep_message(&arrived("e1", "1.0", "hi"))?;
            u.keep_message(&arrived("e2", "3.0", "<@U1> look"))?;
            u.keep_message(&arrived("e3", "9.0", "<@U1> later"))?;
            let query = fridica_core::store::MentionQuery {
                workspace: "T".into(),
                channels: r#"["C"]"#.into(),
                since: 0.0,
                until: 5.0,
                owner: "U1".into(),
                mention: "<@U1>".into(),
            };
            let found = u.historical_mentions(&query)?;
            let obligations = found
                .iter()
                .map(|m| fridica_core::store::HistoricalObligation {
                    id: format!("backfill:mention:T:C:{}", m.ts),
                    session: m.session.clone(),
                    dedup_key: format!("mention:T:C:{}", m.ts),
                    event_id: m.event_id.clone(),
                    source: "{}".into(),
                    created: m.received_at,
                    due: 20.0,
                    state: r#"{"kind":"deferred"}"#.into(),
                    updated: 10.0,
                })
                .collect();
            u.apply_backfill(&fridica_core::store::Backfill {
                obligations,
                time: 10.0,
                actor: "U1".into(),
                client_id: "client-1".into(),
                result: r#"{"count":1}"#.into(),
            })?;
            u.record(
                "obligations_backfill",
                10.0,
                r#"{"request":{"client_id":"client-1"},"result":{"count":1}}"#,
                true,
            )?;
            Ok((
                found,
                u.historical_mentions(&query)?,
                u.backfill_record("client-1")?,
                u.backfill_record("client-2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        found,
        [fridica_core::store::HistoricalMention {
            event_id: "e2".into(),
            session: "T:C:1.0".into(),
            workspace: "T".into(),
            channel: "C".into(),
            ts: "3.0".into(),
            received_at: 2.0
        }]
    );
    assert_eq!(after, []);
    assert_eq!(
        prior.as_deref(),
        Some(r#"{"request":{"client_id":"client-1"},"result":{"count":1}}"#)
    );
    assert_eq!(missing, None);
    let rows = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT (SELECT state FROM obligations),(SELECT group_concat(event_id) FROM messages WHERE mentions_owner=1),(SELECT details_json FROM audit WHERE action='obligations.backfill')",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        rows,
        ("deferred".into(), "e2,e3".into(), r#"{"count":1}"#.into())
    );
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

/// A thread `T:C:1` with worker `w1` on machine `m`, and job `j1` queued on it.
async fn thread_with_job(store: &Store) {
    store
        .call(|c| {
            c.execute("INSERT INTO threads(id,workspace,channel,root_ts,created,updated) VALUES('T:C:1','T','C','1',1.0,1.0)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    store
        .transact(|u| {
            u.add_workers(
                &serde_json::from_value::<Vec<_>>(serde_json::json!([{
                    "id":"w1","session_id":"T:C:1","machine":"m","workspace":"/w","backend":"claude"
                }]))?,
                1.0,
            )?;
            u.queue_jobs(
                &serde_json::from_value::<Vec<_>>(serde_json::json!([{
                    "id":"j1","worker_id":"w1","session_id":"T:C:1","brief":"look",
                    "fetch_repo":"r","fetch_ref":"main"
                }]))?,
                2.0,
            )
        })
        .await
        .unwrap();
}

fn machines() -> fridica_core::config::registry::Registry {
    serde_json::from_value(serde_json::json!({"default":"m","machines":[{
        "name":"m","transport":"local","workspaces":[],"backends":["claude"],
        "default_backend":"claude","policy":{},"host":"","tags":[],"resources":{},
        "max_workers":2,"max_jobs":2,"slurm":null,"description":""}]}))
    .unwrap()
}

/// Claim `j1` in slot 1.
async fn claim_j1(store: &Store) -> fridica_core::store::ClaimedJob {
    let machines = machines();
    store
        .transact(move |u| {
            u.claim_job(
                "j1",
                1,
                &machines,
                &fridica_core::config::Limits::default(),
                3.0,
            )
        })
        .await
        .unwrap()
        .unwrap()
}

fn post(key: &str, text: &str, after: &str) -> fridica_core::delivery::Post {
    fridica_core::delivery::Post {
        idem_key: key.into(),
        session_id: "T:C:1".into(),
        kind: "reply".into(),
        channel: "C".into(),
        thread_ts: Some("1".into()),
        text: text.into(),
        meta: None,
        filename: String::new(),
        blob: None,
        after: after.into(),
    }
}

#[tokio::test]
async fn the_outbox_queues_once_and_fences_delivery_attempts() {
    use fridica_core::{delivery::DeliveryOutcome, store::PostOutcome, Authority};
    let (_dir, store) = store().await;
    let (first, again, conflict, ready) = store
        .transact(|u| {
            let first = u.queue_post(&post("k1", "hi", ""), 1.0)?;
            let again = u.queue_post(&post("k1", "hi", ""), 1.0)?;
            let conflict = u
                .queue_post(&post("k1", "changed", ""), 1.0)
                .unwrap_err()
                .to_string();
            u.queue_post(&post("k2", "next", "k1"), 1.0)?;
            Ok((first, again, conflict, u.ready_posts(2.0, 10)?))
        })
        .await
        .unwrap();
    assert_eq!(first, again);
    assert_eq!(
        conflict,
        "outbox idempotency key reused with different content"
    );
    // The second post waits for the first.
    assert_eq!(ready, [first]);
    let claim = store
        .transact(|u| u.claim_post(2.0, None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((claim.id, claim.attempt), (first, 1));
    // A late result of another attempt is recorded, and reported as stale.
    let mut late = claim.clone();
    late.attempt = 9;
    let sent = DeliveryOutcome::Sent {
        reference: "5.000001".into(),
    };
    let (stale, done) = store
        .transact(move |u| {
            let stale = u.finish_delivery(&late, &sent, "U1", 3.0)?;
            Ok((stale, u.finish_delivery(&claim, &sent, "U1", 3.0)?))
        })
        .await
        .unwrap();
    assert_eq!((stale, done), (PostOutcome::Stale, PostOutcome::Sent));
    let (second, refused) = store
        .transact(|u| {
            let second = u.claim_post(4.0, None)?.unwrap();
            let refused = u.finish_delivery(
                &second,
                &DeliveryOutcome::Rejected {
                    code: "nope".into(),
                },
                "U1",
                4.0,
            )?;
            Ok((second, refused))
        })
        .await
        .unwrap();
    assert_eq!(refused, PostOutcome::Unsent);
    let (by_other, retried, recovered) = store
        .transact(move |u| {
            let by_other = u.retry_post(second.id, Authority::System, 5.0).is_err();
            let retried = u.retry_post(second.id, Authority::Owner, 5.0)?;
            u.claim_post(6.0, Some(second.id))?.unwrap();
            Ok((by_other, retried, u.recover_posts(7.0)?))
        })
        .await
        .unwrap();
    assert!(by_other && retried);
    assert_eq!(recovered, 1);
    let rows = store
        .call(|c| {
            let states = c
                .prepare("SELECT state,error FROM outbox ORDER BY id")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<Vec<(String, String)>>>()?;
            let kinds = c
                .prepare("SELECT kind FROM replay_events ORDER BY seq")?
                .query_map([], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<String>>>()?;
            let echo: String =
                c.query_row("SELECT source FROM messages WHERE ts='5.000001'", [], |r| {
                    r.get(0)
                })?;
            Ok((states, kinds, echo))
        })
        .await
        .unwrap();
    assert_eq!(
        rows.0,
        [
            ("sent".into(), String::new()),
            ("ambiguous".into(), "daemon_stopped_during_send".into())
        ]
    );
    assert_eq!(
        rows.1,
        [
            "delivery_call",
            "delivery_late",
            "delivery",
            "delivery_call",
            "delivery",
            "delivery_call"
        ]
    );
    assert_eq!(rows.2, "self");
}

#[tokio::test]
async fn a_post_being_sent_is_confirmed_once() {
    let (_dir, store) = store().await;
    let id = store
        .transact(|u| {
            let id = u.queue_post(&post("k1", "hi", ""), 1.0)?;
            u.claim_post(2.0, Some(id))?;
            u.confirm_post(id, "5.1", 3.0)?;
            Ok(id)
        })
        .await
        .unwrap();
    let again = store
        .transact(move |u| u.confirm_post(id, "5.1", 4.0))
        .await
        .unwrap_err();
    assert_eq!(again.to_string(), "post was not being sent");
}

#[tokio::test]
async fn jobs_are_claimed_progressed_and_completed() {
    use fridica_core::store::Completed;
    let (_dir, store) = store().await;
    thread_with_job(&store).await;
    let snapshot = store.transact(|u| u.work_snapshot()).await.unwrap();
    assert_eq!(
        (
            snapshot.queued.len(),
            snapshot.running.len(),
            snapshot.workers.len()
        ),
        (1, 0, 1)
    );
    let claimed = claim_j1(&store).await;
    assert_eq!(
        (
            claimed.job.status.as_str(),
            claimed.job.attempt,
            claimed.worker.slot
        ),
        ("running", 1, 1)
    );
    let (progressed, stale_progress, busy, context, missing) = store
        .transact(|u| {
            Ok((
                u.record_job_progress("j1", 1, "halfway", 4.0)?,
                u.record_job_progress("j1", 7, "old", 4.0)?,
                u.busy_by_machine()?,
                u.work_context("T:C:1")?,
                u.job_record("nope").is_err(),
            ))
        })
        .await
        .unwrap();
    assert!(progressed && !stale_progress && missing);
    assert_eq!(busy.get("m"), Some(&1));
    assert_eq!(context["progress"][0]["note"], "halfway");
    assert_eq!(context["jobs"][0]["id"], "j1");
    let completion: fridica_core::store::Completion = serde_json::from_value(serde_json::json!({
        "outcome":{"Err":{"kind":"refusal","code":"no"}},
        "artifacts":[],"interrupted":false,"stopped":false,"allow_retry":false
    }))
    .unwrap();
    let (stale, done, job, worker, files) = store
        .transact(move |u| {
            Ok((
                u.complete_job("j1", 9, &completion, 5.0)?,
                u.complete_job("j1", 1, &completion, 5.0)?,
                u.job_record("j1")?,
                u.worker_record("w1")?,
                u.delegable_files("T:C:1")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!((stale, done), (Completed::Stale, Completed::Finished));
    assert_eq!(job.status, "failed");
    assert_eq!(worker.status, "idle");
    assert!(files.is_empty());
    let (snapshot, recovered) = store
        .transact(|u| {
            u.stop_worker("w1", "owner", 6.0)?;
            Ok((u.previous_snapshot("w1", "j2")?, u.recover_jobs(7.0)?))
        })
        .await
        .unwrap();
    assert_eq!(snapshot, None);
    assert_eq!(recovered, 0);
    assert_eq!(
        store
            .transact(|u| u.worker_record("w1"))
            .await
            .unwrap()
            .status,
        "stopped"
    );
}

#[tokio::test]
async fn approvals_begin_settle_and_cancel() {
    use fridica_core::{
        store::{ApprovalStart, NewApproval, Settlement},
        worker::ApprovalDecision,
    };
    let (_dir, store) = store().await;
    thread_with_job(&store).await;
    let claimed = claim_j1(&store).await;
    let request = |id: &str, automatic| NewApproval {
        id: id.into(),
        worker: claimed.worker.clone(),
        job: claimed.job.clone(),
        request: serde_json::from_value(serde_json::json!({
            "backend_request_id":"b","kind":"tool","summary":"run it","detail":{"b":1,"a":2}
        }))
        .unwrap(),
        automatic,
        now: 4.0,
        expires_at: 100.0,
    };
    let (first, second, automatic) = (
        request("a1", None),
        request("a2", None),
        request("a3", Some(ApprovalDecision::Deny)),
    );
    let (started, denied, pending) = store
        .transact(move |u| {
            let started = u.begin_approval(&first)?;
            u.begin_approval(&second)?;
            Ok((
                started,
                u.begin_approval(&automatic)?,
                u.pending_approvals(10)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(started, ApprovalStart::Pending);
    assert_eq!(denied, ApprovalStart::Immediate(ApprovalDecision::Deny));
    assert_eq!(pending.len(), 2);
    let (accepted, repeated, cancelled, after) = store
        .transact(|u| {
            let accepted = u.settle_approval(
                "a1",
                Settlement::Decide(ApprovalDecision::Once),
                "owner",
                5.0,
            )?;
            let repeated = u.settle_approval("a1", Settlement::Cancel, "owner", 5.0)?;
            u.cancel_worker_approvals("w1", 6.0)?;
            let cancelled = u.approval("a2")?.unwrap();
            u.cancel_pending_approvals(7.0)?;
            Ok((accepted, repeated, cancelled, u.approval("a1")?.unwrap()))
        })
        .await
        .unwrap();
    assert!(accepted && !repeated);
    assert_eq!(
        (cancelled.status.as_str(), cancelled.decided_by.as_str()),
        ("cancelled", "interrupted")
    );
    assert_eq!(
        (after.status.as_str(), after.scope.as_str()),
        ("approved", "once")
    );
    assert_eq!(
        store
            .transact(|u| u.approval("nope"))
            .await
            .unwrap()
            .map(|a| a.id),
        None
    );
}

#[tokio::test]
async fn a_fetch_is_fenced_by_its_job_attempt() {
    let (_dir, store) = store().await;
    thread_with_job(&store).await;
    let claimed = claim_j1(&store).await;
    let job = claimed.job.clone();
    let (seq, accepted, again) = store
        .transact(move |u| {
            let request = serde_json::json!({"repo":"r"});
            let seq = u.begin_fetch(&job, &request, 4.0)?.unwrap();
            let result = serde_json::json!({"commit":"abc"});
            let accepted = u.finish_fetch(&job, seq, &result, 5.0)?;
            Ok((
                seq,
                accepted,
                u.finish_fetch(&job, seq, &result, 6.0).is_err(),
            ))
        })
        .await
        .unwrap();
    assert!(accepted && again);
    let events = store
        .transact(move |u| u.events_after(seq - 1, 1))
        .await
        .unwrap();
    assert_eq!(events[0].kind, "repo_fetch");
    assert!(events[0].complete);
    // A stale attempt fetches nothing.
    let mut stale = claimed.job;
    stale.attempt = 5;
    assert_eq!(
        store
            .transact(move |u| u.begin_fetch(&stale, &serde_json::json!({}), 7.0))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn worker_controls_are_queued_current_and_completed() {
    use fridica_core::parent::{ParentRequest, WorkerControl, WorkerOperation};
    let (_dir, store) = store().await;
    thread_with_job(&store).await;
    claim_j1(&store).await;
    let jobs = store
        .transact(|u| u.controlled_jobs("T:C:1"))
        .await
        .unwrap();
    let request: ParentRequest = serde_json::from_value(serde_json::json!({
        "inbox_id":4,"call":"c","session":{"id":"T:C:1","work":{"jobs":jobs}},"trigger":{},
        "history":[],"obligations":[],"previous":null,"errors":[]
    }))
    .unwrap();
    let controls = [WorkerControl {
        worker_id: "w1".into(),
        op: WorkerOperation::Interrupt,
    }];
    let (current, pending, interrupt, listed) = store
        .transact(move |u| {
            let current = u.worker_controls_current(&request, &controls)?;
            u.queue_worker_controls(&request, &controls, 5.0)?;
            Ok((
                current,
                u.worker_control_pending("T:C:1")?,
                u.interrupt_pending("j1", 1)?,
                u.pending_worker_controls()?,
            ))
        })
        .await
        .unwrap();
    assert!(current && pending && interrupt);
    assert_eq!(listed.len(), 1);
    assert_eq!(
        (
            listed[0].intent.worker.as_str(),
            listed[0].intent.job.as_deref(),
            listed[0].intent.attempt
        ),
        ("w1", Some("j1"), Some(1))
    );
    let seq = listed[0].seq;
    let (pending, recent) = store
        .transact(move |u| {
            u.complete_worker_control(seq, "interrupted", 6.0)?;
            Ok((
                u.worker_control_pending("T:C:1")?,
                u.recent_worker_controls("T:C:1")?,
            ))
        })
        .await
        .unwrap();
    assert!(!pending);
    assert_eq!(recent[0]["outcome"], "interrupted");
    assert_eq!(recent[0]["complete"], true);
}

#[tokio::test]
async fn slack_scopes_are_read_when_recorded() {
    let (_dir, store) = store().await;
    assert_eq!(store.transact(|u| u.slack_scopes()).await.unwrap(), None);
    store
        .call(|c| {
            c.execute(
                "INSERT INTO meta(key,value) VALUES('slack_scopes','files:read')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        store.transact(|u| u.slack_scopes()).await.unwrap(),
        Some("files:read".into())
    );
}

#[tokio::test]
async fn links_are_recorded_and_backfilled_once() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,created,updated) VALUES
                    ('T:C:1','T','C','1',1,1),('T:C:2','T','C','2',1,1);
                 INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at)
                    VALUES('e1','T','C','2','2','U','see snapy/x#12','socket',90);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (first, second) = store
        .transact(|u| {
            u.record_links(
                "T",
                "C",
                "T:C:1",
                "about #12 and thread 1000000002.000000 and 2",
                5.0,
            )?;
            Ok((
                u.backfill_links(100.0, 50.0)?,
                u.backfill_links(100.0, 50.0)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!((first, second), (1, 0));
    let items = store
        .call(|c| {
            Ok(
                c.prepare("SELECT session_id,item,repo FROM item_links ORDER BY session_id")?
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                    .collect::<rusqlite::Result<Vec<(String, String, String)>>>()?,
            )
        })
        .await
        .unwrap();
    assert_eq!(
        items,
        [
            ("T:C:1".into(), "#12".into(), String::new()),
            ("T:C:2".into(), "#12".into(), "x".into())
        ]
    );
}

#[tokio::test]
async fn an_archived_thread_is_found_and_revived() {
    let (dir, store) = store().await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,summary,created,updated) VALUES('T:C:1','T','C','1','old work',1,1);
                 INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at)
                    VALUES('e1','T','C','1','1','U','find the needle','socket',1);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let db = dir.path().join("state.sqlite3");
    let limits = fridica_store_sqlite::archive::Limits {
        threads_after: 10.0,
        threads: 10,
        events_after: 0.0,
        events: 0,
    };
    let round = store
        .call(move |c| fridica_store_sqlite::archive::round(c, &db, 1000.0, limits))
        .await
        .unwrap();
    assert_eq!(round.threads, 1);
    let (hits, never, revived, again) = store
        .transact(|u| {
            Ok((
                u.search_archives("NEEDLE", 5)?,
                u.revive_thread("T:C:9", 2000.0)?,
                u.revive_thread("T:C:1", 2000.0)?,
                u.revive_thread("T:C:1", 2001.0)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        (hits[0].thread.as_str(), hits[0].summary.as_str()),
        ("T:C:1", "old work")
    );
    assert_eq!(hits[0].matches, ["1 U: find the needle"]);
    assert!(!never && revived && !again);
    assert!(store.transact(|u| u.thread_exists("T:C:1")).await.unwrap());
}

#[tokio::test]
async fn a_revival_that_fails_is_noted_and_undone() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute(
                "INSERT INTO archived_threads(session_id,archive,last_activity,archived_at) VALUES('T:C:1','/nonexistent/2026-W01.sqlite3',1,2)",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    store
        .transact(|u| u.revive_or_note("T:C:1", 3.0))
        .await
        .unwrap();
    let (restored, noted) = store
        .call(|c| {
            let restored: Option<f64> = c.query_row(
                "SELECT restored_at FROM archived_threads WHERE session_id='T:C:1'",
                [],
                |r| r.get(0),
            )?;
            let noted: String = c.query_row("SELECT kind FROM health_events", [], |r| r.get(0))?;
            Ok((restored, noted))
        })
        .await
        .unwrap();
    assert_eq!(restored, None);
    assert_eq!(noted, "archive_revive_failed");
}

#[tokio::test]
async fn a_configuration_edit_is_pending_until_completed() {
    use fridica_core::store::ConfigurationIntent;
    let (_dir, store) = store().await;
    assert_eq!(
        store
            .transact(|u| u.pending_configuration_edit())
            .await
            .unwrap(),
        None
    );
    let intent = ConfigurationIntent {
        path: "/etc/fridica.toml".into(),
        before: "b".into(),
        after: "a".into(),
    };
    fridica_store_sqlite::configuration::replace(&store, intent.clone(), 1.0, || Ok(()))
        .await
        .unwrap();
    let pending = store
        .transact(|u| u.pending_configuration_edit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.intent, intent);
    let seq = pending.seq;
    let after = store
        .transact(move |u| {
            u.complete_configuration_edit(seq, true, 2.0)?;
            u.pending_configuration_edit()
        })
        .await
        .unwrap();
    assert_eq!(after, None);
    let events = store.transact(|u| u.events_after(0, 10)).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|e| (e.kind.as_str(), e.complete))
            .collect::<Vec<_>>(),
        [("configuration_edit", true), ("configuration_result", true)]
    );
    assert_eq!(
        events[1].payload,
        format!(r#"{{"call":{seq},"outcome":"applied"}}"#)
    );
}
