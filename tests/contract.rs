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
