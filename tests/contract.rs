//! The storage contract (`fridica_core::store`) where only SQLite can check
//! it. The backend-agnostic checks are fridica-core's conformance suite, run
//! in `tests/conformance.rs`; each test here says why it needs SQL: a
//! fixture no trait writes, or a stored column no trait reads back.
use fridica_core::store::Store as _;
use fridica_store_sqlite::Store;

async fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.sqlite3")).await.unwrap();
    (dir, store)
}

// SQLite-only: How the watermark is stored: the `catchup:` meta value's format and the
// `pinned` column, which the contract never reads back.
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

// SQLite-only: The `obligation_posts` link and the deferred inbox item's `not_before`,
// which no trait reads back.
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

// SQLite-only: The messages' `mentions_owner` flag, which no trait reads back.
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
// SQLite-only: The `thread_inbox` rows a resume reuses or finishes and a clean drops,
// which no trait lists.
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
    assert_eq!(text[0].text, "");
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

// SQLite-only: Its fixture: links with chosen first- and last-seen times, a finished
// job's result and thread states that the contract only writes through long
// chains of turns and jobs.
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

// SQLite-only: The runtime row's pid, heartbeat, observe-only flag, control socket and
// configuration fingerprint, which no trait reads back.
#[tokio::test]
async fn the_runtime_row_starts_beats_and_stops() {
    use fridica_core::store::RuntimeStart;
    let (_dir, store) = store().await;
    assert_eq!(
        store.transact(|u| u.previous_slack_status()).await.unwrap(),
        None
    );
    store
        .transact(|u| {
            u.start_runtime(&RuntimeStart {
                pid: 42,
                started_at: 10.0,
                observe_only: true,
                config_fingerprint: "f1".into(),
            })?;
            u.advertise_control("/run/sock")?;
            u.heartbeat(11.0)
        })
        .await
        .unwrap();
    let row = || {
        store.call(|c| {
            Ok(c.query_row(
                "SELECT pid,started_at,heartbeat_at,slack_status,observe_only,control_socket,config_fingerprint FROM runtime WHERE id=1",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?, r.get::<_, f64>(2)?, r.get::<_, String>(3)?, r.get::<_, bool>(4)?, r.get::<_, String>(5)?, r.get::<_, String>(6)?)),
            )?)
        })
    };
    assert_eq!(
        row().await.unwrap(),
        (
            42,
            10.0,
            11.0,
            "starting".into(),
            true,
            "/run/sock".into(),
            "f1".into()
        )
    );
    assert_eq!(
        store.transact(|u| u.previous_slack_status()).await.unwrap(),
        Some("starting".into())
    );
    store.transact(|u| u.stop_runtime(12.0)).await.unwrap();
    // A restart clears the advertised endpoint.
    store
        .transact(|u| {
            u.start_runtime(&RuntimeStart {
                pid: 43,
                started_at: 20.0,
                observe_only: false,
                config_fingerprint: "f2".into(),
            })
        })
        .await
        .unwrap();
    assert_eq!(
        row().await.unwrap(),
        (
            43,
            20.0,
            20.0,
            "starting".into(),
            false,
            String::new(),
            "f2".into()
        )
    );
}

async fn neighbour_thread(store: &Store) {
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,status,control,turns,version,debriefed_turn,decisions_json,created,updated) VALUES
                    ('T:C:1','T','C','1','complete','active',3,7,1,'[\"a\"]',1,1);
                 INSERT INTO messages(event_id,workspace,channel,ts,root_ts,sender,text,source,received_at,meta_json) VALUES
                    ('e1','T','C','1','1','U1','root','socket',1,NULL),
                    ('e2','T','C','2','1','U2','peer','socket',2,'{}'),
                    ('e3','T','D','3','3','U3','other','socket',3,NULL);
                 INSERT INTO thread_inbox(id,session_id,kind,ref,payload_json,state,created,not_before) VALUES
                    (1,'T:C:1','message','e1','{}','done',1,0),
                    (2,'T:C:1','worker_result','J1','{}','processing',2,0),
                    (3,'T:C:1','message','e2','{}','pending',3,5);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

// SQLite-only: The notes' `source` column and the audit rows in id order.
#[tokio::test]
async fn thread_memory_keeps_decisions_and_parent_notes() {
    let (_dir, store) = store().await;
    neighbour_thread(&store).await;
    let (notes, decisions) = store
        .transact(|u| {
            let before = u.latest_notes("T:C:1")?;
            assert_eq!(before, None);
            u.keep_decisions("T:C:1", r#"["a","b"]"#)?;
            u.write_parent_notes("T:C:1", 1, r#"{"repo":"x"}"#, 2, 9.0)?;
            u.write_parent_notes("T:C:1", 2, r#"{"z":1,"a":2}"#, 3, 10.0)?;
            Ok((u.latest_notes("T:C:1")?, u.thread_decisions("T:C:1")?))
        })
        .await
        .unwrap();
    assert_eq!(notes, Some((2, r#"{"z":1,"a":2}"#.into())));
    assert_eq!(decisions, r#"["a","b"]"#);
    assert!(store
        .transact(|u| u.thread_decisions("T:C:9"))
        .await
        .is_err());
    let audit: Vec<(String, String, String)> = store
        .call(|c| {
            Ok(
                c.prepare("SELECT actor,action,details_json FROM audit ORDER BY id")?
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                    .collect::<rusqlite::Result<_>>()?,
            )
        })
        .await
        .unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(
        (audit[0].0.as_str(), audit[0].1.as_str()),
        ("parent", "notes.write")
    );
    let details: serde_json::Value = serde_json::from_str(&audit[1].2).unwrap();
    assert_eq!(details, serde_json::json!({"revision":2,"inbox_id":3}));
    let source: String = store
        .call(|c| {
            Ok(
                c.query_row("SELECT source FROM notes WHERE revision=2", [], |r| {
                    r.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(source, "3");
    store
        .call(|c| {
            c.execute("INSERT INTO outbox(idem_key,session_id,kind,channel,created) VALUES('2:reply','T:C:1','reply','C',1)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let queued = store
        .transact(|u| {
            Ok((
                u.post_queued("T:C:1", "2:reply")?,
                u.post_queued("T:C:1", "3:reply")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(queued, (true, false));
}

// SQLite-only: Its fixture: jobs in a join group with results and retry times, and
// artifacts with stored bytes, which no trait writes directly; and the
// `obligation_posts` links, which no trait reads back.
#[tokio::test]
async fn result_snapshots_follow_jobs_and_their_files() {
    use fridica_core::store::InboxEntry;
    let (_dir, store) = store().await;
    neighbour_thread(&store).await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO workers(id,session_id,machine,workspace,backend,role,created,updated) VALUES
                    ('W1','T:C:1','m','/w','claude','coder',1,1);
                 INSERT INTO jobs(id,worker_id,session_id,inbox_id,join_group,brief,deliverable,status,queued_at,result_json,retry_at) VALUES
                    ('J1','W1','T:C:1',1,'g','one','markdown','done',1,'{\"status\":\"done\"}',NULL),
                    ('J2','W1','T:C:1',NULL,'g','two','report','queued',2,NULL,99.5),
                    ('J3','W1','T:C:1',2,'','three','figures_pdf','done',3,NULL,NULL);
                 INSERT INTO artifacts(id,job_id,session_id,machine,path,kind,blob,status) VALUES
                    ('A2','J1','T:C:1','m','/out/b.md','file',X'02','ready'),
                    ('A1','J1','T:C:1','m','/out/a.md','file',X'01','ready'),
                    ('A3','J1','T:C:1','m','/out/c.md','file',NULL,'ready'),
                    ('A4','J2','T:C:1','m','/out/d.md','file',X'04','ready'),
                    ('A5','J3','T:C:1','m','/out/e.pdf','file',X'05','ready');",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (groups, results, single) = store
        .transact(|u| {
            Ok((
                (u.job_group("J1", "T:C:1")?, u.job_group("J1", "T:C:2")?),
                u.group_results("T:C:1", "g", "J1")?,
                u.group_results("T:C:1", "", "J3")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(groups, (Some("g".into()), None));
    assert_eq!(results.len(), 2);
    let first: serde_json::Value = serde_json::from_str(&results[0]).unwrap();
    assert_eq!(
        (&first["id"], &first["role"], &first["result"]["status"]),
        (&"J1".into(), &"coder".into(), &"done".into())
    );
    let second: serde_json::Value = serde_json::from_str(&results[1]).unwrap();
    assert_eq!(second["rate_limit_resets_at"], 99.5);
    assert!(first.get("rate_limit_resets_at").is_none());
    assert_eq!(single.len(), 1);
    let (entry, origin, inboxes) = store
        .transact(|u| {
            Ok((
                (u.inbox_entry(2, "T:C:1")?, u.inbox_entry(2, "T:C:2")?),
                (
                    u.message_origin("e1")?,
                    u.message_origin("e2")?,
                    u.message_origin("e9")?,
                ),
                (
                    u.job_inbox("J1", "T:C:1")?,
                    u.job_inbox("J2", "T:C:1")?,
                    u.job_inbox("J9", "T:C:1")?,
                ),
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        entry,
        (
            Some(InboxEntry {
                kind: "worker_result".into(),
                reference: "J1".into(),
                payload: "{}".into()
            }),
            None
        )
    );
    assert_eq!(
        origin,
        (Some(("e1".into(), false)), Some(("e2".into(), true)), None)
    );
    assert_eq!(inboxes, (Some(1), None, None));
    // Only file deliverables' ready files, job by job in the order given.
    let files = store
        .transact(|u| {
            u.deliverable_files(
                "T:C:1",
                &[
                    Some("J3".into()),
                    None,
                    Some("J2".into()),
                    Some("J1".into()),
                ],
            )
        })
        .await
        .unwrap();
    assert_eq!(
        files,
        vec![
            ("/out/e.pdf".into(), vec![5]),
            ("/out/a.md".into(), vec![1]),
            ("/out/b.md".into(), vec![2]),
        ]
    );
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO obligations(id,session_id,kind,dedup_key,source_json,created,due,updated) VALUES
                    ('O1','T:C:1','mention','k1','{}',1,5,1),('O2','T:C:1','ask','k2','{}',1,5,1);
                 INSERT INTO outbox(id,idem_key,session_id,kind,channel,created) VALUES(7,'2:upload:0','T:C:1','upload','C',1);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    store
        .transact(|u| u.link_answers(7, &["O1".into(), "O2".into()]))
        .await
        .unwrap();
    let links: i64 = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT COUNT(*) FROM obligation_posts WHERE outbox_id=7",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(links, 2);
}

// SQLite-only: Its fixture (`neighbour_thread`): inbox items with fixed ids and states and
// a sent reply, which the contract only reaches through a full delivery.
#[tokio::test]
async fn reply_evidence_reads_the_last_reply_and_the_thread() {
    use fridica_core::store::LastReply;
    let (_dir, store) = store().await;
    neighbour_thread(&store).await;
    assert_eq!(
        store.transact(|u| u.last_reply("T:C:1")).await.unwrap(),
        None
    );
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO outbox(id,idem_key,session_id,kind,channel,state,created) VALUES
                    (1,'1:reply','T:C:1','reply','C','sent',1),
                    (2,'x','T:C:1','notice','C','pending',2);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (last, messages, senders) = store
        .transact(|u| {
            Ok((
                u.last_reply("T:C:1")?,
                u.latest_messages("T:C:1")?,
                (
                    u.message_sender("e2", "T:C:1")?,
                    u.message_sender("e3", "T:C:1")?,
                ),
            ))
        })
        .await
        .unwrap();
    // The reply answered inbox item 1, message e1.
    assert_eq!(
        last,
        Some(LastReply {
            id: 1,
            state: "sent".into(),
            requester: "U1".into()
        })
    );
    assert_eq!(
        messages,
        vec![("U2".into(), "peer".into()), ("U1".into(), "root".into())]
    );
    assert_eq!(senders, (Some("U2".into()), None));
}

// SQLite-only: Its fixture (`neighbour_thread`, reservations on fixed inbox ids) and the
// inbox, reservation, `parent_turns` and outbox columns it checks, which no
// trait reads back.
#[tokio::test]
async fn debriefs_queue_post_and_settle() {
    use fridica_core::store::{DebriefOrigin, DebriefPost, DebriefTurn};
    let (_dir, store) = store().await;
    neighbour_thread(&store).await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO outbox(id,idem_key,session_id,kind,channel,trigger_class,created) VALUES
                    (1,'1:reply','T:C:1','reply','C','peer',1),
                    (2,'2:debrief','T:C:1','debrief_root','C','legacy',2);
                 INSERT INTO reply_reservations(id,session_id,inbox_id,trigger_class,reserved_at) VALUES
                    ('r2','T:C:1',2,'peer',1);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let origin = store
        .transact(|u| {
            let none = u.debrief_origin("T:C:1", "9:reply")?;
            assert_eq!(none, None);
            let origin = u.debrief_origin("T:C:1", "1:reply")?;
            u.queue_debrief("T:C:1", 1, r#"{"z":1,"a":2}"#, 4.0)?;
            Ok(origin)
        })
        .await
        .unwrap();
    assert_eq!(
        origin,
        Some(DebriefOrigin {
            version: 7,
            turn: 3,
            class: "peer".into()
        })
    );
    let queued: (String, String, String) = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT kind,ref,payload_json FROM thread_inbox WHERE id=4",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        queued,
        ("debrief".into(), "1".into(), r#"{"z":1,"a":2}"#.into())
    );
    let due = store
        .transact(|u| {
            Ok((
                u.debrief_due("T:C:1", Some(7), 3, 2)?,
                u.debrief_due("T:C:1", Some(6), 3, 2)?,
                u.debrief_due("T:C:1", Some(7), 1, 2)?,
                u.debrief_due("T:C:1", Some(7), 3, 3)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(due, (true, false, false, false));
    // As stored: an unknown version compares as NULL, which is an error.
    assert!(store
        .transact(|u| u.debrief_due("T:C:1", None, 3, 2))
        .await
        .is_err());
    assert!(store
        .transact(|u| u.debrief_due("T:C:9", Some(7), 3, 2))
        .await
        .is_err());
    let state = || {
        store.call(|c| {
            Ok(c.query_row(
                "SELECT i.state,r.state,r.outbox_id FROM thread_inbox i JOIN reply_reservations r ON r.inbox_id=i.id WHERE i.id=2",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, Option<i64>>(2)?)),
            )?)
        })
    };
    store
        .transact(|u| {
            u.debrief_stale(2)?;
            // Only an item being processed returns to pending.
            u.debrief_stale(1)
        })
        .await
        .unwrap();
    assert_eq!(
        state().await.unwrap(),
        ("pending".into(), "released".into(), None)
    );
    store
        .call(|c| {
            c.execute_batch("UPDATE thread_inbox SET state='processing' WHERE id=2; UPDATE reply_reservations SET state='reserved' WHERE id='r2';")?;
            Ok(())
        })
        .await
        .unwrap();
    store
        .transact(|u| {
            u.debrief_posted(&DebriefPost {
                session: "T:C:1".into(),
                inbox: 2,
                post: 2,
                class: "peer".into(),
                turn: 3,
                now: 8.0,
            })?;
            u.keep_debrief_turn(&DebriefTurn {
                session: "T:C:1".into(),
                inbox: 2,
                action: r#"{"debrief":"done"}"#.into(),
                response: "null".into(),
                context: r#"{"b":1,"a":2}"#.into(),
                created: 8.0,
            })?;
            u.inbox_done(2)
        })
        .await
        .unwrap();
    assert_eq!(
        state().await.unwrap(),
        ("done".into(), "reserved".into(), Some(2))
    );
    let (thread, class, turn): ((i64, f64, i64), String, (String, String, String)) = store
        .call(|c| {
            Ok((
                c.query_row(
                    "SELECT debriefed_turn,updated,version FROM threads WHERE id='T:C:1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?,
                c.query_row("SELECT trigger_class FROM outbox WHERE id=2", [], |r| {
                    r.get(0)
                })?,
                c.query_row(
                    "SELECT call,backend,context_json FROM parent_turns WHERE inbox_id=2",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(thread, (3, 8.0, 8));
    assert_eq!(class, "peer");
    assert_eq!(
        turn,
        (
            "debrief".into(),
            "adapter".into(),
            r#"{"b":1,"a":2}"#.into()
        )
    );
    store
        .call(|c| {
            c.execute(
                "UPDATE reply_reservations SET outbox_id=NULL WHERE id='r2'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    store
        .transact(|u| u.debrief_unavailable("T:C:1", 2, 9.0))
        .await
        .unwrap();
    let (reservation, audit): (String, (String, String, String)) = store
        .call(|c| {
            Ok((
                c.query_row(
                    "SELECT state FROM reply_reservations WHERE id='r2'",
                    [],
                    |r| r.get(0),
                )?,
                c.query_row("SELECT actor,action,details_json FROM audit", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(reservation, "released");
    assert_eq!(
        audit,
        (
            "system".into(),
            "debrief.unavailable".into(),
            r#"{"inbox_id":2}"#.into()
        )
    );
}

// SQLite-only: Its fixture: fixed inbox ids, a job on its second attempt and progress
// notes of older attempts, which no trait writes directly.
#[tokio::test]
async fn thread_turns_schedule_and_read_progress() {
    use fridica_core::store::{ProgressNote, ProgressState};
    let (_dir, store) = store().await;
    neighbour_thread(&store).await;
    store
        .call(|c| {
            c.execute_batch(
                "INSERT INTO threads(id,workspace,channel,root_ts,created,updated) VALUES('T:C:2','T','C','2',1,1);
                 INSERT INTO thread_inbox(id,session_id,kind,state,created,not_before) VALUES
                    (10,'T:C:2','message','pending',1,0),(11,'T:C:1','message','pending',1,0);
                 INSERT INTO workers(id,session_id,machine,workspace,backend,created,updated) VALUES
                    ('W1','T:C:1','m','/w','claude',1,1);
                 INSERT INTO jobs(id,worker_id,session_id,brief,status,attempt,queued_at) VALUES
                    ('J1','W1','T:C:1','one','running',2,1),('J2','W1','T:C:1','two','done',1,1);
                 INSERT INTO job_progress(job_id,attempt,seq,text,created) VALUES
                    ('J1',2,1,'halfway',1),('J1',1,1,'old',1),('J2',1,1,'late',1);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let ready = store
        .transact(|u| {
            Ok((
                u.ready_threads(4.0, 10)?,
                u.ready_threads(5.0, 10)?,
                u.ready_threads(5.0, 1)?,
            ))
        })
        .await
        .unwrap();
    // Item 3 of T:C:1 waits until 5; T:C:2's item 10 is older than 11.
    assert_eq!(
        ready,
        (
            vec!["T:C:2".to_owned(), "T:C:1".to_owned()],
            vec!["T:C:1".to_owned(), "T:C:2".to_owned()],
            vec!["T:C:1".to_owned()],
        )
    );
    let progress = store
        .transact(|u| {
            Ok((
                u.progress_state(2, "T:C:1")?,
                u.progress_state(3, "T:C:1")?,
                u.running_progress_note("J1", 2, 1, "T:C:1")?,
                u.running_progress_note("J1", 1, 1, "T:C:1")?,
                u.running_progress_note("J2", 1, 1, "T:C:1")?,
                u.running_progress_note("J1", 2, 1, "T:C:2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        progress,
        (
            ProgressState {
                processing: true,
                active: true
            },
            ProgressState {
                processing: false,
                active: true
            },
            Some(ProgressNote {
                text: "halfway".into(),
                worker: "W1".into()
            }),
            None,
            None,
            None,
        )
    );
    assert!(store
        .transact(|u| u.progress_state(2, "T:C:9"))
        .await
        .is_err());
    store.transact(|u| u.inbox_done(3)).await.unwrap();
    assert_eq!(
        store.transact(|u| u.ready_threads(5.0, 10)).await.unwrap(),
        vec!["T:C:2".to_owned(), "T:C:1".to_owned()]
    );
}
fn parent_turn(call: &str, error: &str, blocked: Option<&str>) -> fridica_core::store::ParentTurn {
    fridica_core::store::ParentTurn {
        call: Some(call.into()),
        response: r#"{"b":1,"a":2}"#.into(),
        context: r#"{"call":"decide"}"#.into(),
        error: error.into(),
        created: Some(3.0),
        blocked: blocked.map(str::to_owned),
    }
}

// SQLite-only: The message's recorded `verdict`, which no trait reads back.
#[tokio::test]
async fn a_turn_loads_its_thread_and_settles_only_while_current() {
    let (_dir, store) = store().await;
    let (input, roots, decisions, live, stale, held, done, attempts) = store
        .transact(|u| {
            u.keep_message(&arrived("e1", "1.0", "<@U1> hi"))?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            u.claim_next("T:C:1.0", 5.0)?;
            let input = u.turn_input("T:C:1.0", first)?;
            let roots = u.earlier_roots(Some("T"), Some("C"), Some("2.0"))?;
            let decisions = u.turn_decisions("T:C:1.0")?;
            let live = u.turn_live("T:C:1.0", Some(0), first)?;
            let version: i64 = serde_json::from_str::<serde_json::Value>(&input.thread)?["version"]
                .as_i64()
                .unwrap();
            let settle = |version, until| fridica_core::store::Settlement {
                id: first,
                session: "T:C:1.0".into(),
                version,
                event: Some("e1".into()),
                verdict: "observe: test".into(),
                until,
            };
            // A stale version only puts the item back.
            let stale = u.settle_turn(&settle(version + 1, None))?;
            u.claim_next("T:C:1.0", 5.0)?;
            let held = u.settle_turn(&settle(version, Some(40.0)))?;
            u.claim_next("T:C:1.0", 50.0)?;
            let done = u.settle_turn(&settle(version, None))?;
            Ok((
                input,
                roots,
                decisions,
                live,
                stale,
                held,
                done,
                u.inbox_attempts(first)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        (input.kind.as_str(), input.reference.as_str()),
        ("message", "e1")
    );
    assert!(input.message.unwrap().contains(r#""event_id":"e1""#));
    assert_eq!(
        (input.from_peer, input.history.len(), input.review_required),
        (None, 1, false)
    );
    assert_eq!(roots.len(), 1);
    assert_eq!(decisions.1, 0);
    assert!(live && !stale && held && done);
    assert_eq!(attempts, (0, "done".into()));
    let verdict: String = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT verdict FROM messages WHERE event_id='e1'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(verdict, "observe: test");
}

// SQLite-only: The `parent_turns` rows a failed or triaged turn keeps and the signal's
// `source_json`, which no trait reads back.
#[tokio::test]
async fn a_failed_or_rate_limited_turn_keeps_its_evidence() {
    let (_dir, store) = store().await;
    let (current, recovered, triaged) = store
        .transact(|u| {
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            u.claim_next("T:C:1.0", 5.0)?;
            u.fail_turn(&fridica_core::store::TurnFailure {
                id: first,
                session: "T:C:1.0".into(),
                state: "pending".into(),
                not_before: 35.0,
                signal: "inbox-failed:1".into(),
                source: r#"{"inbox_id":1}"#.into(),
                details: r#"{"inbox_id":1,"attempt":1}"#.into(),
                now: 5.0,
            })?;
            u.claim_next("T:C:1.0", 40.0)?;
            let current = u.retry_turn(&fridica_core::store::TurnRetry {
                id: first,
                session: "T:C:1.0".into(),
                version: Some(0),
                calls: vec![parent_turn("decide", "parent_rate_limited", None)],
                retry_at: 100.0,
                details: r#"{"retry_at":100.0}"#.into(),
                now: 40.0,
            })?;
            u.claim_next("T:C:1.0", 100.0)?;
            let recovered = u.recover_turns()?;
            u.claim_next("T:C:1.0", 100.0)?;
            let triaged = u.settle_triage(&fridica_core::store::TriageSettlement {
                id: first,
                session: "T:C:1.0".into(),
                version: Some(0),
                calls: vec![parent_turn("triage", "", None)],
                event: None,
                verdict: "ignore: triage".into(),
                now: 101.0,
            })?;
            Ok((current, recovered, triaged))
        })
        .await
        .unwrap();
    assert!(current && triaged);
    assert_eq!(recovered, 1);
    let rows = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT (SELECT group_concat(call||':'||action_json||':'||error||':'||response_json,'|') FROM parent_turns),
                    (SELECT state||':'||attempts FROM thread_inbox),
                    (SELECT source_json FROM obligations WHERE id='inbox-failed:1'),
                    (SELECT group_concat(action,',') FROM audit),
                    (SELECT version FROM threads)",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, i64>(4)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        rows,
        (
            r#"decide:{}:parent_rate_limited:{"b":1,"a":2}|triage:{}::{"b":1,"a":2}"#.into(),
            "done:1".into(),
            r#"{"inbox_id":1}"#.into(),
            "inbox.failed,parent.rate_limited".into(),
            1
        )
    );
}

// SQLite-only: The `thread_inbox` kinds and the `obligation_posts` count, which no trait
// reads back.
#[tokio::test]
async fn a_committed_turn_records_its_effects_once_fenced() {
    let (_dir, store) = store().await;
    store
        .call(|c| {
            c.execute("INSERT INTO outbox(id,idem_key,session_id,kind,channel,created) VALUES(7,'1:reply','T:C:1.0','reply','C',1.0)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let (fence, arrival, again, running, class, changed, unchanged) = store
        .transact(|u| {
            u.keep_message(&arrived("e1", "1.0", "hi"))?;
            u.keep_message(&arrived("e2", "3.0", "later"))?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            u.open_thread("T:C:2.0", "T", "C", "2.0", 2.0)?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            u.claim_next("T:C:1.0", 5.0)?;
            let fence = u.fence_turn("T:C:1.0", first)?;
            let arrival = u.arrival("T:C:1.0", first, Some(1.0), "U1")?;
            let again = u.arrival("T:C:1.0", first, Some(1.0), "U1")?;
            u.mark_unsolicited(Some("T"), Some("C"), 6.0)?;
            assert_eq!(u.last_unsolicited(Some("T"), Some("C"))?, Some(6.0));
            u.patch_context("T:C:1.0", r#"{"repo":"x"}"#)?;
            u.label_post(7, r#"{"v":2}"#, "e1", true)?;
            u.mark_reported("T:C:1.0", &[Some("j1".into()), None])?;
            let running = u.jobs_running("T:C:1.0")?;
            let class = u.handoff_class(first)?;
            let handoff = |target: &str| fridica_core::store::QueuedHandoff {
                target: target.into(),
                from: "T:C:1.0".into(),
                payload: "{}".into(),
                dedup_key: format!("handoff:{target}"),
                queued: r#"{"queued":true}"#.into(),
                skipped: r#"{"queued":false}"#.into(),
            };
            u.queue_handoffs(&[handoff("T:C:2.0"), handoff("T:C:9.0")], 6.0)?;
            u.open_asks(
                "T:C:1.0",
                &[fridica_core::store::NewAsk {
                    id: "ask:1:0".into(),
                    source: "{}".into(),
                    summary: "Answer".into(),
                    due: 50.0,
                }],
                6.0,
            )?;
            u.answer_for_handoff("T:C:1.0", &["ask:1:0".into(), "missing".into()], 7, 6.0)?;
            u.open_asks(
                "T:C:1.0",
                &[fridica_core::store::NewAsk {
                    id: "ask:1:1".into(),
                    source: "{}".into(),
                    summary: "Later".into(),
                    due: 50.0,
                }],
                6.0,
            )?;
            let change = |id: &str| fridica_core::store::ObligationChange {
                id: id.into(),
                state: "deferred".into(),
                details: r#"{"until":60.0}"#.into(),
                due: Some(60.0),
            };
            let changed = u.change_obligations("T:C:1.0", &[change("ask:1:1")], 6.0)?;
            let unchanged = u.change_obligations("T:C:1.0", &[change("ask:1:0")], 6.0)?;
            u.open_streak_signal("streak:T:C:1.0:1", "T:C:1.0", r#"{"inbox_id":1}"#, 6.0)?;
            u.open_streak_signal("streak:T:C:1.0:1", "T:C:1.0", r#"{"inbox_id":1}"#, 7.0)?;
            u.record_parent_calls(
                "T:C:1.0",
                first,
                r#"{"reply":null}"#,
                &[parent_turn(
                    "decide",
                    "parent_unavailable",
                    Some(r#"{"reason":"parent_unavailable"}"#),
                )],
                6.0,
            )?;
            u.close_turn(&fridica_core::store::TurnClose {
                session: "T:C:1.0".into(),
                status: "complete".into(),
                reply_key: "1:reply".into(),
                turn: 1,
                waiting: 0,
                quiet: 0,
                hash: "h".into(),
                summary: String::new(),
                now: 6.0,
            })?;
            u.finish_turn(first, Some("e1"), "respond: test")?;
            Ok((fence, arrival, again, running, class, changed, unchanged))
        })
        .await
        .unwrap();
    assert_eq!(
        fence,
        fridica_core::store::Fence {
            version: 0,
            active: true
        }
    );
    assert_eq!(
        arrival,
        fridica_core::store::Arrival {
            arrived: true,
            reread: false
        }
    );
    assert_eq!(
        again,
        fridica_core::store::Arrival {
            arrived: true,
            reread: true
        }
    );
    assert!(!running && changed && !unchanged);
    assert_eq!(class, "peer");
    let rows = store
        .call(|c| {
            Ok(c.query_row(
                "SELECT (SELECT kind||':'||meta_json||':'||trigger_event FROM outbox WHERE id=7),
                    (SELECT context_json||':'||status||':'||turns||':'||version||':'||last_reply_hash FROM threads WHERE id='T:C:1.0'),
                    (SELECT group_concat(session_id||'='||kind,',') FROM thread_inbox),
                    (SELECT group_concat(id||'='||state,',') FROM obligations),
                    (SELECT group_concat(details_json,',') FROM audit),
                    (SELECT count(*) FROM obligation_posts)",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, i64>(5)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        rows,
        (
            r#"report:{"v":2}:e1"#.into(),
            r#"{"repo":"x"}:complete:1:1:h"#.into(),
            "T:C:1.0=message,T:C:2.0=handoff".into(),
            "ask:1:0=awaiting_delivery,ask:1:1=deferred,streak:T:C:1.0:1=open".into(),
            r#"{"queued":true},{"queued":false},{"reason":"parent_unavailable"}"#.into(),
            1
        )
    );
}
// SQLite-only: The `item_links` rows (thread, item, repo), which no trait lists.
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

// SQLite-only: Archiving is a host operation outside the unit of work
// (`archive::round`).
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

// SQLite-only: Its fixture: an `archived_threads` row whose archive is missing, which
// only archiving writes.
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

// SQLite-only: Recording an edit is a host operation outside the unit of work
// (`configuration::replace`).
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
