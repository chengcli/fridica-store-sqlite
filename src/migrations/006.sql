ALTER TABLE messages ADD COLUMN mentions_owner INTEGER NOT NULL DEFAULT 0;
CREATE INDEX messages_mentions ON messages(mentions_owner, received_at);
ALTER TABLE threads ADD COLUMN control_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE threads ADD COLUMN throttled_until REAL NOT NULL DEFAULT 0;
ALTER TABLE thread_inbox ADD COLUMN not_before REAL NOT NULL DEFAULT 0;
ALTER TABLE thread_inbox ADD COLUMN dedup_key TEXT;
CREATE UNIQUE INDEX inbox_dedup ON thread_inbox(dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX inbox_due ON thread_inbox(session_id, state, not_before, id);
ALTER TABLE outbox ADD COLUMN delivered_at REAL;
ALTER TABLE outbox ADD COLUMN trigger_event TEXT NOT NULL DEFAULT '';
ALTER TABLE outbox ADD COLUMN trigger_class TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE outbox ADD COLUMN answers_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE outbox ADD COLUMN retry_base INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN work_item_id TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN target_sha TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN target_tree TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN clearance TEXT NOT NULL DEFAULT 'worker';
ALTER TABLE jobs ADD COLUMN stale_for TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN retry_of TEXT NOT NULL DEFAULT '';
ALTER TABLE parent_turns ADD COLUMN response_json TEXT;
ALTER TABLE parent_turns ADD COLUMN context_json TEXT;

CREATE TABLE obligations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES threads(id),
    kind TEXT NOT NULL CHECK(kind IN ('mention','ask','signal')),
    dedup_key TEXT NOT NULL UNIQUE,
    source_json TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    created REAL NOT NULL,
    due REAL NOT NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN
        ('open','awaiting_delivery','answered','declined','deferred','expired','owner_closed')),
    state_json TEXT NOT NULL DEFAULT '{}',
    updated REAL NOT NULL
);
CREATE INDEX obligations_due ON obligations(state, due);
CREATE TABLE obligation_posts (
    obligation_id TEXT NOT NULL REFERENCES obligations(id),
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    PRIMARY KEY(obligation_id, outbox_id)
);
CREATE TABLE reply_reservations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES threads(id),
    inbox_id INTEGER NOT NULL UNIQUE REFERENCES thread_inbox(id),
    trigger_class TEXT NOT NULL CHECK(trigger_class IN ('owner','peer','human')),
    reserved_at REAL NOT NULL,
    outbox_id INTEGER UNIQUE REFERENCES outbox(id),
    state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','sent','released'))
);
CREATE INDEX reservations_window ON reply_reservations(session_id, state, reserved_at);
CREATE TABLE work_items (
    id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, head_sha TEXT NOT NULL,
    head_tree TEXT NOT NULL, revision INTEGER NOT NULL, data_json TEXT NOT NULL, updated REAL NOT NULL
);
CREATE TABLE campaign_mirrors (
    id TEXT PRIMARY KEY, revision INTEGER NOT NULL, data_json TEXT NOT NULL, updated REAL NOT NULL
);
CREATE TABLE campaign_evidence (
    id TEXT PRIMARY KEY, item_id TEXT NOT NULL REFERENCES work_items(id),
    head_sha TEXT NOT NULL, head_tree TEXT NOT NULL, kind TEXT NOT NULL,
    verified_by TEXT NOT NULL, verified_at REAL NOT NULL, data_json TEXT NOT NULL
);
CREATE TABLE overseer_exchanges (
    id TEXT PRIMARY KEY, direction TEXT NOT NULL, capability TEXT NOT NULL,
    payload_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
    response_json TEXT, created REAL NOT NULL, acknowledged_at REAL
);
CREATE TABLE reports (
    channel TEXT NOT NULL, day TEXT NOT NULL, timezone TEXT NOT NULL,
    data_json TEXT NOT NULL, markdown TEXT NOT NULL, created REAL NOT NULL,
    PRIMARY KEY(channel, day)
);
CREATE TABLE report_exports (
    channel TEXT NOT NULL, day TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'pending', error TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(channel, day), FOREIGN KEY(channel, day) REFERENCES reports(channel, day)
);
CREATE TABLE report_posts (
    channel TEXT NOT NULL, day TEXT NOT NULL, outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    PRIMARY KEY(channel, day)
);
CREATE TABLE health_events (
    id INTEGER PRIMARY KEY, kind TEXT NOT NULL, details_json TEXT NOT NULL, created REAL NOT NULL
);
CREATE TABLE channel_watermarks (
    workspace TEXT NOT NULL, channel TEXT NOT NULL, last_complete_pass REAL NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(workspace, channel)
);
CREATE TABLE replay_events (
    seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, time REAL NOT NULL,
    payload_json TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 1
);
