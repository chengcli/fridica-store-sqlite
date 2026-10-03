CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE runtime (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    pid INTEGER NOT NULL,
    started_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    slack_status TEXT NOT NULL,
    observe_only INTEGER NOT NULL,
    control_socket TEXT NOT NULL DEFAULT '',
    config_fingerprint TEXT NOT NULL DEFAULT ''
);

CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    workspace TEXT NOT NULL,
    channel TEXT NOT NULL,
    ts TEXT NOT NULL,
    root_ts TEXT NOT NULL,
    thread_ts TEXT,
    sender TEXT NOT NULL,
    text TEXT NOT NULL,
    files_json TEXT NOT NULL DEFAULT '[]',
    source TEXT NOT NULL,
    meta_json TEXT,
    received_at REAL NOT NULL,
    verdict TEXT NOT NULL DEFAULT '',
    UNIQUE (workspace, channel, ts)
);
CREATE INDEX messages_thread ON messages (workspace, channel, root_ts, ts);

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    workspace TEXT NOT NULL,
    channel TEXT NOT NULL,
    root_ts TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    control TEXT NOT NULL DEFAULT 'active',
    pause_reason TEXT NOT NULL DEFAULT '',
    turns INTEGER NOT NULL DEFAULT 0,
    wait_streak INTEGER NOT NULL DEFAULT 0,
    no_progress INTEGER NOT NULL DEFAULT 0,
    last_reply_hash TEXT NOT NULL DEFAULT '',
    reset_at REAL NOT NULL DEFAULT 0,
    summary TEXT NOT NULL DEFAULT '',
    decisions_json TEXT NOT NULL DEFAULT '[]',
    context_json TEXT NOT NULL DEFAULT '{}',
    debriefed_turn INTEGER NOT NULL DEFAULT 0,
    last_unsolicited REAL NOT NULL DEFAULT 0,
    created REAL NOT NULL,
    updated REAL NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    UNIQUE (workspace, channel, root_ts)
);
CREATE INDEX threads_updated ON threads (updated);

CREATE TABLE cooldowns (
    workspace TEXT NOT NULL,
    channel TEXT NOT NULL,
    last_unsolicited REAL NOT NULL,
    PRIMARY KEY (workspace, channel)
);

CREATE TABLE thread_inbox (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES threads (id),
    kind TEXT NOT NULL,
    ref TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'pending',
    created REAL NOT NULL
);
CREATE INDEX thread_inbox_pending ON thread_inbox (session_id, state, id);

CREATE TABLE parent_turns (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL,
    inbox_id INTEGER NOT NULL REFERENCES thread_inbox (id),
    backend TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    call TEXT NOT NULL,
    action_json TEXT,
    prompt_chars INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    created REAL NOT NULL
);
CREATE INDEX parent_turns_inbox ON parent_turns (inbox_id);

CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES threads (id),
    machine TEXT NOT NULL,
    workspace TEXT NOT NULL,
    backend TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'general',
    ephemeral INTEGER NOT NULL DEFAULT 0,
    backend_session_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'idle',
    summary TEXT NOT NULL DEFAULT '',
    last_result_json TEXT,
    created REAL NOT NULL,
    updated REAL NOT NULL
);
CREATE INDEX workers_session ON workers (session_id);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers (id),
    session_id TEXT NOT NULL REFERENCES threads (id),
    inbox_id INTEGER,
    join_group TEXT NOT NULL DEFAULT '',
    brief TEXT NOT NULL,
    deliverable TEXT NOT NULL DEFAULT 'report',
    status TEXT NOT NULL DEFAULT 'queued',
    attempt INTEGER NOT NULL DEFAULT 0,
    reported INTEGER NOT NULL DEFAULT 0,
    result_json TEXT,
    error TEXT NOT NULL DEFAULT '',
    queued_at REAL NOT NULL,
    started_at REAL NOT NULL DEFAULT 0,
    finished_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX jobs_status ON jobs (status, queued_at);
CREATE INDEX jobs_group ON jobs (join_group);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs (id),
    session_id TEXT NOT NULL,
    machine TEXT NOT NULL,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    caption TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL DEFAULT 0,
    blob BLOB,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE approvals (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    backend_request_id TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    scope TEXT NOT NULL DEFAULT 'once',
    decided_by TEXT NOT NULL DEFAULT '',
    created REAL NOT NULL,
    decided_at REAL NOT NULL DEFAULT 0,
    expires_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX approvals_status ON approvals (status);

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY,
    idem_key TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    channel TEXT NOT NULL,
    thread_ts TEXT,
    text TEXT NOT NULL DEFAULT '',
    meta_json TEXT,
    filename TEXT NOT NULL DEFAULT '',
    blob BLOB,
    after TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_at REAL NOT NULL DEFAULT 0,
    sent_ts TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created REAL NOT NULL
);
CREATE INDEX outbox_state ON outbox (state, retry_at);

CREATE TABLE notes (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    actor TEXT NOT NULL,
    data_json TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    created REAL NOT NULL,
    UNIQUE (session_id, revision)
);

CREATE TABLE audit (
    id INTEGER PRIMARY KEY,
    time REAL NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}'
);
