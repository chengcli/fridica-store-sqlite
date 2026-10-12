-- The archived_threads table reached migration 010 after some databases had
-- already applied it (fridica#115 appended it to a shipped file), so those
-- databases report schema v11 without the table: every message intake notes
-- `archive_revive_failed` and the weekly archive round never runs (#1).
-- Databases created after that already have it; IF NOT EXISTS keeps them.
CREATE TABLE IF NOT EXISTS archived_threads (
    session_id TEXT PRIMARY KEY, archive TEXT NOT NULL, last_activity REAL NOT NULL,
    archived_at REAL NOT NULL, restored_at REAL
);
-- The noise those databases accumulated meanwhile: one health event per
-- message and one per hourly archive round, all with the same error.
DELETE FROM health_events
 WHERE kind IN ('archive_revive_failed', 'archive_failed')
   AND details_json LIKE '%no such table: archived_threads%';
