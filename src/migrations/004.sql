ALTER TABLE messages ADD COLUMN attachments_json TEXT NOT NULL DEFAULT '[]';
CREATE INDEX outbox_uploads ON outbox (kind, channel, thread_ts, filename, sent_ts);
