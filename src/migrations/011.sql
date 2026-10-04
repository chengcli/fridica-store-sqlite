-- The external-driver surface (fridica#130): who drives a thread's work, its
-- parent turns or a driver on the control API, and the free correlation
-- labels such a driver attaches to a job (never machine selectors).
ALTER TABLE threads ADD COLUMN driver TEXT NOT NULL DEFAULT 'parent';
ALTER TABLE jobs ADD COLUMN tags_json TEXT NOT NULL DEFAULT '[]';
