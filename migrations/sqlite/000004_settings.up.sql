CREATE TABLE settings (
  key         TEXT NOT NULL,
  value       TEXT NOT NULL,
  scope       TEXT NOT NULL DEFAULT 'global',
  updated_by  TEXT REFERENCES users(id),
  updated_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  PRIMARY KEY (key, scope)
);

CREATE INDEX idx_settings_scope ON settings (scope);
