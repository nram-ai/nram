CREATE TABLE settings (
  key         TEXT NOT NULL,
  value       JSONB NOT NULL,
  scope       TEXT NOT NULL DEFAULT 'global',
  updated_by  UUID REFERENCES users(id),
  updated_at  TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (key, scope)
);

CREATE INDEX idx_settings_scope ON settings (scope);
