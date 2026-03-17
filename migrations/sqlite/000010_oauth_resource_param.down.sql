-- SQLite does not support DROP COLUMN on older versions; recreate the table without resource.
CREATE TABLE oauth_authorization_codes_new (
  code            TEXT PRIMARY KEY,
  client_id       TEXT NOT NULL REFERENCES oauth_clients(client_id),
  user_id         TEXT NOT NULL REFERENCES users(id),
  redirect_uri    TEXT NOT NULL,
  scope           TEXT DEFAULT '',
  code_challenge  TEXT,
  code_challenge_method TEXT DEFAULT 'S256',
  expires_at      TEXT NOT NULL,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT INTO oauth_authorization_codes_new
  SELECT code, client_id, user_id, redirect_uri, scope, code_challenge, code_challenge_method, expires_at, created_at
  FROM oauth_authorization_codes;

DROP TABLE oauth_authorization_codes;
ALTER TABLE oauth_authorization_codes_new RENAME TO oauth_authorization_codes;
