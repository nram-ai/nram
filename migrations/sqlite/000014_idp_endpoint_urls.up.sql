ALTER TABLE oauth_idp_configs ADD COLUMN authorize_url TEXT;
ALTER TABLE oauth_idp_configs ADD COLUMN token_url TEXT;
ALTER TABLE oauth_idp_configs ADD COLUMN userinfo_url TEXT;
