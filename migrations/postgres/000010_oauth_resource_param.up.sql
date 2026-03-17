-- RFC 8707: Add resource parameter support to OAuth authorization codes.
-- The resource column is nullable; existing codes without a resource are valid.
ALTER TABLE oauth_authorization_codes ADD COLUMN resource TEXT;
