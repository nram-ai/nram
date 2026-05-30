-- One-way: the deleted non-global settings rows cannot be reconstructed, so
-- this down direction is an explicit no-op (a rollback must not error). Mirrors
-- the 000026_users_ranking_weights_strip precedent.
SELECT 1 WHERE 0;
