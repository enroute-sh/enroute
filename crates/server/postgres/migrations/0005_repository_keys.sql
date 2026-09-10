-- What each tenant calls each of its repositories, chosen by the application.
--
-- Here and not on `repositories` because a key is unique to a tenant, not to
-- the install: a global constraint would let one customer's key collide with
-- another's, or squat it. Beside `tenant_id` it is scoped by construction, and
-- the engine's tables stay as ignorant of tenancy as `0004` left them.
--
-- Backfilled from `repo_id`, which is what a repository claimed before keys
-- existed was already addressed by.
ALTER TABLE tenant_repositories
    ADD COLUMN IF NOT EXISTS repo_key text;

UPDATE tenant_repositories SET repo_key = repo_id::text WHERE repo_key IS NULL;

ALTER TABLE tenant_repositories
    ALTER COLUMN repo_key SET NOT NULL;

-- What every call resolves through: a tenant and a key to a `repo_id`.
CREATE UNIQUE INDEX IF NOT EXISTS tenant_repositories_by_key
    ON tenant_repositories (tenant_id, repo_key);

-- Redundant once the index above exists, which leads on the same column.
DROP INDEX IF EXISTS tenant_repositories_by_tenant;
