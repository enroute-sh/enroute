-- Tenancy, which is one table: which tenant a repository belongs to.
--
-- Who the tenants are is not here and is not a row. A deployment names them
-- in a list at a URI, so a tenant is reviewed, diffed and rolled back rather
-- than inserted. This table is what cannot be configuration, because a
-- repository comes into being while the process runs.
--
-- Last in the history and holding no foreign key into the engine's tables.
-- That absence is the separation, not a second `search_path`.

-- The isolation boundary, and the only thing between one customer and every
-- other one's storage: repository ids are a `bigserial`, so without this an
-- authenticated caller could walk them from 1.
--
-- `tenant_id` is the id configuration gives a tenant, never their slug: a
-- slug says which hostname reaches them and may be edited, while this says
-- the repository is theirs and may not. A row naming a tenant the
-- configuration no longer holds resolves to nobody, which is what removing
-- one is meant to do.
--
-- One row per repository, so a repository cannot be shared into a second
-- tenant by adding a row.
CREATE TABLE IF NOT EXISTS tenant_repositories (
    repo_id    bigint PRIMARY KEY,
    tenant_id  text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Every repository a tenant has, for a listing and for the usage question
-- "what is this customer storing".
CREATE INDEX IF NOT EXISTS tenant_repositories_by_tenant
    ON tenant_repositories (tenant_id);
