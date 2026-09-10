-- The engine's rows: which repositories exist, which segment objects hold
-- their pack images, and where their refs point.
--
-- What a repository is called and who owns it belong to the application in
-- front of this, which keeps its own schema. Every name here is unqualified,
-- so `search_path` decides where these land.
--
-- This file is history and its bytes are checksummed. A table changes by a
-- new `0005_*.sql` named in `schema.rs`.

-- A repository, which the engine knows only by `id`.
--
-- `deleted_at` is a flag rather than a delete: purging millions of objects is
-- unbounded work no request should wait on, so maintenance reclaims them and
-- drops this row later. Every lookup filters the column, so the id stops
-- resolving at once either way.
CREATE TABLE IF NOT EXISTS repositories (
    id              bigserial PRIMARY KEY,
    storage_key     uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz,
    -- What `HEAD` resolves to. A push can never set it, since the wire has no
    -- encoding for a symbolic ref, and a ref listing synthesizes the entry.
    default_branch  text NOT NULL DEFAULT 'refs/heads/main'
);

-- The bucket objects holding pack images back-to-back.
--
-- A segment is named by its ULID, so a pack location finds bytes without
-- reading this table. The row is for the sweep's anti-join: an object with no
-- row is a failed push's orphan.
CREATE TABLE IF NOT EXISTS commit_segments (
    repo_id    bigint NOT NULL REFERENCES repositories(id),
    segment_id bytea NOT NULL,
    -- Room for future image layouts, such as intra-segment deltas.
    format     smallint NOT NULL DEFAULT 1,
    -- When a gather copied this segment's images away, or NULL while it is
    -- the live copy. The row outlives that by a grace window, since a reader
    -- that composed the index earlier is still reading this object.
    retired_at timestamptz,
    PRIMARY KEY (repo_id, segment_id)
);

-- The one place oid space meets seq space, for all four kinds.
--
-- Each kind is counted in a numbering of its own, so a seq means nothing
-- without the kind beside it. Seqs are dense from zero, which bounds a
-- roaring bitmap over them to one 32-bit universe and lets an index be an
-- array rather than a search tree.
--
-- `kind` sits in the primary key so `branches` can name `(repo_id, oid,
-- kind)` and mean a commit rather than an object. It costs nothing: the btree
-- answers a lookup by `(repo_id, oid)` from its prefix.
CREATE TABLE IF NOT EXISTS object_seqs (
    repo_id bigint   NOT NULL,
    oid     bytea    NOT NULL,
    -- 1 = commit, 2 = tree, 3 = blob, 4 = tag (`enroute_git_core::kind_to_u8`).
    kind    smallint NOT NULL,
    seq     bigint   NOT NULL,
    PRIMARY KEY (repo_id, oid, kind),
    UNIQUE (repo_id, kind, seq)
);

-- The next seq each of a repository's kinds will hand out.
--
-- One short `UPDATE … RETURNING` per kind per push, held for that round trip
-- rather than the length of the push, which is what makes seqs unique and
-- dense at once. A push that aborts after allocating leaves a bounded gap.
CREATE TABLE IF NOT EXISTS repo_object_seq (
    repo_id  bigint   NOT NULL,
    kind     smallint NOT NULL,
    next_seq bigint   NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, kind)
);

-- refs/heads/*, with the tip oid stored rather than referenced by seq: an
-- advertisement reads every branch once per push, and joining out for the oid
-- cost about a third of all database time, against 12 bytes a row saved.
--
-- The foreign key holds "an oid this repository has numbered as a commit",
-- and `kind` is what keeps it saying commit rather than object. Whether the
-- commit is *recorded* is the index's answer, checked before a write lands.
--
-- `updated_at` is the only record of when anything in a repository last
-- happened, and rides on rows a push already writes. Dating a repository
-- reads its `default_branch` row alone: the newest of all refs would say
-- "just now" for everything active and tell no two repositories apart.
CREATE TABLE IF NOT EXISTS branches (
    repo_id    bigint NOT NULL REFERENCES repositories(id),
    refname    text NOT NULL,
    oid        bytea NOT NULL,
    -- 1 = commit (`enroute_git_core::kind_to_u8`), and nothing else.
    kind       smallint NOT NULL DEFAULT 1 CHECK (kind = 1),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, refname),
    FOREIGN KEY (repo_id, oid, kind) REFERENCES object_seqs (repo_id, oid, kind)
);

-- Every ref that is not a branch: tags, notes, an application's own
-- `refs/merge-requests/*`, anything else git takes as a refname.
--
-- `branches` minus the foreign key, which is the whole difference: a branch
-- must point at a commit because a fetch walk starts from one, and nothing
-- else must point at anything in particular. Which namespaces a repository
-- wants is the application's question, asked in `pre-receive`.
CREATE TABLE IF NOT EXISTS refs (
    repo_id    bigint NOT NULL REFERENCES repositories(id),
    refname    text NOT NULL,
    oid        bytea NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, refname)
);
