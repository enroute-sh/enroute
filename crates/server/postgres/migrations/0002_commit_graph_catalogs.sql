-- The commit graph's two catalogs: the graph itself, and the pack bitmaps and
-- locations beside it. Two segmented values rather than two sections of one,
-- because a walk that only traverses must not pay to read the bitmaps.
--
-- One table per value rather than one with a kind column, so a write cannot
-- land in the wrong list: a catalog row carries nothing that would say which
-- list it belonged to.
--
-- The shape is the substrate's rather than this file's — it is what `Table`
-- creates and what its statements read, which a test pins. A row is one
-- segment: `scope` is the repository, `first_key`/`last_key` the range it
-- covers, `tier` how far it has been merged, and its bytes are inlined or in
-- the bucket, never both and never neither. The covering index is what
-- answers a read for a range.

CREATE TABLE IF NOT EXISTS commit_graph_segments (
    scope      bigint   NOT NULL,
    id         uuid     NOT NULL,
    first_key  bigint   NOT NULL,
    last_key   bigint   NOT NULL,
    tier       smallint NOT NULL,
    bytes      bigint   NOT NULL,
    inline     bytea,
    object_key text,
    PRIMARY KEY (scope, id),
    CHECK (first_key >= 0 AND last_key >= first_key),
    CHECK (tier BETWEEN 0 AND 255),
    CHECK (bytes >= 0),
    CHECK ((inline IS NULL) <> (object_key IS NULL))
);
CREATE INDEX IF NOT EXISTS commit_graph_segments_covering ON commit_graph_segments (scope, first_key, last_key);
CREATE TABLE IF NOT EXISTS commit_pack_segments (
    scope      bigint   NOT NULL,
    id         uuid     NOT NULL,
    first_key  bigint   NOT NULL,
    last_key   bigint   NOT NULL,
    tier       smallint NOT NULL,
    bytes      bigint   NOT NULL,
    inline     bytea,
    object_key text,
    PRIMARY KEY (scope, id),
    CHECK (first_key >= 0 AND last_key >= first_key),
    CHECK (tier BETWEEN 0 AND 255),
    CHECK (bytes >= 0),
    CHECK ((inline IS NULL) <> (object_key IS NULL))
);
CREATE INDEX IF NOT EXISTS commit_pack_segments_covering ON commit_pack_segments (scope, first_key, last_key);
