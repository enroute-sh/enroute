-- The object index's two catalogs: where a tree's entries are, and where a
-- blob is stored. Split from the commit graph's because they are written at
-- different times — a commit's pack facts land once, while an object gains
-- packs later.
--
-- The same shape as `0002`, for the same reasons: one table per segmented
-- value, and what `Table` creates rather than anything decided here.

CREATE TABLE IF NOT EXISTS tree_segments (
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
CREATE INDEX IF NOT EXISTS tree_segments_covering ON tree_segments (scope, first_key, last_key);
CREATE TABLE IF NOT EXISTS blob_segments (
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
CREATE INDEX IF NOT EXISTS blob_segments_covering ON blob_segments (scope, first_key, last_key);
