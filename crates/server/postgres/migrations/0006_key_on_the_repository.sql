-- One customer per install, and a repository carries its own name.
--
-- `0004` and `0005` built a ledger of two things: whose a repository is, and
-- what they call it. The first is gone, there being one answer to it now. The
-- second moves onto the repository, where one write does what two did — and
-- where `0004` deliberately would not put it, since keeping the ledger clear
-- of the engine's tables was how one tenant's rows stayed unjoinable from
-- another's.
--
-- Nothing downstream is keyed by the name. Objects are addressed by
-- `storage_key` and every other table by `repo_id`, both minted per row, so a
-- key that comes back into use is a different repository with storage of its
-- own.

ALTER TABLE repositories ADD COLUMN IF NOT EXISTS external_key text;

-- Refused rather than resolved when two repositories that are both still here
-- would end up with one name: which of them keeps it is the operator's to
-- decide, and guessing would take a repository away from whoever did not win.
--
-- Only the live ones can collide, the index below covering nothing else. So a
-- name shared with a deleted repository is no collision, and neither is a
-- ledger row naming a repository that was reclaimed out from under it.
DO $$
DECLARE clashing text;
BEGIN
    SELECT string_agg(repo_key, ', ') INTO clashing
      FROM (SELECT ledger.repo_key
              FROM tenant_repositories ledger
              JOIN repositories held ON held.id = ledger.repo_id
             WHERE held.deleted_at IS NULL
             GROUP BY ledger.repo_key
            HAVING count(*) > 1
             ORDER BY ledger.repo_key
             LIMIT 10) AS shared;
    IF clashing IS NOT NULL THEN
        RAISE EXCEPTION
            'more than one repository is called %. Give each of them a name '
            'of its own before this install serves one application.',
            clashing;
    END IF;
END $$;

UPDATE repositories held
   SET external_key = ledger.repo_key
  FROM tenant_repositories ledger
 WHERE ledger.repo_id = held.id
   AND held.external_key IS NULL;

-- A repository nothing ever named is one no application could reach. Its id is
-- what it was addressed by before keys existed, and is already a key's shape.
-- A deployment that somehow holds both a repository named `7` and an unnamed
-- repository 7 stops at the index below, which is the same place it would stop
-- for any other pair that cannot both keep a name.
UPDATE repositories SET external_key = id::text WHERE external_key IS NULL;

ALTER TABLE repositories ALTER COLUMN external_key SET NOT NULL;

-- A name belongs to one *live* repository, which is the whole of the
-- constraint. Partial because `deleted_at` is a flag and the row outlives the
-- delete by `deleted_grace_secs`: the predicate is what hands the name back the
-- moment the repository is deleted, rather than when maintenance gets to it.
CREATE UNIQUE INDEX IF NOT EXISTS repositories_by_external_key
    ON repositories (external_key)
 WHERE deleted_at IS NULL;

DROP TABLE IF EXISTS tenant_repositories;
