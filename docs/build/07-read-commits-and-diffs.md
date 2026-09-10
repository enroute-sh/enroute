# Chapter 7: Read commits and diffs

This chapter adds the other half of a repository page: the history, and one
commit rendered as a diff.

At the end of this chapter you can read what changed in a commit, file by file,
highlighted on both sides.

## The reads

| Page element | Call |
| --- | --- |
| History | `ListCommits`, paged by `nextPageToken` |
| What one commit changed | `DiffCommit` with an unset base |

`DiffCommit` returns changed paths and the blob ID for each side. Fetch blobs
by object ID so the page can cache repeated content.

## Add the reads

Extend `lib/enroute.ts`:

```ts
import type {
  Commit, DiffCommitResponse, FileChange, ListCommitsResponse,
} from "./gen/enroute/api/v1alpha1/object";

export async function listCommits(key: string, hex: string, pageToken = "") {
  const stream = objects.listCommits(
    { repo: { key }, commitId: { hex }, limit: 50, pageToken },
    tenant(),
  );

  const commits: Commit[] = [];
  let nextPageToken = "";
  for await (const page of stream as AsyncIterable<ListCommitsResponse>) {
    commits.push(...page.commits);
    nextPageToken = page.nextPageToken;
  }

  return { commits, nextPageToken };
}

export async function diffCommit(key: string, hex: string) {
  // An unset base_commit_id diffs against the first parent, which is what
  // "what this commit changed" means.
  const stream = objects.diffCommit(
    { repo: { key }, commitId: { hex }, baseCommitId: undefined },
    tenant(),
  );

  const changes: FileChange[] = [];
  let truncated = false;
  for await (const page of stream as AsyncIterable<DiffCommitResponse>) {
    changes.push(...page.changes);
    truncated ||= page.truncated;
  }

  return { changes, truncated };
}
```

**An empty `nextPageToken` means the history ended.** The walk reached a commit
with no parent. It never means "ask again". Feed the token back as `pageToken`
rather than reading a commit ID out of it: it is a token, not an ID.

**Check `truncated` before you draw.** A truncated diff is a prefix of the
answer. Drawn without a warning it is a commit that silently changed fewer
files than it did.

## Which side is missing tells you the status

A `FileChange` carries an object ID for each side, and which one is unset is
the file status:

```ts
export function statusOf(change: FileChange) {
  if (!change.oldObjectId) return "added" as const;
  if (!change.newObjectId) return "deleted" as const;
  return "modified" as const;
}
```

> **Caution:** A side that does not exist must be `null`, never an empty
> string. An empty string means a file that exists and has nothing in it, so an
> added file handed to a diff renderer as `""` draws as a modification of an
> empty file, with every line marked as inserted into something that was never
> there. Render added and deleted files as one-sided diffs instead.

## Fetch both sides, a few at a time

A commit touching two hundred paths must not open two hundred requests:

```ts
import { readBlob } from "@/lib/enroute";

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>) {
  const results: R[] = new Array(items.length);
  let next = 0;

  async function worker() {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i]);
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

async function sideOf(key: string, path: string, id?: { hex: string }) {
  if (!id) return null;                        // added or deleted: null, not ""
  const blob = await readBlob(key, id.hex);
  if (blob.kind !== "text") return undefined;  // unshowable
  return { name: path, contents: blob.text, cacheKey: id.hex };
}
```

`sideOf` returns three distinct values: `null` for a side that does
not exist, `undefined` for one that exists but cannot be shown, and contents
otherwise. **One binary or oversized side makes the whole file unshowable**,
because a diff needs both.

## The commit page

Create `app/r/[...path]/commit/[hex]/page.tsx`:

```tsx
import { notFound } from "next/navigation";
import { preloadMultiFileDiff } from "@pierre/diffs/ssr";
import { diffCommit } from "@/lib/enroute";
import { repoByPath } from "@/lib/store";
import { CommitDiff } from "./commit-diff";

export const runtime = "nodejs";

const THEME = {
  theme: { light: "pierre-light", dark: "pierre-dark" },
  themeType: "system",
} as const;

export default async function Page({
  params,
}: {
  params: Promise<{ path: string[]; hex: string }>;
}) {
  const { path, hex } = await params;
  const repo = await repoByPath(path.join("/"));
  if (!repo) notFound();

  const { changes, truncated } = await diffCommit(repo.id, hex);

  const files = await mapLimit(changes, 8, async (change) => {
    const oldFile = await sideOf(repo.id, change.path, change.oldObjectId);
    const newFile = await sideOf(repo.id, change.path, change.newObjectId);

    if (oldFile === undefined || newFile === undefined) {
      return { path: change.path, unshowable: true as const };
    }

    return {
      path: change.path,
      preloaded: await preloadMultiFileDiff({ oldFile, newFile, options: THEME }),
    };
  });

  return (
    <main>
      <h1>{hex.slice(0, 8)}</h1>
      {truncated && <p>This diff is truncated. Some files are not shown.</p>}
      <CommitDiff files={files} />
    </main>
  );
}
```

And the client boundary, `app/r/[...path]/commit/[hex]/commit-diff.tsx`:

```tsx
"use client";

import { MultiFileDiff } from "@pierre/diffs/react";
import type { PreloadMultiFileDiffResult } from "@pierre/diffs/ssr";

type Entry =
  | { path: string; unshowable: true }
  | { path: string; preloaded: PreloadMultiFileDiffResult<undefined, undefined> };

export function CommitDiff({ files }: { files: Entry[] }) {
  return (
    <>
      {files.map((file) => (
        <section key={file.path}>
          <h2>{file.path}</h2>
          {"unshowable" in file ? (
            <p>This file cannot be shown.</p>
          ) : (
            <MultiFileDiff {...file.preloaded} />
          )}
        </section>
      ))}
    </>
  );
}
```

**Match the preload function to the surface it feeds.** `preloadFile` feeds
`File`, and `preloadMultiFileDiff` feeds `MultiFileDiff`. Each result holds the
props of its own component and no other.

**Report a file that failed as one failed file.** The ones already drawn stay
drawn. A single unreadable blob should not cost the whole page.

**The bundled themes are `pierre-light` and `pierre-dark`.** The theme type
accepts any string, so a name the package does not bundle is not a compile
error. It is a page that renders unstyled.

## Link the history

Add the commit list to the repository page from chapter 6:

```tsx
const { commits } = await listCommits(repo.id, tip.hex);

<ul>
  {commits.map((c) => (
    <li key={c.commitId!.hex}>
      <a href={`/r/${repo.path}/commit/${c.commitId!.hex}`}>
        {c.summary}
      </a>{" "}
      — {c.author?.name}
    </li>
  ))}
</ul>
```

`ListCommits` walks first parents, newest first. `Commit` carries the summary
and body separately, and an `Identity` for the author and the committer, so a
listing needs no parsing of its own.

## Check your work

Make a change with something on both sides of a diff:

```sh
cd widgets
echo 'export const hello = "everyone";' > src/hello.ts
echo 'temporary' > NOTES.md
git add -A && git commit -m "rename the greeting, add notes"
git push origin main
```

Open the repository page, then follow the newest commit. `src/hello.ts` renders
as a modification with one line on each side. `NOTES.md` renders as an addition,
with no old side at all rather than a diff against an empty file.

Delete a file and push again. It renders as a deletion, with no new side.

## Result

- Paged history, and a diff for one commit.
- A renderer that tells added, deleted, and modified apart from which object ID
  is missing.
- Bounded concurrency, so a large commit is a slower page rather than a burst of
  requests.

## Going further

For one scroll region holding many files, which the client parses as each one
lands, use `parseDiffFromFile` from `@pierre/diffs` instead of a preload per
file. A code hosting platform has both kinds of page: they are different
shapes, not a better one and a worse one. Both packages ship a README, and
[diffs.com](https://diffs.com) documents the renderer in full.

Two things worth adding once the pages work:

- **A promise cache keyed on `<repo>/<objectId>`.** Share the promise rather
  than starting one per caller. Delete the key on failure, or the error becomes
  the cached answer for good.
- **One shared theme.** Both packages speak the same theme format. Page CSS
  cannot reach into a tree drawn in a shadow root, so resolve the theme once
  rather than matching colours by hand.

That is the guide. Your app serves Git, decides who may do what, and renders
code and diffs on a page.

Where to go next:

- [Protected branches](../patterns/protected-branches.md) — the first
  pattern built on this app, and where real policy starts.
- [Hook reference](../reference/hooks.md) — every field of every hook.
- [The contract](../reference/the-contract.md) — every RPC your app calls.
- [Limitations](../reference/limitations.md) — what does not work yet.
