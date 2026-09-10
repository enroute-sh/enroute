# Rendering diffs and trees

Turning what the contract answers into a diff and a file tree on a page.

## The problem

`diffCommit` answers *which paths changed, and the blob on each side*.
`listTree` answers *a flat list of paths*. Neither carries a byte of text, and
at first reading both look like half an answer, as though something between
them and the page still has to parse git objects and run a diff algorithm.

Nothing does. The contract is shaped for the two libraries that draw this,
and the join is close to free. What is left is a handful of small decisions,
and two of them fail without complaining.

A file that was added has no old side. If you hand a diff parser an empty
string, that means something different: a file that exists and has nothing in
it. An add then renders as a modification of an empty file, with every line
marked as inserted into something that was never there. It looks almost
right, which is why it survives review.

The other is where the bytes come from. An object id addresses a blob, so the
obvious economy is to let the page fetch them itself. That means the contract
listener answering the open internet, which is not a thing to put in front of
a browser: reaching it is what says which tenant a call is for, so anything
that can reach it is every tenant.

## The solution

Two libraries take it from there. Both are React. What matters below the
React is the shape, and that holds in any language.

| Package | Takes | Draws |
| --- | --- | --- |
| [`@pierre/diffs`](https://www.npmjs.com/package/@pierre/diffs) | two file contents | the hunks between them, highlighted |
| [`@pierre/trees`](https://www.npmjs.com/package/@pierre/trees) | a list of paths | a file tree, with per-row git status |

This recipe is written against `@pierre/diffs` 1.3.6 and `@pierre/trees`
1.0.0-beta.6. What is below is the whole of what the join needs. Each package
ships a README, and [diffs.com](https://diffs.com) documents the renderer in
full, for the surface this recipe does not reach.

```ts
function status(change: FileChange): "added" | "deleted" | "modified" {
  if (!change.oldObjectId) return "added";     // unset, not an empty string
  if (!change.newObjectId) return "deleted";
  return "modified";
}

// A side that does not exist is null, never an empty string.
const before = change.oldObjectId
  ? { name: change.path, contents: oldText }
  : null;
const after = change.newObjectId
  ? { name: change.path, contents: newText }
  : null;

const item = {
  id: change.path,
  type: "diff" as const,
  fileDiff: parseDiffFromFile(before, after),
};
```

Which id is unset is the whole of the status of the file, and it is the same
word the tree colours its row with, so the two cannot disagree.

**A missing side is `null`, never an empty string.** That is how the parser
is told the file was added or deleted.

**Pass the path as the name**, although the parser never opens a file. The
extension is how the language for highlighting is chosen.

**Pass the object id of the blob as `cacheKey`.** `FileContents` takes one,
and it wants an identifier that is unique and stable. A git object id is
exactly that. The contract hands one over, so there is no key to invent and
none that can go stale against its content.

**One binary or truncated side makes the whole file unshowable.** A diff
needs both.

## Draw it on the server

The application already holds both sides as text. It read those blobs to
build the diff. If you give that text to the browser to highlight a second
time, you pay for the same work twice. A surface that draws only on the
client can also mount, report no error, and paint nothing.

`@pierre/diffs/ssr` closes both. Each surface has a preload function. The
function returns the props of that component, with the highlighted markup in
them. The server sends HTML, and the browser hydrates what it receives.

```tsx
// The page, on the server.
import { preloadMultiFileDiff } from "@pierre/diffs/ssr";

const preloaded = await preloadMultiFileDiff({
  oldFile: before,                 // null for an add, as above
  newFile: after,                  // null for a delete
  options: {
    theme: { light: "pierre-light", dark: "pierre-dark" },
    themeType: "system",
  },
});

return <DiffSurface {...preloaded} />;
```

```tsx
// The client boundary, and nothing else in the file.
"use client";
import { MultiFileDiff } from "@pierre/diffs/react";
import type { PreloadMultiFileDiffResult } from "@pierre/diffs/ssr";

export function DiffSurface(props: PreloadMultiFileDiffResult<undefined>) {
  return <MultiFileDiff {...props} />;
}
```

**Match the preload function to the surface it feeds.** `preloadFile` feeds
`File`, and `preloadMultiFileDiff` feeds `MultiFileDiff`. The result holds
the props of that component and no other.

**The bundled themes are `pierre-light` and `pierre-dark`.** `DiffsThemeNames`
accepts any string, so a name the package does not bundle is not a compile
error.

**Use this path for a page that draws one file or one commit.** Use the
`parseDiffFromFile` shape above for one scroll region that holds many files,
which the client parses as each one lands. A forge has both kinds of page.
They are different shapes, not a better one and a worse one.

## A tree, from a flat list of paths

`listTree` lists directories as well as the files in them. Drop the
directories: a tree library rebuilds them from the paths, and an empty
directory is not worth a second shape of entry.

```ts
async function filesAt(repo: RepoKey, commit: string) {
  const files: { path: string; objectId: string }[] = [];
  let truncated = false;

  // A commit resolves to its root tree, so no round trip to find one.
  for await (const page of objects.listTree({ repo, objectId: { hex: commit } })) {
    for (const e of page.entries) {
      if (e.kind === ObjectKind.BLOB) {
        files.push({ path: e.path, objectId: e.objectId!.hex });
      }
    }
    truncated ||= page.truncated;
  }

  return { files: files.sort(byTreeOrder), truncated };
}
```

Keep the `objectId` of each entry beside its path. That is what gets asked
for when somebody opens the file, and it saves resolving a path a second time.

## Read blobs by object id, and cache on it

`getObject` is addressed by id, not by path, and that is what makes the
answer worth keeping. An id names those bytes and no others, so two pages
asking for one blob ask the same question. In a commit where the old side of
one file is the new side of another, it is read once.

```ts
async function read(repo: RepoKey, hex: string) {
  const chunks: Uint8Array[] = [];
  let size = 0;

  for await (const res of objects.getObject({ repo, objectId: { hex } })) {
    if (res.chunk.case !== "data") continue;   // the header arrives first
    chunks.push(res.chunk.value);
    size += res.chunk.value.length;
    if (size > MAX_BYTES) break;               // stop reading at the cap
  }

  const bytes = concat(chunks);
  if (bytes.includes(0)) return { reason: "binary" as const };
  return { text: new TextDecoder().decode(bytes), truncated: size > MAX_BYTES };
}
```

**Serve the bytes from a route of the application**, never from the browser
to Enroute. The application reaches the contract from inside whatever
authenticates it. A page that fetched for itself would need that same reach.

**A promise cache keyed on `<repo>/<hex>` is all it takes.** Share the promise
instead of starting one per caller, so a reader that navigates away costs the
next one nothing. Delete the key on failure, or the error becomes the cached
answer for good.

**A NUL byte is the binary test.** It is what git uses, and it is a better
test than a failed decode. UTF-8 accepts plenty of byte sequences nobody wants
rendered.

**Cap the bytes and stop reading at the cap.** The header carries the size,
so a renderer can refuse a large blob before it buffers any of it. A stream
stopped early costs less than one read to the end and thrown away.

## Fetching a whole commit's worth

- **Bound the concurrency.** A commit touching two hundred paths must not open
  two hundred requests. Send a handful at a time, and start another as each
  one lands.
- **Parse as each file lands, not in a pass at the end.** The parse is the
  expensive half of drawing, and a page that grows by a file would otherwise
  pay it again for every file already on screen.
- **Report a file that failed as one failed file.** The ones already drawn
  stay drawn.
- **Sort once, into the order of the tree.** Enroute sends changes in path
  order, which is not the order a tree reads in: a directory takes its files
  with it. Sort the scrolling list and the tree with the same comparator, or
  the two name the files in different orders.

**Share one theme.** Both libraries speak the same theme format. The bundled
pair is `pierre-light` and `pierre-dark`. Page CSS cannot reach into a tree
drawn in a shadow root. Resolve the theme once and convert it for the tree,
instead of matching colours by hand.

## What it does not do

**A merge base that came back `exhausted` is not a merge base.** Draw no diff
and say why. `exhausted` means the walk stopped before it had the answer, and
a diff against the wrong commit is worse than no diff.

**Enroute renders nothing for you.** Enroute answers with ids and paths. The
application fetched and drew every byte on the page. That does not mean the
page must draw in the browser. See [Draw it on the
server](#draw-it-on-the-server).

## See also

- [browsing.md](browsing.md): which call answers which page, and the reads
  these draw from.
- [merge-requests.md](merge-requests.md): where the merge base being diffed
  against comes from.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
