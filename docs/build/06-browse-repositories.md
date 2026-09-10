# Chapter 6: Browse repositories

A code hosting platform has to be readable by something that is not a Git
client. This chapter builds the repository page: branches, a file tree, and one
file, highlighted.

At the end of this chapter you can read your code in a browser. Nothing is
cloned, no checkout sits on a disk, and no `git` binary runs behind the web
tier.

## The reads a page needs

Enroute answers these questions directly. One call per question:

| Page element | Call |
| --- | --- |
| Branch list | `ListRefs`, narrowed by `prefixes` |
| File tree | `ListTree` on a commit, which resolves to its root tree |
| One file | `GetObject`: a header, then the bytes |

Three of these stream, so what arrives is pages rather than an answer. Two of
them stop at a server limit and set `truncated`. Surface that state instead of
rendering a partial directory as complete.

## Add the reads

Extend `lib/enroute.ts` with the two clients and three helpers:

```ts
import {
  ObjectKind, ObjectServiceClient,
} from "./gen/enroute/api/v1alpha1/object";
import type {
  GetObjectResponse, ListTreeResponse, TreeEntry,
} from "./gen/enroute/api/v1alpha1/object";
import { RefServiceClient } from "./gen/enroute/api/v1alpha1/ref";
import type { ListRefsResponse } from "./gen/enroute/api/v1alpha1/ref";

const refs = new RefServiceClient(ADDRESS, credentials.createInsecure());
const objects = new ObjectServiceClient(ADDRESS, credentials.createInsecure());

export function listRefs(key: string, prefixes: string[] = []) {
  return new Promise<ListRefsResponse>((resolve, reject) => {
    refs.listRefs({ repo: { key }, prefixes }, tenant(), (err, res) =>
      err ? reject(err) : resolve(res),
    );
  });
}

export async function listTree(key: string, hex: string) {
  const stream = objects.listTree({ repo: { key }, objectId: { hex } }, tenant());

  const entries: TreeEntry[] = [];
  let truncated = false;
  for await (const page of stream as AsyncIterable<ListTreeResponse>) {
    entries.push(...page.entries);
    truncated ||= page.truncated;
  }

  return { entries, truncated };
}
```

A server-streaming call from `grpc-js` is a Node readable stream, so `for await`
consumes it. Fold `truncated` across the pages: any page that sets it means the
whole answer is a prefix.

## Read one file

`GetObject` sends an `ObjectHeader` first, then the bytes. The header carries
the size Git recorded, so a renderer can refuse a large blob before it buffers
any of it.

```ts
const MAX_BYTES = 512 * 1024;

export type Blob =
  | { kind: "text"; text: string }
  | { kind: "binary" }
  | { kind: "too-large"; size: number };

export async function readBlob(key: string, hex: string): Promise<Blob> {
  const stream = objects.getObject({ repo: { key }, objectId: { hex } }, tenant());

  const chunks: Buffer[] = [];
  let size = 0;

  for await (const res of stream as AsyncIterable<GetObjectResponse>) {
    if (res.header) {
      const declared = Number(res.header.size);
      if (declared > MAX_BYTES) {
        stream.cancel();                  // refuse before buffering anything
        return { kind: "too-large", size: declared };
      }
      continue;
    }
    if (!res.data) continue;
    chunks.push(Buffer.from(res.data));
    size += res.data.length;
  }

  const bytes = Buffer.concat(chunks);

  // A NUL byte is git's own binary test, and a better one than a failed
  // decode: UTF-8 accepts plenty of byte sequences nobody wants rendered.
  if (bytes.includes(0)) return { kind: "binary" };

  return { kind: "text", text: bytes.toString("utf8") };
}
```

**`GetObject` is addressed by object ID, not by path.** That is what makes the
answer worth caching: an ID names those bytes and no others, so two pages
asking for one blob ask the same question. Keep the ID of each entry beside its
path in the listing, and opening a file costs no second lookup.

**Serve bytes from a route of your app, never from the browser to Enroute.**
Your app reaches the API from inside whatever authenticates it. A page that
fetched for itself would need that same reach, and reaching the API listener is
what says which tenant a call is for.

## The rendering libraries

Two React packages draw what these calls answer. Install them:

```sh
npm install @pierre/trees @pierre/diffs
```

| Package | Takes | Draws |
| --- | --- | --- |
| `@pierre/trees` | a list of paths | a file tree, with per-row Git status |
| `@pierre/diffs` | file contents | the file, highlighted, or a diff between two |

Both render on the server and hydrate in the browser. That matters here for a
reason particular to this kind of app: your app already holds the text, because
it read those blobs to build the page. Handing that text to the browser to
highlight a
second time pays for the same work twice, and a surface that draws only on the
client can mount, report no error, and paint nothing.

Each package has a preload function that returns the props of its component,
with the markup already in them.

## The page

Serve repositories under a prefix of their own. A repository can be named
anything a person types, so an app that serves them at the root has to keep
every other route out of their way for good. `/r/` costs one path segment and
ends the problem.

Create `app/r/[...path]/page.tsx`:

```tsx
import { notFound } from "next/navigation";
import { preloadFile } from "@pierre/diffs/ssr";
import { preloadFileTree } from "@pierre/trees/ssr";
import { ObjectKind } from "@/lib/gen/enroute/api/v1alpha1/object";
import { listRefs, listTree, readBlob } from "@/lib/enroute";
import { repoByPath } from "@/lib/store";
import { Browser } from "./browser";

export const runtime = "nodejs";

const THEME = {
  theme: { light: "pierre-light", dark: "pierre-dark" },
  themeType: "system",
} as const;

export default async function Page({
  params,
  searchParams,
}: {
  params: Promise<{ path: string[] }>;
  searchParams: Promise<{ file?: string }>;
}) {
  const { path } = await params;
  const { file } = await searchParams;

  const repo = await repoByPath(path.join("/"));
  if (!repo) notFound();

  const { refs, defaultBranch } = await listRefs(repo.id, ["refs/heads/"]);
  const tip = refs.find((r) => r.name === defaultBranch)?.objectId;
  if (!tip) return <p>This repository is empty. Push something to it.</p>;

  // A commit resolves to its root tree, so there is no round trip to find one.
  const { entries, truncated } = await listTree(repo.id, tip.hex);
  const blobs = entries.filter((e) => e.kind === ObjectKind.OBJECT_KIND_BLOB);

  // The tree library rebuilds directories from the paths, so drop them.
  const tree = preloadFileTree({ id: "tree", paths: blobs.map((e) => e.path) });

  const selected = blobs.find((e) => e.path === file) ?? blobs[0];
  const blob = selected ? await readBlob(repo.id, selected.objectId!.hex) : undefined;

  const preloadedFile =
    selected && blob?.kind === "text"
      ? await preloadFile({
          file: {
            name: selected.path,          // chooses the highlighting language
            contents: blob.text,
            cacheKey: selected.objectId!.hex,
          },
          options: THEME,
        })
      : undefined;

  return (
    <main>
      <h1>{repo.path}</h1>
      <p>
        {refs.length} branches, default {defaultBranch}
      </p>
      {truncated && <p>This tree is truncated. Some files are not listed.</p>}

      <Browser
        paths={blobs.map((e) => e.path)}
        preloadedTree={{ id: tree.id, shadowHtml: tree.shadowHtml }}
        selectedPath={selected?.path}
        preloadedFile={preloadedFile}
        unrenderable={blob && blob.kind !== "text" ? blob.kind : undefined}
      />
    </main>
  );
}
```

**Pass the path as the file's `name`.** The library never opens a file; the
extension is how it chooses the language to highlight.

**Pass the object ID as `cacheKey`.** It wants an identifier that is unique and
stable for those contents, and a Git object ID is exactly that. There is no key
to invent and none that can go stale.

## The client boundary

`useFileTree` builds the model the tree component draws, and it is a hook, so it
lives on the client. It receives the server's markup as `preloadedData` and
hydrates it rather than drawing again.

Create `app/r/[...path]/browser.tsx`:

```tsx
"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { File } from "@pierre/diffs/react";
import type { PreloadedFileResult } from "@pierre/diffs/ssr";
import { FileTree, useFileTree } from "@pierre/trees/react";
import type { FileTreePreloadedData } from "@pierre/trees/react";

export function Browser({
  paths,
  preloadedTree,
  selectedPath,
  preloadedFile,
  unrenderable,
}: {
  paths: string[];
  preloadedTree: FileTreePreloadedData;
  selectedPath?: string;
  preloadedFile?: PreloadedFileResult<undefined, undefined>;
  unrenderable?: "binary" | "too-large";
}) {
  const router = useRouter();
  const search = useSearchParams();

  const { model } = useFileTree({
    id: "tree",
    paths,
    initialSelectedPaths: selectedPath ? [selectedPath] : [],
    onSelectionChange: (selected) => {
      const next = new URLSearchParams(search);
      if (selected[0]) next.set("file", selected[0]);
      router.push(`?${next}`);
    },
  });

  return (
    <div style={{ display: "grid", gridTemplateColumns: "16rem 1fr", gap: "1rem" }}>
      <FileTree model={model} preloadedData={preloadedTree} />
      {unrenderable === "binary" && <p>This file is binary.</p>}
      {unrenderable === "too-large" && <p>This file is too large to show.</p>}
      {preloadedFile && <File {...preloadedFile} />}
    </div>
  );
}
```

The `id` passed to `preloadFileTree` on the server and to `useFileTree` on the
client must match. It is what joins the markup to the model.

## Check your work

Push a few files, so there is something to look at:

```sh
cd widgets
mkdir -p src && echo 'export const hello = "world";' > src/hello.ts
echo '# Widgets' > README.md
git add -A && git commit -m "add some files"
git push origin main
```

Open `http://127.0.0.1:3000/r/acme/widgets`.

The tree lists `README.md` and `src/hello.ts`, with `src` rebuilt as a
directory from the path. Clicking a file loads it, highlighted, beside the
tree. The page renders with JavaScript disabled too, because both surfaces
arrive as markup.

## Result

- Branch, tree, and file reads over the API.
- A repository page that renders on the server and hydrates.
- A binary and size check, so one large or unreadable blob is a message rather
  than a broken page.

## Scope

**There is no path filter and no single-path lookup.** `ListTree` walks the
whole tree breadth first. A page showing one directory lists *that* directory's
object, using the ID from that listing.

**A path is bytes, not text.** Git stores a filename as bytes, so an invalid
UTF-8 sequence arrives with replacements rather than failing the listing.

**There is no search, no blame, and no rename detection.** None of these is a
question the contract answers. Each wants an index your app builds for itself
out of the documented reads.

**It hides nothing.** Every ref this lists is one the caller may already fetch.
Deciding who sees what is the `visible_refs` hook.

Next: [Read commits and diffs](07-read-commits-and-diffs.md).
