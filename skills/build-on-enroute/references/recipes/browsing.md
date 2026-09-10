# Browse a repository

Branches, history, a file tree, and the contents of one file, on a page, with
nothing cloned.

## The problem

A repository has to be readable by something that is not a git client. The
usual way to build that keeps a working copy on disk and calls git for every
page, which puts a checkout, a disk, and a git binary behind the web tier,
and makes every page a fork and a parse.

Enroute answers the questions directly, so a page is a handful of reads. The
trap is that the reads are *not* the git commands they resemble, and the
differences give no error.

`listTree` has no path filter. It walks the whole tree, breadth first, so a UI
that shows one directory and asks for the root gets everything under it. It
is also a stream, as `listCommits`, `getObject`, and `diffCommit` are, so what
arrives is pages and not an answer. Both it and `diffCommit` stop at a server
limit and say so in `truncated`, a flag that costs nothing to ignore. A
listing that ignores it renders as an ordinary directory with some files
missing, and nobody reading that page can tell. And `nextPageToken` is empty
when the walk reached a commit with no parent, so an empty token means the
history ended, never that there is more to ask for. It is a token, not an id:
feed it back as `pageToken` instead of reading a commit out of it.

## The solution

One call per question, the tip read once, and every stream folded with its
`truncated` flag.

| Page | Call |
| --- | --- |
| Branch list | `listRefs`, and `prefixes` to narrow it |
| History | `listCommits`, paged by `nextPageToken` |
| File tree | `listTree` on a commit, which resolves to its root tree |
| One file | `getObject`: a header, then the bytes |
| One commit | `diffCommit` with an unset base, which is what it changed |

```ts
async function treePage(repo: RepoKey, branch: string, dirObjectId?: string) {
  const { refs: tips } = await refs.listRefs({ repo, prefixes: [branch] });
  const tip = tips[0]?.objectId;
  if (!tip) return { entries: [], truncated: false };

  const entries: TreeEntry[] = [];
  let truncated = false;
  for await (const page of objects.listTree({
    repo,
    objectId: dirObjectId ?? tip,      // an ObjectId, not its hex
  })) {
    entries.push(...page.entries);
    truncated ||= page.truncated;
  }

  // Truncated: what arrived is a prefix. Say so, or list a subtree instead.
  return { entries, truncated };
}
```

**Hold the object id of the directory.** There is no path filter, so a UI
that shows one directory lists *that* object, and the id came from the
listing above it. Entries arrive depth by depth, and the order inside a depth
is not promised.

**`getObject` is a stream with a header first.** One `ObjectHeader`, then the
bytes. The header carries the size git records, so a renderer can refuse a
400 MB blob before it buffers any of it.

**A path does not have to be UTF-8.** Git stores a filename as bytes. An
invalid sequence arrives replaced, so a listing stays a listing instead of
failing over one file nobody can name.

**Page the history, and stop deliberately.** `nextPageToken` is empty only
when the walk reached a commit with no parent. That is the whole history, not
a page of it.

## How far a branch is from the trunk

"Ahead 3, behind 12" is arithmetic on two histories, not a call of its own.
`isAncestor` answers yes or no, and a reader of a branch page wants the
commits. Walk the first parents of the trunk into a map from commit to
position, then walk the branch until it reaches a commit that map holds. What
came before that is ahead, and the position it landed on is behind.

**Cap both walks, and report the cap as a cap.** A branch abandoned a
thousand commits ago must cost the same page as one opened this morning,
and a reader does nothing more with a number than "50+". Report what is over
the cap as over it, never as a number that is wrong.

**This shortcut needs a linear trunk.** It works because a fast-forward-only
trunk *is* its own first-parent history, so a branch cut from it meets it
again on its own first-parent chain. If merge commits can land, use
`findMergeBases` instead. Whoever ends the fast-forward-only rule also makes
this walk answer wrongly, with no warning.

## What it does not do

**There is no path filter and no single-path lookup.** A file at a known path
costs the listing that contains it, then `getObject` on the id that listing
gave you.

**There is no rename detection.** Git stores none, so a rename reads as a
delete and an add until somebody decides how similar is similar enough.

**There is no search and no blame.** Neither is a question the contract
answers, and both want an index the application builds for itself out of
these reads.

**It hides nothing on its own.** Every ref this lists is one the caller may
already fetch. To decide who sees what, see
[private-repositories.md](private-repositories.md).

## See also

- [rendering.md](rendering.md): what to do with these answers to draw a diff
  and a file tree.
- [private-repositories.md](private-repositories.md): who is allowed to see
  the pages this builds.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
