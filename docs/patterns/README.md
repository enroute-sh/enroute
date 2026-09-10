# Patterns

The [Build a code hosting platform](../build/README.md) guide gets Git working: clone, push, and pages to
read the result on. These patterns build on it. None of them is
needed to serve Git.

Each page is written against the app the guide produces, and changes a part of
it. Read the one you want; they do not need to be read in order.

| Pattern | What it is | Carried by |
| --- | --- | --- |
| [Repository lifecycle](repository-lifecycle.md) | Create, rename, and delete a repository, or make one by pushing to it | `RepositoryService`, `authorize` |
| [Protected branches](protected-branches.md) | Refuse a force-push or a delete on a branch | `pre_receive` |
| [Merge requests](merge-requests.md) | Open one, show the change, merge it | `post_receive`, `FindMergeBases`, `DiffCommit`, `IsAncestor`, `UpdateRefs` |
| [Merge queue](merge-queue.md) | Land on evidence, with no lock and no worker | `IsAncestor`, `UpdateRefs` |
| [Checks](checks.md) | Evidence instead of CI: whoever ran it reports the verdict | Your own tables |
| [Private repositories](private-repositories.md) | Who may read, and which refs they are told about | `authorize`, `visible_refs` |
| [Deploy keys](deploy-keys.md) | A credential scoped to one repository, and bot actors | `authorize` |
| [CI on push](ci-on-push.md) | Build what landed, and read what changed | `post_receive`, `DiffCommit`, `ListRefs` |
| [Mirroring](mirroring.md) | Sync refs to another host as they land | `post_receive`, `ListRefs`, `PushToRemote` |

Browsing a repository and rendering diffs are not here. They are
[chapter 6](../build/06-browse-repositories.md) and
[chapter 7](../build/07-read-commits-and-diffs.md) of the guide.

## What every pattern assumes

These hold everywhere and are stated once, where they belong:

| Assumption | Stated in |
| --- | --- |
| The key is yours, and the create is idempotent on it | [Repository keys](../reference/the-contract.md#repository-keys) |
| `actor` is an ID in your namespace, never a credential | [`authorize`](../reference/hooks.md#authorize) |
| An absent object ID is an unset field, never forty zeros | [`ObjectId`](../reference/hooks.md#objectid) |
| A refusal is per ref, and an unjudged command fails the push | [`pre_receive`](../reference/hooks.md#pre_receive) |
| Enroute holds no policy of its own | [Hooks](../concepts/hooks.md) |

Two rules are about design rather than the contract, and every page here
depends on them:

**A rule lives outside what it gates.** A policy a pusher can change by
pushing is not a policy. What a merge is gated on belongs in your app's own
configuration, reviewed and deployed like the rest of it, never in a file in
the branch under test.

`UpdateRefs` does not run `pre_receive`. Use it for application-controlled
merges instead of introducing a privileged Git identity solely to bypass a
push rule.

## Examples

The code is TypeScript on Next.js, continuing the guide's app: `lib/store.ts`
for your own data, `lib/enroute.ts` for the client, and `lib/hooks/` for the
four calls. The contract is language-neutral, and so are these patterns —
translate them into whatever you are building in.
