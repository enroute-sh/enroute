# Recipes

`SKILL.md` makes git work. These are the patterns built on top of it. Each one
is a file of its own. Read the rules below, then open the recipe the user
asked for, and whatever its closing links say it rests on. None of them is
needed to serve git.

Every recipe opens with `## The problem`, which gives the problem it solves
and what the obvious answer costs, then the code under `## The solution`. A
recipe that is not the one the user wants says so on its first screen.

| Recipe | What it is | Carried by |
| --- | --- | --- |
| [repository-lifecycle.md](recipes/repository-lifecycle.md) | Create, rename, and delete a repository, or make one by pushing to it | `RepositoryService`, `authorize`, and a table of the user's own |
| [protected-branches.md](recipes/protected-branches.md) | Refuse a force-push or a delete on a branch | `pre_receive` |
| [merge-requests.md](recipes/merge-requests.md) | Open one, show the change, merge it | `post_receive`, `FindMergeBases`, `DiffCommit`, `IsAncestor`, `UpdateRefs` |
| [merge-queue.md](recipes/merge-queue.md) | Land on evidence, with no lock and no worker | `IsAncestor`, `UpdateRefs` |
| [checks.md](recipes/checks.md) | Evidence instead of CI: whoever ran it reports the verdict | Nothing. The application's own table |
| [private-repositories.md](recipes/private-repositories.md) | Who may read, and which refs they are told about | `authorize`, `visible_refs` |
| [deploy-keys.md](recipes/deploy-keys.md) | A credential scoped to one repository, and bot actors | `authorize` |
| [ci-on-push.md](recipes/ci-on-push.md) | Build what landed, and read what changed | `post_receive`, `DiffCommit`, `ListRefs` |
| [mirroring.md](recipes/mirroring.md) | Sync refs to GitHub or another host as they land | `post_receive`, `ListRefs`, `PushToRemote` |
| [browsing.md](recipes/browsing.md) | A UI over a repository, with no clone | `ListRefs`, `ListCommits`, `ListTree`, `GetObject` |
| [rendering.md](recipes/rendering.md) | Draw the diff and the file tree, with `@pierre/diffs` and `@pierre/trees` | `DiffCommit`, `ListTree`, `GetObject` |

Build a recipe only when the user asks for it, and only after phase 5 passes.

The code in each file is TypeScript, the one language the examples use.
`references/contract.md` names its toolchain first, and real code translates
more reliably than a dialect nobody runs. It is a reference, not a
restriction. Build the integration in the language the user asked for, and
translate these into it.

## What every recipe assumes

**A key the application chose.** Enroute holds no name and no owner, and
allocates no id. The key passed to `CreateRepository` is what every later call
names, so no table maps an id of Enroute's onto the project's. Use the stable
id the project already has for a repository: a row id, a UUID, a ULID. Never
`owner/name`, which a rename or a transfer changes, and which would then name
a different repository. A key is unique within the tenant, holds ASCII
letters, digits, `-`, `_`, and `.`, and starts and ends with a letter or
digit. The create is idempotent on the key, so a retry is safe.

**An actor string.** `Granted.actor` is who is asking, in the application's
namespace. Enroute hands every later hook call that answer and never what the
git client said. Put an id there, never a credential and never an email
address.

**The application holds the policy.** Enroute refuses a push whose objects are
missing, a push whose refname git would not accept, and a push whose
`old_object_id` no longer matches what the ref holds. It refuses nothing else.
A force-push, a deleted trunk, or a branch nobody reviewed each lands unless
`preReceive` refuses it.

**A refusal is per ref.** `preReceive` judges every command it is sent, one at
a time. Enroute offers a git client no `atomic` capability, so one refused ref in
a push of three does not hold back the other two. A command it judges neither
way is the exception: nothing is guessed for it, and the whole push fails.

**An id is a message, and an absent one is unset.** `ObjectId { hex }`
wherever an id appears, so a client cannot pass a refname where an id belongs.
Absence has one spelling in both directions: no `old_object_id` is a create,
no `new_object_id` is a delete. Neither git's forty zeroes nor an empty string
means anything here.

**A rule lives outside what it gates.** A policy a pusher can change by
pushing is not a policy. What a merge is gated on belongs in the
application's configuration, never in a file in the branch under test.

**A rule you must exempt yourself from is at the wrong door.** `UpdateRefs`
runs no `pre-receive`, so a `pre_receive` that refuses *everyone* still lets
the application's merge through. Reach for a privileged identity only after
you check whether the two doors already separate the cases.
