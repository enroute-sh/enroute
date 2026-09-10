# Mirror to another git host

Keeping a copy of every ref in step on GitHub or another git server, as they
land.

## The problem

Somebody wants what is here to also be over there: a public copy on GitHub, a
backup on another host, a mirror somebody else's tooling reads. The refs move
here, and something has to make them move there too.

`pushToRemote` is the one call that dials out, so the application holds no
checkout and runs no push of its own. `postReceive` says a ref moved. The two
look like they compose directly: take the commands the hook handed you and
send those ids.

They do not, and both halves fail quietly.

If you carry the commands of the hook, the job becomes a log of events instead
of a statement of what is true. Two pushes land in quick succession, two jobs
go on the queue, and nothing promises they run in that order, so the older job
sends the older tip and overwrites the newer one. The mirror is behind by one
push, and nothing retries, because both jobs succeeded.

The other half never announces itself. `postReceive` is best effort and **at
most once**: Enroute sends it after the push succeeded, so it cannot retry
without lying to a client that has gone, and nothing tells an application that
was unreachable for those seconds. A mirror driven only by that hook drifts
the first time one delivery is missed, and a stale mirror looks exactly like a
working one. It answers clones. It has branches. Nobody notices until
somebody reads a commit that must be there and is not.

## The solution

Put the *repository* on the queue, and have the job read the tips itself and
send whatever differs from what it last recorded.

```ts
import {
  RefPushOutcome_Status as Status,
} from "./gen/enroute/api/v1alpha1/sync_pb";

async function postReceive(
  req: PostReceiveRequest,
): Promise<PostReceiveResponse> {
  await enqueueMirror(req.repo);        // the repository, not the commands
  return { messages: [] };
}

async function mirror(repo: RepoKey): Promise<void> {
  const { refs: here } = await refs.listRefs({ repo });
  const sent = await lastMirrored(repo);           // your own record

  const specs: RefSpec[] = [];
  for (const r of here) {
    if (sent.get(r.name) !== r.objectId!.hex) {
      specs.push({ source: r.name, destination: r.name, force: false });
    }
  }
  for (const name of sent.keys()) {                 // gone here, gone there
    if (!here.some((r) => r.name === name)) {
      specs.push({ source: "", destination: name, force: false });
    }
  }
  if (specs.length === 0) return;

  const { outcomes } = await sync.pushToRemote({
    repo,
    remote: {
      url: await remoteUrl(repo),
      credentials: {
        case: "basic",
        value: {
          username: "x-access-token",
          password: await installationToken(),
        },
      },
    },
    refs: specs,
  });

  for (const o of outcomes) {
    if (o.status === Status.REJECTED) {
      await recordFailure(o.destination, o.message);
    } else if (o.status === Status.DELETED || !o.newObjectId) {
      await dropMirrored(o.destination);      // or it re-sends forever
    } else {
      await recordMirrored(o.destination, o.newObjectId.hex);
    }
  }
}
```

This is a mirror of *what the repository holds now*, and every property worth
having comes from that:

- **It is idempotent.** To run it twice costs one `Status.UP_TO_DATE` per ref.
- **The order stops mattering.** Two jobs for two quick pushes can run in
  either order, or at the same time, and both drive toward the same tips.
- **A burst becomes one job.** Ten pushes while the queue is behind cost the
  work of one job, not ten.
- **A delete needs no special case.** A refname in the record and not in
  `listRefs` is one to remove, which is an empty `source`. A mirror driven off
  the hook's commands has to remember the delete. This one cannot forget it.

**An outcome may carry no new id.** A delete of a ref the remote never held
comes back `Status.UP_TO_DATE` with both ids unset, because nothing was sent
for it. Test for the id before you read it, or one such outcome throws and
takes the whole pass with it.

**Run the same job on a timer.** The hook makes the mirror prompt. The timer
makes it true. It compares and sends nothing when there is nothing to send,
which makes it cheap enough to run often. This is the same shape as the sweep
in [merge-queue.md](merge-queue.md), for the same reason, and a failure in
one repository must not stop the rest.

**Read the credential in the job**, never carry it in the queue payload. A
token in a queue is a token in whatever the queue writes to, and it outlives
its rotation.

## What the call does and does not do

**Enroute stores nothing about the remote.** The URL and the credential ride
on every call, because a remote is a relationship between the application's
repository and somebody else's, and Enroute holds neither name. Rotation
belongs to the application, and there is nothing here to rotate.

**A push that timed out is safe to repeat.** A ref the remote already holds
at the value asked for comes back `Status.UP_TO_DATE`, with nothing sent.

**Enroute refuses a force, out of its own commit graph.** The git wire
protocol carries no force bit, so the client is what refuses a push that
would drop what the remote holds, and here the client is Enroute. Set
`RefSpec.force` when you intend to overwrite what the remote holds. A mirror
of a repository whose branches get rebased needs it. A mirror of a trunk
must not have it.

**`atomic` is all-or-nothing, where the remote offers it.** Enroute refuses
the call rather than fall back to one ref at a time, because a caller that
asked for all-or-nothing and got a partial push has no way to tell.

**A rejected ref is one ref.** The others in the same call still landed.
Record per destination, and let the next pass retry what failed. It will,
because the record for that ref was not advanced.

**Enroute refuses a redirect.** The error names the URL the remote pointed
at. Update the remote URL to that. A renamed GitHub repository answers `301`
until the URL is updated or the rename undone.

**A remote that cannot be reached fails the call.** No outcome is reported.
Leave the record alone and let the next pass find out, which is what a
desired-state job does anyway.

## See also

- [ci-on-push.md](ci-on-push.md): the same hook, the same at-most-once
  problem, and the same answer of reconciling against state.
- [deploy-keys.md](deploy-keys.md): credentials in the other direction, for
  something reading from here.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
