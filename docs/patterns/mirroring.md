# Mirroring

Mirroring keeps a copy of every ref in step on another Git host as changes
land.

## Use this pattern when

Use mirroring for a public copy, backup, or integration that requires current
refs on another Git host.

`PushToRemote` is the one call that dials out, so your app holds no checkout
and runs no push of its own. `post_receive` says a ref moved. The two look
like they compose directly: take the commands the hook handed you and send
those IDs.

Do not enqueue individual hook commands as the mirror state. Two jobs can run
out of order and send an older tip after a newer one.

`post_receive` is best-effort and at most once. Use it as a trigger, but run a
periodic reconciliation job to repair missed deliveries.

## Implement the pattern

Put the *repository* on the queue, and have the job read the tips itself and
send whatever differs from what it last recorded.

```ts
import { RefPushOutcome_Status as Status } from "@/lib/gen/enroute/api/v1alpha1/sync";
import { listRefs, pushToRemote } from "@/lib/enroute";

export async function postReceive(req: PostReceiveRequest) {
  await enqueueMirror(req.repo!.key);      // the repository, not the commands
  return { messages: [] };
}

export async function mirror(repoId: string) {
  const { refs: here } = await listRefs(repoId);
  const sent = await lastMirrored(repoId);          // your own record

  const specs = [];
  for (const ref of here) {
    if (sent.get(ref.name) !== ref.objectId!.hex) {
      specs.push({ source: ref.name, destination: ref.name, force: false });
    }
  }
  for (const name of sent.keys()) {                 // gone here, gone there
    if (!here.some((r) => r.name === name)) {
      specs.push({ source: "", destination: name, force: false });
    }
  }
  if (specs.length === 0) return;

  const { outcomes } = await pushToRemote(repoId, {
    url: await remoteUrl(repoId),
    basic: { username: "x-access-token", password: await installationToken() },
  }, specs);

  for (const o of outcomes) {
    if (o.status === Status.STATUS_REJECTED) {
      await recordFailure(o.destination, o.message);
    } else if (o.status === Status.STATUS_DELETED || !o.newObjectId) {
      await dropMirrored(o.destination);            // or it re-sends forever
    } else {
      await recordMirrored(o.destination, o.newObjectId.hex);
    }
  }
}
```

This is a mirror of *what the repository holds now*, and every property worth
having comes from that:

- **It is idempotent.** Running it twice costs one `STATUS_UP_TO_DATE` per ref.
- **The order stops mattering.** Two jobs for two quick pushes can run in
  either order, or at the same time, and both drive toward the same tips.
- **A burst becomes one job.** Ten pushes while the queue is behind cost the
  work of one job, not ten.
- **A delete needs no special case.** A refname in the record and not in
  `ListRefs` is one to remove, which is an empty `source`. A mirror driven off
  the hook's commands has to remember the delete. This one cannot forget it.

**An outcome may carry no new ID.** A delete of a ref the remote never held
comes back `STATUS_UP_TO_DATE` with both IDs unset, because nothing was sent
for it. Test for the ID before you read it, or one such outcome throws and
takes the whole pass with it.

**Run the same job on a timer.** The hook makes the mirror prompt. The timer
makes it true. It compares and sends nothing when there is nothing to send,
which makes it cheap enough to run often. This is the same shape as the sweep
in [Merge queue](merge-queue.md), for the same reason, and a failure in one
repository must not stop the rest.

**Read the credential in the job**, never carry it in the queue payload. A
token in a queue is a token in whatever the queue writes to, and it outlives
its rotation.

### Know what the call does and does not do

**Enroute stores nothing about the remote.** The URL and the credential ride
on every call, because a remote is a relationship between your repository and
somebody else's, and Enroute holds neither name. Rotation is yours, and there
is nothing here to rotate.

**A push that timed out is safe to repeat.** A ref the remote already holds at
the value asked for comes back `STATUS_UP_TO_DATE`, with nothing sent.

**Enroute refuses a force, out of its own commit graph.** The Git wire
protocol carries no force bit, so the client is what refuses a push that would
drop what the remote holds, and here the client is Enroute. Set
`RefSpec.force` when you intend to overwrite what the remote holds. A mirror
of a repository whose branches get rebased needs it. A mirror of a trunk must
not have it.

**`atomic` is all-or-nothing, where the remote offers it.** Enroute refuses
the call rather than fall back to one ref at a time, because a caller that
asked for all-or-nothing and got a partial push has no way to tell.

**A rejected ref is one ref.** The others in the same call still landed.
Record per destination, and let the next pass retry what failed. It will,
because the record for that ref was not advanced.

**Enroute refuses a redirect.** The error names the URL the remote pointed at.
Update the remote URL to that: a renamed repository on the other host answers
`301` until the URL is updated or the rename undone.

**A remote that cannot be reached fails the call.** No outcome is reported.
Leave the record alone and let the next pass find out, which is what a
desired-state job does anyway.

## See also

- [CI on push](ci-on-push.md) — the same hook, the same at-most-once problem,
  and the same answer of reconciling against state.
- [Deploy keys](deploy-keys.md) — credentials in the other direction, for
  something reading from here.
- [Push to a remote](../reference/push-to-a-remote.md) — the URL rules, the
  credential shapes, and every outcome status.
