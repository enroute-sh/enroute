# Push to a remote

`SyncService.PushToRemote` sends refs from an Enroute repository to another
Git server. Enroute builds the pack and acts as the Git client.

Each call supplies its remote URL and either basic or bearer credentials.
Credentials are sent for the call and then discarded; remotes are not stored.

`refs` cannot be empty. An empty `source` deletes `destination`; an empty
`destination` uses the source name. Set `force` per ref when Enroute cannot
prove a fast-forward. Set `atomic` to require all refs to succeed; the call is
rejected if the remote does not support atomic pushes.

`outcomes` match request order. `STATUS_UPDATED`, `STATUS_DELETED`, and
`STATUS_UP_TO_DATE` are successful outcomes. `STATUS_REJECTED` includes a
message from the remote or Enroute's fast-forward check.

A call that fails before it has outcomes fails the RPC. `UNAVAILABLE` means
that Enroute could not reach the remote. `UNKNOWN` means that Enroute sent the
push and could not read the remote's answer, so the push may have landed. List
the remote's refs before you act on an `UNKNOWN` failure.

The call has a 30-second ref-discovery limit, a one-hour total limit, and a
one-minute no-output timeout. Retrying a timeout is safe if the remote already
has the requested values.

## URL restrictions

- Use `https`; `http` requires `git.allow_private_remotes`.
- Do not include a username, password, query, or fragment in the URL.
- Hosts must resolve to public addresses unless private remotes are enabled.
  Enroute checks the addresses that the connection dials, not an earlier
  lookup.
- Redirects are rejected so credentials cannot reach an unnamed host.
