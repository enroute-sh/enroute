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
message from the remote or Enroute's fast-forward check. Transport failures
fail the RPC and no refs are updated.

The call has a 30-second ref-discovery limit, a one-hour total limit, and a
one-minute no-output timeout. Retrying a timeout is safe if the remote already
has the requested values.

## URL restrictions

- Use `https`; `http` requires `sync.allow_private_remotes`.
- Do not include a username, password, query, or fragment in the URL.
- Hosts must resolve to public addresses unless private remotes are enabled.
- Redirects are rejected so credentials cannot reach an unnamed host.

The public-address check and connection can use different DNS resolutions, so
it is not complete protection against DNS rebinding.
