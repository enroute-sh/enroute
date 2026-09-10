# Protocol Buffers

Proto files define every wire type and field.

| Source | Use |
| --- | --- |
| `/usr/share/enroute/proto` in the image | Generate a client for that image |
| A copy in your project | Build and version the dependency |
| gRPC reflection | Inspect a running server |
| `proto/enroute/` in source | Inspect the current source definition |

| Package | Contents |
| --- | --- |
| `enroute.api.v1alpha1` | Repository, ref, object, and sync services |
| `enroute.hook.v1alpha1` | Hook request and response messages |
| `enroute.common.v1alpha1` | `RepoKey` and `ObjectId` |

Optional object IDs are unset fields, not empty strings or forty zeroes.

The hook package has no gRPC service. Hooks are HTTP `POST` requests to one
endpoint, and `HookRequest.call` selects the hook type. Generate all proto
files, including the hook messages:

```sh
grpcurl -plaintext 127.0.0.1:50051 describe enroute.hook.v1alpha1.HookRequest
```

The API listener exposes gRPC reflection from the descriptors built into the
server. Reflection does not require a tenant header and exposes protocol
definitions, not repository data. Use it to inspect a running server; use a
committed proto copy to generate and version a client.

`v1alpha1` can change before Enroute 1.0. Copy and commit the proto files in
your project rather than using this repository as a build dependency.
