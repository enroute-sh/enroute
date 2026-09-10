# Clients and interfaces

Enroute has two inbound interfaces and one callback path.

```text
Git client -- Git smart HTTP --> Enroute <-- gRPC -- application
                                    |
                                    +-- signed HTTP hooks --> application
```

| Interface | Caller | Use |
| --- | --- | --- |
| `listen.git` | Git clients | Clone, fetch, and push over Git smart HTTP |
| `listen.api` | Your application | Create repositories, read Git data, and move refs over gRPC |
| Hook endpoint | Enroute | Ask your application for identity and policy decisions |

Enroute terminates Git connections. Your application does not process
pkt-lines or serve clone and push connections. It uses the gRPC API to read
objects and metadata, then exposes them in its own UI or services.

The Git listener is normally public. Keep the API listener private: the tenant
header identifies the caller's tenant, so any client that can set it can act as
that tenant. See [Security](../operate/security.md).

Enroute stores durable metadata in Postgres and Git objects and indexes in an
object store. Its compute processes hold no repository state. See
[Storage](storage.md).
