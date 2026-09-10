# Chapter 1: Set up Enroute and your app

Create the project, generate code from the contract, and configure the hook
endpoint. At the end of this chapter, Enroute and the app run locally but do
not exchange requests yet.

## Before you start

Complete the [Quickstart](../quickstart.md). Enroute must be running, with Git
on `127.0.0.1:8080` and the API on `127.0.0.1:50051`.

## Create the project

```sh
npx create-next-app@latest codehost --typescript --app --no-tailwind --no-src-dir
cd codehost
npm install @grpc/grpc-js
npm install --save-dev ts-proto
```

This is a normal Next.js application. Enroute terminates Git connections, so
your code does not handle clone or push connections or parse pkt-lines.

## Choose the hook URL

Enroute calls one route in your app for every decision. Choose its URL now,
because chapter 3 serves it and Enroute has to be told about it first.

Do not choose a path that a repository could take. A code hosting platform
serves repositories at paths named after repositories, and a repository can be
named anything a user types, including the path your hooks are on. A repository named `enroute`
would take `/enroute/hooks`.

Put the route in the namespace Next.js keeps for machine traffic:

```text
http://host.docker.internal:3000/api/enroute/hooks
```

Enroute runs in a container, so it reaches your development server at
`host.docker.internal` rather than `127.0.0.1`.

## Point Enroute at it

Edit `config/tenants.toml` in the stack you started in the quickstart:

```toml
[tenants.dev]
hook_endpoint_url = "http://host.docker.internal:3000/api/enroute/hooks"
domains = ["*"]
```

The quickstart sets `tenants.refresh_secs` to two seconds, so Enroute re-reads
the file. No restart is needed.

Write this URL down. Chapter 3 verifies signatures against it, and the two
must match byte for byte. Enroute signs over the URL you configured here, not
over the `Host` header that arrives.

## Get the contract

Copy the proto files out of the image you run, so that your code builds
against the contract that image serves:

```sh
docker create --name enroute-proto ghcr.io/enroute-sh/enroute:latest
mkdir -p proto && docker cp enroute-proto:/usr/share/enroute/proto/enroute proto/
docker rm enroute-proto
```

Commit the copy. Do not add the Enroute repository as a build dependency:
`v1alpha1` can change before `1.0.0`, and a contract that moves under your
project is a build that breaks without a commit. See
[Protocol Buffers](../reference/proto.md).

## Generate the code

```sh
mkdir -p lib/gen
protoc \
  --plugin=./node_modules/.bin/protoc-gen-ts_proto \
  --ts_proto_out=lib/gen \
  --ts_proto_opt=outputServices=grpc-js,esModuleInterop=true \
  --proto_path=proto \
  proto/enroute/*/v1alpha1/*.proto
```

Add the command to `package.json`:

```json
{
  "scripts": {
    "proto": "protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto --ts_proto_out=lib/gen --ts_proto_opt=outputServices=grpc-js,esModuleInterop=true --proto_path=proto proto/enroute/*/v1alpha1/*.proto"
  }
}
```

Six files are generated, in three packages:

| Package | Holds |
| --- | --- |
| `enroute.api.v1alpha1` | The four services your app calls |
| `enroute.hook.v1alpha1` | `HookRequest` and `HookResponse`, which Enroute sends you |
| `enroute.common.v1alpha1` | `RepoKey` and `ObjectId`, used by both |

`hook.proto` declares no service, because every hook is one `POST` to one URL
rather than a method per call. A codegen command written around services alone
skips that file without a message, and chapters 3 to 6 cannot be written
without its types.

## Check your work

The hook types exist:

```sh
grep -c "HookRequest" lib/gen/enroute/hook/v1alpha1/hook.ts
```

A count above zero confirms that code generation included `hook.proto`. Fix a
zero result before continuing; later chapters depend on its types.

The app runs:

```sh
npm run dev
```

Enroute answers, independently of your app:

```sh
grpcurl -plaintext -H 'x-enroute-tenant: dev' 127.0.0.1:50051 list
```

Four services are listed. Git does not serve anything yet, because no endpoint
answers Enroute's questions. That starts in chapter 4.

## Result

- A Next.js project with the Enroute contract generated into `lib/gen`.
- A tenant that names a hook URL your app does not serve yet.

Next: [Create repositories](02-create-repositories.md), where your app creates its first
repository over the API.
