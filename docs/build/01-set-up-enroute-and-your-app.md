# Chapter 1: Set up Enroute and your app

Create the project, generate code from the contract, and configure the hook
endpoint. At the end of this chapter, Enroute and the app run locally but do
not exchange requests yet.

## Before you start

Complete the [Quickstart](../quickstart.md). The `enroute` command must be
installed.

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
http://localhost:3000/api/enroute/hooks
```

## Point Enroute at it

Stop the `enroute dev` from the quickstart. In the `codehost` directory, start
it again with the hook URL:

```sh
enroute dev --hooks http://localhost:3000/api/enroute/hooks --enable authorize
```

Every hook is opt-in. Enable a hook only when your app answers it, because
Enroute refuses a Git request when an enabled hook gets no answer. This guide
enables `authorize` now, so that chapter 3 can see a signed request arrive, and
chapter 4 implements it. Chapter 5 enables `pre_receive` and `post_receive`.
Without `--enable`, `--hooks` enables all four hooks.

On the first run, Enroute generates a signing
key and keeps it in `.enroute/dev` for later runs. The banner shows the key ID
and the path of the public key, `.enroute/dev/hooks-signing-key.pub.pem`.
Chapter 3 verifies signatures with that file.

Keep the directory out of Git:

```sh
echo ".enroute/" >> .gitignore
```

Write the URL down. Chapter 3 verifies signatures against it, and the two
must match byte for byte. Enroute signs over the URL you configured here, not
over the `Host` header that arrives.

A deployment sets the same values in its configuration file:

```toml
[hooks]
enabled = ["authorize"]
endpoint_url = "https://codehost.example.com/api/enroute/hooks"
signing_key = "${ENROUTE_HOOK_SIGNING_KEY}"
```

Add each hook to `enabled` when your app answers it. See
[Configuration](../operate/configuration.md).

## Get the contract

Copy the proto files of the release that you run, so that your code builds
against the contract that `enroute` serves. `enroute --version` prints the
release. Each release is a tag of the source repository on GitHub, so take
the files from its archive. For release `0.4.0`:

```sh
curl -fsSL https://github.com/enroute-sh/enroute/archive/refs/tags/v0.4.0.tar.gz | tar -xz
mkdir -p proto && cp -R enroute-0.4.0/proto/enroute proto/
rm -rf enroute-0.4.0
```

The image carries the same files at `/usr/share/enroute/proto`. If you built
`enroute` from a checkout, copy `proto/enroute` from it.

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

Seven files are generated, in three packages:

| Package | Holds |
| --- | --- |
| `enroute.api.v1alpha1` | The five services your app calls |
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
enroute repo list
```

The command exits with no error. Git does not serve anything yet, because
`authorize` is enabled and no endpoint answers it. That starts in chapter 4.

## Result

- A Next.js project with the Enroute contract generated into `lib/gen`.
- An Enroute pointed at a hook URL your app does not serve yet.

Next: [Create repositories](02-create-repositories.md), where your app creates its first
repository over the API.
