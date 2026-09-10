# Chapter 2: Create repositories

Your app owns names, users, and permissions. Enroute owns storage. This
chapter builds the database that holds the first of those, and creates a
repository in both systems at once.

At the end of this chapter, the application can create a repository. Git
access is added in later chapters.

## Use the Postgres you already have

The quickstart stack runs Postgres for Enroute's own metadata. Your app needs
a database too, and that container can hold both.

Give your app a database of its own rather than tables inside Enroute's.
Enroute owns its schema and migrates it on startup; your app owns yours. They
share a server and nothing else, and the two never join.

Expose the port, because your app runs on the host rather than inside the
compose project. Add `ports` to the `postgres` service in `compose.yaml`:

```yaml
  postgres:
    image: postgres:17-alpine
    ports: ["5432:5432"]
```

```sh
docker compose up -d
docker compose exec -T postgres createdb -U enroute codehost
```

Record the URL in `.env.local`, which Next.js loads automatically:

```sh
DATABASE_URL=postgres://enroute:enroute@127.0.0.1:5432/codehost
```

> **Note:** This guide uses Postgres because it is already running. Nothing in
> your app depends on it. This section uses ordinary Drizzle, so swapping the
> driver for SQLite or libSQL changes the two files in this section and no other
> line in the guide.

## The schema

```sh
npm install drizzle-orm pg
npm install --save-dev drizzle-kit tsx @types/pg
```

Create `lib/db/schema.ts`:

```ts
import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

export const users = pgTable("users", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  token: text("token").notNull().unique(),
});

export const repos = pgTable("repos", {
  // This id is also the key Enroute knows the repository by. There is no
  // Enroute id to store beside your own, so this column is both.
  id: text("id").primaryKey(),
  path: text("path").notNull().unique(),
  ownerId: text("owner_id").notNull().references(() => users.id),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});
```

Two columns hold repository data that Enroute does not store: `repos.path`, which is
what a repository is called, and `users`, which is who anybody is.

**The primary key is the Enroute key.** This is the point of the contract:
Enroute stores no identifier of its own, so the row ID you already allocate
*is* the name every later call uses. There is nothing to keep in step.

**Never key on `path`.** A key is what a repository *is* to Enroute. If the key
were `acme/widgets` and somebody renamed the project, every push would go to a
different repository. `path` is a name, and names change; `id` is an identity,
and it does not.

A key is 1 to 256 bytes of ASCII letters, digits, `-`, `_`, and `.`, starting
and ending with a letter or digit. It holds no `/`, so it is one path segment
that needs no encoding. Your Git URLs can still nest to any depth: chapter 4 is
where a URL becomes a key. See
[Repository keys](../reference/the-contract.md#repository-keys).

## The connection

Create `lib/db/index.ts`:

```ts
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import * as schema from "./schema";

// Next.js reloads modules on every edit in development, so keep one pool on
// the global rather than opening a new one per reload.
const globalForDb = globalThis as unknown as { pool?: Pool };

const pool =
  globalForDb.pool ?? new Pool({ connectionString: process.env.DATABASE_URL });

if (process.env.NODE_ENV !== "production") globalForDb.pool = pool;

export const db = drizzle(pool, { schema });
```

Create `drizzle.config.ts` and push the schema:

```ts
import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./lib/db/schema.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: { url: process.env.DATABASE_URL! },
});
```

```sh
npx drizzle-kit push
```

`push` is right while the schema is still moving. Switch to generated
migrations before anything you care about is in there.

## The queries

Create `lib/store.ts`. Every later chapter reads your app's data through this
file:

```ts
import { eq } from "drizzle-orm";
import { db } from "./db";
import { repos, users } from "./db/schema";

export function userByToken(token: string) {
  return db.query.users.findFirst({ where: eq(users.token, token) });
}

export function repoByPath(path: string) {
  return db.query.repos.findFirst({ where: eq(repos.path, path) });
}

export function repoById(id: string) {
  return db.query.repos.findFirst({ where: eq(repos.id, id) });
}
```

## The client

Create `lib/enroute.ts`:

```ts
import { credentials, Metadata } from "@grpc/grpc-js";
import { RepositoryServiceClient } from "./gen/enroute/api/v1alpha1/repository";

const ADDRESS = process.env.ENROUTE_API ?? "127.0.0.1:50051";

// Every API call names its tenant. On a laptop you set the header yourself.
// A deployment puts a proxy in front that authenticates the caller and sets
// it, because a caller that can set this header is every tenant.
function tenant() {
  const metadata = new Metadata();
  metadata.set("x-enroute-tenant", process.env.ENROUTE_TENANT ?? "dev");
  return metadata;
}

const repositories = new RepositoryServiceClient(
  ADDRESS,
  credentials.createInsecure(),
);

export function createRepository(key: string) {
  return new Promise<void>((resolve, reject) => {
    repositories.createRepository(
      { repo: { key }, defaultBranch: "" },
      tenant(),
      (err) => (err ? reject(err) : resolve()),
    );
  });
}
```

The API is plain HTTP/2 with no TLS and no authentication of its own. That is
correct on a laptop and wrong in a deployment, where the listener must be
private: anything that can reach it and set the header is every tenant. See
[Protect the API listener](../operate/security.md#protect-the-api-listener).

## Create a repository

Your app creates a repository when a person asks for one. Add
`app/api/repos/route.ts`:

```ts
import { randomUUID } from "node:crypto";
import { db } from "@/lib/db";
import { repos } from "@/lib/db/schema";
import { createRepository } from "@/lib/enroute";
import { userByToken } from "@/lib/store";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const token = request.headers.get("authorization")?.replace(/^Bearer /, "");
  const user = token ? await userByToken(token) : undefined;
  if (!user) return new Response(null, { status: 401 });

  const { path } = await request.json();

  // Allocate the id first: it is the key, and Enroute has to be told it
  // before the row that claims it exists.
  const id = `repo-${randomUUID().slice(0, 8)}`;
  await createRepository(id);
  await db.insert(repos).values({ id, path, ownerId: user.id });

  return Response.json({ path, key: id });
}
```

The order matters, and only one of the two calls can be retried safely.
`CreateRepository` is idempotent on the key, so calling it twice answers with
the same repository and makes nothing new. The insert is not. Creating in
Enroute first means a crash between the two leaves an empty repository nobody
references, which the next attempt can reuse or a sweep can collect. The other
order leaves a row pointing at a repository that does not exist, which every
later clone fails on.

## Seed the data

Create `scripts/seed.ts`:

```ts
import { db } from "../lib/db";
import { repos, users } from "../lib/db/schema";
import { createRepository } from "../lib/enroute";

const seedUsers = [
  { id: "user-8471", name: "ada", token: "tok_ada" },
  { id: "user-9002", name: "linus", token: "tok_linus" },
];

const seedRepos = [
  { id: "repo-4f2a1c", path: "acme/widgets", ownerId: "user-8471" },
];

await db.insert(users).values(seedUsers).onConflictDoNothing();

for (const repo of seedRepos) {
  await createRepository(repo.id);          // idempotent on the key
  await db.insert(repos).values(repo).onConflictDoNothing();
  console.log("ensured", repo.id, repo.path);
}

process.exit(0);
```

```sh
npx tsx scripts/seed.ts
```

Run it as often as you like. Every step is idempotent, so it reconciles rather
than duplicates, and it needs no record of what it did last time.

## Check your work

The row and the repository both exist. Ask your database:

```sh
docker compose exec -T postgres psql -U enroute -d codehost \
  -c 'select id, path from repos'
```

Then ask Enroute for the same repository, by the ID that row carries:

```sh
grpcurl -plaintext -H 'x-enroute-tenant: dev' \
  -d '{"repo": {"key": "repo-4f2a1c"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RefService/ListRefs
```

An empty answer is success: the repository exists and holds no refs. An error
means the create did not land.

The route works too. With `npm run dev` running:

```sh
curl -X POST http://127.0.0.1:3000/api/repos \
  -H 'authorization: Bearer tok_ada' \
  -H 'content-type: application/json' \
  -d '{"path": "acme/gadgets"}'
```

It answers with the path and the key it allocated.

## Result

- A database your app owns, beside the one Enroute owns.
- A `repos` table whose primary key is the key Enroute knows.
- A create that reaches both systems, in the order that fails safely.

A `git clone` still fails, because Enroute does not yet know who is allowed to
do that. Answering is the job of the endpoint.

Next: [Handle hooks](03-handle-hooks.md).
