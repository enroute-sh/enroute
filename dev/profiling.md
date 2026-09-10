# CPU profiling

How to take sampling CPU profiles of the git server with
[samply](https://github.com/mstange/samply) (`brew install samply` /
`cargo install samply`).

For the _push_ path, use `dev/push-bench` before a profiler. It answers "how
much, and where did the time go in each phase" in one command. See
[Benchmarking the push path](#benchmarking-the-push-path) at the end of this
file. Take a profile once the phase breakdown has told you which phase to open
up.

## Why not just `--release`

A plain `--release` build has no debug info at all, so a sampling profiler can
resolve only the raw symbol names from the symbol table of the binary. There are
no line numbers, and no way to see _inside_ a symbol. The fetch path of Enroute
is small hot functions calling each other constantly — varint and delta decode,
hash comparisons, buffer copies — and the optimizer inlines almost all of them
into their caller. Profile a vanilla release build and the flamegraph shows one
fat frame for whatever the inlining happened to collapse into, and not where the
time actually goes.

The `profiling` Cargo profile in the root `Cargo.toml` fixes this. It uses the
same optimization level as `release`, but it adds full DWARF debug info. That
includes `DW_TAG_inlined_subroutine` records, which let the profiler expand an
inlined call back into its own frame in the call tree, attributed to its real
function and line. `split-debuginfo = "packed"` runs `dsymutil`, so the debug
info ends up in a self-contained `.dSYM` next to the binary instead of scattered
across `target/.../deps/*.o`. Without it, the profile stops symbolicating the
moment somebody cleans those intermediate object files or copies the binary
elsewhere.

Build it with:

```sh
cargo build --profile profiling -p load-test --bin load-test
```

which produces `target/profiling/load-test` and
`target/profiling/load-test.dSYM`.

## Frame pointers

Build with `RUSTFLAGS="-C force-frame-pointers=yes"` set. Only the profiling
build needs the flag, so do not set it globally in `.cargo/config.toml`. When
both are available, the unwinder of samply walks frame pointers rather than
DWARF CFI: it is faster, and it is more robust against the occasional case where
the CFI LLVM generates is imprecise around aggressively optimized leaf
functions.

## Profiling the fetch path

`dev/load-test` is the easiest target. It can stand up a real `enroute` server,
with an in-memory object store and a real Postgres-backed metadata store, and
drive concurrent synthetic `git fetch` traffic against it with
[`rlt`](https://github.com/wfxr/rlt). See
`docs/internals/quality-assurance.md` for the one-time local Postgres setup it
needs.

The server and the load generator need to run as **separate processes**, not
just as separate tasks in one process. samply can attach only to a pid it
launched itself, because `-p <pid>` is Linux-only. So if one process does both
jobs, that one profile mixes the frames of the load generator (`rlt::`,
`client::`) in with the frames of the server (`enroute::`, `enroute_git_*::`).
That is harmless if you already know which is which, but it is avoidable.
`--serve-only` stands up the server, seeds it, prints its URL, and blocks, so it
can be the only thing samply traces:

```sh
RUSTFLAGS="${RUSTFLAGS:-} -C force-frame-pointers=yes" \
    cargo build --profile profiling -p load-test --bin load-test
samply record -- ./target/profiling/load-test --serve-only --commits 5000
```

This prints a line such as `serving http://127.0.0.1:PORT/bench/repo.git`. From
another terminal, drive load against it with a second `load-test` process, which
is not profiled:

```sh
cargo run --profile profiling -p load-test -- --against http://127.0.0.1:PORT/bench/repo.git -c 20 -d 30s
```

Press Ctrl-C on the `samply` process once the load run finishes. That stops the
recording and opens the result in the Firefox Profiler UI.

To profile the real `enroute` binary instead — to see the actual S3 latency
rather than the in-memory store, for example — build and run it the same way,
pointed at real infrastructure:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile profiling --bin enroute
samply record -- ./target/profiling/enroute \
  --config=file:///etc/enroute/enroute.toml
```

Then drive load against it separately, with `git clone` or with `load-test
--against http://...` as above.

## Saving a profile instead of viewing it live

```sh
samply record --save-only --unstable-presymbolicate -o profile.json -- ./target/profiling/load-test ...
samply load profile.json
```

`--unstable-presymbolicate` resolves the symbols, inline frames included, and
embeds them in a `.syms.json` sidecar at record time. So the saved profile stays
viewable even after somebody rebuilds or deletes the binary and the `.dSYM` it
was recorded against.

## Benchmarking the push path

`dev/push-bench` ingests one real packfile and reports what it cost. It drives
`enroute-git-ingest` directly rather than over HTTP, so `enroute` is not in its
rebuild path: edit `enroute-git-ingest`, rebuild, measure again.

```sh
cargo build --release -p push-bench
./target/release/push-bench --from-repo ~/path/to/react --runs 5
```

For a given tip, the first run builds the pack a first push of the `HEAD` of
that checkout would send, and caches it in `~/.cache/enroute-push-bench`, keyed
by the tip so nothing can reuse a stale pack. It needs the same local Postgres
the test suite does (see `docs/internals/quality-assurance.md`). Each run gets
its own scratch schema, because a second push of the same objects would find
them already stored and measure nothing.

### Read the CPU number, not the wall number

The size of the resolve pool comes from `available_parallelism`, so wall time on
a ten-core laptop and wall time on a Lambda with one usable thread are different
measurements of different machines. Total CPU is the work done rather than the
rate it was done at, it barely moves between runs, and where the pool is one
thread wide it is very nearly the wall time too.

The report gives wall time as a tripwire, not as a target: a change that cuts
CPU while serialising the pool shows up as wall time that did not move.

That holds only while nothing waits on I/O. Once a latency model is in front of
a store, or staging is on a real disk, most of the wall clock is waiting. No CPU
count and no instruction count can see that wait, so wall time becomes the
number. The header of the report says which regime the run was in.

### Modelling the storage a push actually runs against

`--storage <none|s3|express>` puts a latency model in front of the permanent
object store. `--staging <disk|memory>` chooses whether staging is a real
directory — as it is on the ingest Lambda, which stages into `/tmp` through
`LocalFileSystem` — or free.

The defaults are `--storage none --staging disk`: no latency where latency has
been measured not to matter, and a real filesystem where it does. The header
reports both, because the two regimes rank on different metrics. Use `--staging
memory` to reproduce a measurement taken before these flags existed.

The `i/o` section counts what each store was asked for. The request counts are
the durable half of this harness. They are a property of the code, so multiply
them by whatever round trip a deployment has and you get the floor for that
deployment, whatever machine the count was taken on.

### What it doesn't measure

- **Postgres CPU**, which is in another process. A change that trades local
  work for more queries looks free here; the report gives the `queries` and
  `lookups` counters alongside, so that trade stays visible.
- **Postgres latency.** The pool talks to a local server. `queries` is the
  statement count, so the cost of a remote one is a multiplication. It does not
  include `COPY FROM STDIN` bodies, because `sqlx` does not log them.
- **A shared pipe.** The latency store in `bench-support` gives every request
  its own throughput, so concurrent transfers never contend for bandwidth. A
  change that wins by uploading more at once looks better here than it is.
- **The client.** The pack is already in memory. A real one arrives over the
  network while the scan reads it.
- **Pool width.** The pool takes every core and there is no knob to pin it, so
  you cannot yet ask "does this still help at one thread?"

### Comparing two branches

`--json` emits the whole report, including the span fields for each phase, for a
caller that is diffing runs rather than reading them. The correctness gate is
built in: every run records how many objects and commits it stored, and runs
that disagree fail rather than report. Compare those counts across branches too
— the cheapest way to make ingestion faster is to do less of it.
