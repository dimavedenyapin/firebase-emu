# Firebase emulator in Rust

One loopback-only process serves Firestore gRPC and browser
WebChannel (8080), Auth REST (9099), Storage REST (9199), and optional Firebase
Functions (5001). It is intended for local development and tests with `demo-`
projects, not production traffic. The backwards-compatible default is
in-memory; `--data-dir` opts into durable local persistence.

## Install and run

Release archives are built and smoke-tested natively for:

- macOS 15 on Intel (`x86_64-apple-darwin`) and Apple Silicon
  (`aarch64-apple-darwin`)
- Ubuntu 24.04 with glibc 2.39 on x64 (`x86_64-unknown-linux-gnu`) and
  arm64 (`aarch64-unknown-linux-gnu`)
- Windows Server 2022 x64 (`x86_64-pc-windows-msvc`)

Those are tested baselines, not claims of compatibility with older operating
system or libc versions.

Merging to `main` (or otherwise pushing a reviewed commit to `main`) starts an
automatic release. The workflow records one synchronized source version, runs
the required Rust/Node CI gates, builds and smoke-tests all five native targets,
verifies the complete bundle, and only then publishes a normal, non-draft
GitHub Release. No manually created tag or follow-up release PR is needed.

The first merge publishes the current `0.1.0` version if `v0.1.0` is unused.
After that, an unchanged or stale source version is advanced from the latest
stable release by one patch (for example, `0.1.0` to `0.1.1`). To intentionally
release a minor or major version, update all six version-bearing manifests in
the same feature PR to a stable version greater than every existing release:
`Cargo.toml`, the `firebase-emu` entry in `Cargo.lock`, the root `package.json`
and root package in `package-lock.json`, plus the equivalent two files under
`functions-runtime/`. Such a forward source version is honored; a version not
greater than the latest release is overridden by the automatic patch policy.

The automatic patch is a normal fast-forward `github-actions[bot]` commit on
`main`, never a force push. It carries a source-commit marker so retries reuse
the same version commit. Default-branch release runs are serialized, stale
runs refuse to overwrite newer work, existing tags must already point to the
exact built commit, and an existing published release must have byte-identical
assets. GitHub's built-in `GITHUB_TOKEN` performs the commit, tag, and release;
the build and publication remain in one workflow graph because a tag pushed by
that token does not start another workflow. Repositories that later protect
`main` must explicitly permit this ordinary version commit or choose a policy
that accepts it; the workflow does not bypass branch protection or require a
hidden PAT.

Once `v0.1.0` is published, Node 18+ users can run its verified prebuilt asset
without installing Rust:

```sh
npx --yes github:dimavedenyapin/firebase-emu#v0.1.0 --no-functions
```

The launcher downloads the matching public GitHub Release archive, verifies it
against the release SHA-256 manifest, caches it per version and target, and
then forwards arguments without shell interpolation. An npm-registry package
has not been published, so `npx firebase-emu-rs` is not currently an npm
installation path; the GitHub URL above is the supported `npx` relationship.

Build from source with stable Rust when developing:

```sh
cargo build --locked --release
GCLOUD_PROJECT=demo-sdk-compat ./target/release/firebase-emu --no-functions
```

`FIREBASE_EMU_HOST` selects a loopback IP. `FIRESTORE_EMU_PORT`,
`FIREBASE_AUTH_EMU_PORT`, and `FIREBASE_STORAGE_EMU_PORT` change the three
service ports.

## Durable local data

No option, or the explicit `--in-memory` option, preserves the historical
ephemeral behavior. Use a dedicated directory to retain acknowledged data:

```sh
firebase-emu --data-dir "./.firebase-emu-data" --no-functions
firebase-emu --data-dir "./local data/firebase" --no-functions
```

These options are newer than `v0.1.0`; use a binary built from this branch until
the next release is published. After that release, the GitHub `npx` launcher
for that tag accepts the same arguments.

`--data-dir` and `--in-memory` conflict and are rejected. Relative data paths
resolve from the process working directory, independently of `--config`; the
startup log prints the canonical SQLite and object paths. The GitHub `npx`
launcher forwards options directly, so do not insert an extra `--`. No custom
field is read from `firebase.json`.

The directory contains `firebase-emu.sqlite3` in SQLite WAL mode, an exclusive
owner lock, opaque object files under `blobs/`, and crash-recovery files under
`tmp/`. SQLite is compiled into every release binary; no database server or
installed SQLite library is required. Firestore protobufs are stored without a
JSON conversion, preserving 64-bit integers, timestamps, bytes, references,
geopoints, nested values, NaN, full resource names, and document timestamps.
Auth users, password material, custom claims, ID/refresh sessions, revocation
state, and Storage metadata are durable. Active Firestore transaction handles,
open HTTP requests, sockets, and incomplete resumable uploads are process-local.

One bounded 128-entry writer queue serializes SQLite transactions on a
dedicated OS thread. Up to eight blocking read connections can run
concurrently; every connection has a 5-second busy timeout and an 8 MiB SQLite
page-cache target. Acknowledged writes use WAL with `synchronous=FULL`.
Persistent queries narrow in SQLite and then reuse the existing Rust query
evaluator, so complex queries remain scans and hold only their transient result
set in memory—there is no unbounded full database mirror or result cache.

Storage finalization flushes a unique temporary blob, renames it to an
immutable opaque UUID name, then commits metadata and its event. Overwrite and
delete cleanup occurs only after that commit. Startup discards incomplete temp
files and unreferenced blobs; a missing referenced blob or corrupt/newer schema
stops startup instead of resetting data. A retained exclusive lock rejects a
second process using the same directory.

Supported Firestore and Storage triggers use a durable outbox when Functions
are configured. Pending/in-flight work is recovered after restart, stable event
IDs are retried up to five times with bounded backoff, and terminal failures are
reported by Functions status/drain. Delivery is at least once: a crash after a
handler succeeds but before its durable acknowledgement can deliver the same
event ID again, and one source event targeting several handlers can repeat the
whole matching group. Exactly-once delivery is not promised. Starting with
`--no-functions` retains existing pending work without delivering it.

The existing Firestore ClearData API durably resets only its requested
project/database and does not emit create events during restart rehydration.
There is no automatic reset, import, or seed. To reset every service, stop the
owner and remove that one explicitly selected data directory. For backup, stop
the emulator before copying the whole directory (database, any `-wal`/`-shm`,
and `blobs/`); copying only a live main database file is not a valid backup.

## Functions runtime

Functions stay disabled unless Firebase configuration or a Functions source is
selected. A release archive includes this layout:

```text
firebase-emu[.exe]
functions-runtime/
  adapter.cjs
  package.json
  package-lock.json
  node_modules/        # production adapter dependencies
```

The binary resolves the adapter relative to its own executable, so it remains
portable when the archive is moved. `FIREBASE_FUNCTIONS_ADAPTER` is an explicit
absolute-path override for custom packaging. Source builds can use
`FIREBASE_FUNCTIONS_ADAPTER="$PWD/functions-runtime/adapter.cjs"`.

Install every Functions application's own locked dependencies, then select a
matching Node 18, 20, or 22 executable with `FIREBASE_FUNCTIONS_NODE_18`, `_20`,
or `_22` (or `FIREBASE_FUNCTIONS_NODE`):

```sh
FIREBASE_FUNCTIONS_NODE_22=/path/to/node \
  firebase-emu --config /path/to/firebase-project --project demo-local
```

The Rust process reads `firebase.json`, `.firebaserc`, dotenv files, and legacy
runtime config; owns public routing and child lifecycle; and starts one private
Node worker per codebase. HTTP/callable endpoints use
`http://127.0.0.1:5001/<project>/<region>/<function>`. Supported v1 background
triggers are Firestore create/update/delete/write, Storage finalize/delete,
Pub/Sub publish, and schedules. Pub/Sub and schedules use local injection APIs;
there is no Pub/Sub service on port 8085. See [FUNCTIONS-CONFIG.md](FUNCTIONS-CONFIG.md).

## Deterministic test setup

All Node workspaces have committed lockfiles. Bootstrap a clean checkout with:

```sh
npm ci --prefix functions-runtime --ignore-scripts
npm ci --prefix compat --ignore-scripts
npm ci --prefix examples/node-app --ignore-scripts
npm ci --prefix examples/web-app --ignore-scripts
npx --prefix functions-runtime playwright install chromium
```

Run bounded checks with:

```sh
cargo fmt --all -- --check
cargo check --all-targets
cargo clippy --all-targets
cargo test --all-targets
npm test --prefix functions-runtime
npm test --prefix examples/node-app
npm test --prefix examples/web-app
```

Every pull request is validated when it is opened, updated, reopened, or marked
ready for review. The `Rust and Functions checks` job checks out the exact PR
head, cancels superseded runs for the same PR, and has a 30-minute timeout. It
runs the locked Rust formatting/check/clippy gates and all Rust targets, then
builds the release binary and exercises real Node/Admin and browser Firebase
SDK traffic, the relocated full Functions runtime, SQLite WAL process restart
and crash recovery, browser restart persistence, and durable Functions outbox
redelivery at the delivery/ack crash boundary. A failure in any suite fails that
single validation job; emulator logs are printed when the SDK gate fails.

The separate `Release binaries` PR workflow retains native build and packaged
smoke coverage for Linux x64/arm64, macOS x64/arm64, and Windows x64. PR runs
have read-only repository permissions and never publish. Release publication is
only requested by the serialized default-branch automatic-release workflow
after its required validation job succeeds.

`PLAYWRIGHT_CHROMIUM_EXECUTABLE` selects an explicit Chromium executable;
otherwise browser tests use Playwright's managed Chromium. The full Functions
test relocates the release binary, adapter, dependencies, and fixtures before
startup so it cannot fall back to the source checkout.

The generic SDK matrix is `./scripts/sdk-compat.sh`. Set
`SDK_COMPAT_INSTALL=1` to install any missing locked dependencies. It supports
isolated ports through the emulator host/port environment variables. The
Google-emulator half additionally needs Java and the pinned Firebase CLI in
`compat/`; the expensive official/browser matrix is also available as a manual
CI workflow.

## Implemented scope

Firestore supports document CRUD, batch reads/writes, scoped reset, nested
update masks, partial merge, numeric increment, array union, field deletion,
server timestamps, filters, ordering, limits, offsets, cursors, projections,
transactions used by the tested SDK, and document/query listeners. Browser
REST and WebChannel share the gRPC store.

Auth supports Admin account operations and browser email/password sign-up,
sign-in, profile operations, token lookup, and refresh. ID sessions expire after
one hour; refresh sessions expire after 30 days. Expired sessions are pruned
opportunistically, and deleting a user invalidates all of that user's sessions.

Storage supports Node GCS and browser upload/download/list/delete, CORS,
CRC32C metadata, and resumable chunks. Abandoned resumable sessions expire
after one hour and are pruned opportunistically.

## Deliberate limits

This emulator does not implement Security Rules, production transaction
isolation, composite-index enforcement, aggregation or partition queries, or
Firestore maximum/minimum/array-remove transforms. Storage does not implement
resumable retry recovery, IAM, or signed-URL verification. Auth is not a full
production identity service. Auth, RTDB, Eventarc, task queue, analytics, and
Functions v2/CloudEvent triggers are not implemented.

The final pre-cleanup validation historically passed 45 Rust tests, a 68-check
SDK matrix, and two separate real-application compatibility suites (18 checks
and 11 checks, respectively). Those are historical results, not claims about
this revision. Current commands and results are recorded in
[compat-sdk/README.md](compat-sdk/README.md).

## Licensing and provenance

Project code is offered under Apache License 2.0. Reduced Google API Protocol
Buffer definitions retain Google copyright and Apache notices; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Firebase and Google product
names are trademarks of their owners and do not imply endorsement.
