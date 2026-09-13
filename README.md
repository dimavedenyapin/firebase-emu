# FireRust

<p align="center">
  <img src="docs/images/firerust-logo.png" width="320" alt="FireRust flame and crab logo">
</p>

FireRust is a local Firebase emulator for development and automated tests. One
Rust process serves Firestore, Auth, Storage, Pub/Sub, and a browser console. An
optional Node worker runs Firebase Functions. You can keep local data in memory
or save it in SQLite.

FireRust is the user-facing project name. The executable and command remain
`firebase-emu` for compatibility. The repository, package, crate, environment
variables, data formats, protocol identifiers, and release URLs also keep their
existing names.

**Public beta:** FireRust is an independent project. It is not affiliated with
Google. It does not implement Security Rules or production transaction
isolation. Use synthetic data and `demo-` projects. A successful local test
does not prove production compatibility.

Lower memory use is an important goal. In a historical v0.1.3 comparison, the
idle process-tree RSS was **68.1–68.2 MB** for the Rust emulator and
**735.3–781.7 MB** for the official suite. The Rust test used SQLite storage.
The official test used memory storage. Summed RSS can count shared pages more
than once. These results do not describe startup time, speed, or the current
release. Read the [benchmark method and limits](docs/BENCHMARKS.md).

[Compatibility](docs/COMPATIBILITY.md) · [Benchmarks](docs/BENCHMARKS.md) ·
[Minimal example](examples/quickstart/README.md) ·
[Troubleshooting](docs/TROUBLESHOOTING.md) · [Contributing](CONTRIBUTING.md) ·
[Security](SECURITY.md) ·
[Releases](https://github.com/dimavedenyapin/firebase-emu/releases)

## Quickstart

The current published release is v0.1.5. Run it with Node 22. You do not need
Rust.

```sh
npx --yes github:dimavedenyapin/firebase-emu#v0.1.5 --project demo-local --ui-port 0 --no-functions
```

Open the loopback URL that FireRust prints at startup. Use synthetic data only.
Then run the [minimal example](examples/quickstart/README.md).

The FireRust rebrand is newer than v0.1.5. The published v0.1.5 console can show
the earlier name. This source revision does not replace or republish v0.1.5.

## Install and run

Release archives have native smoke tests for these systems:

- macOS 15 on Intel (`x86_64-apple-darwin`)
- macOS 15 on Apple Silicon (`aarch64-apple-darwin`)
- Ubuntu 24.04 with glibc 2.39 on x64
  (`x86_64-unknown-linux-gnu`)
- Ubuntu 24.04 with glibc 2.39 on arm64
  (`aarch64-unknown-linux-gnu`)
- Windows Server 2022 x64 (`x86_64-pc-windows-msvc`)

These systems are the tested baselines. Compatibility with older operating
systems or libc versions is not guaranteed.

Use the [Quickstart](#quickstart) command to run the published binary. The
launcher downloads the matching public GitHub Release archive. It verifies
the archive against the release SHA-256 manifest. It caches the archive by
version and target. It then forwards the command arguments without shell
interpolation.

There is no published npm-registry package. Therefore,
`npx firebase-emu-rs` is not a supported installation command. Use the GitHub
URL in the command above.

Use stable Rust to build from source:

```sh
cargo build --locked --release
GCLOUD_PROJECT=demo-sdk-compat ./target/release/firebase-emu --no-functions
```

Use these environment variables to change the listeners:

- `FIREBASE_EMU_HOST` selects a loopback IP address.
- `FIRESTORE_EMU_PORT` changes the Firestore port.
- `FIREBASE_AUTH_EMU_PORT` changes the Auth port.
- `FIREBASE_STORAGE_EMU_PORT` changes the Storage port.
- `PUBSUB_EMULATOR_PORT` changes the Pub/Sub port.
- `FIREBASE_UI_EMU_PORT` changes the console port.

You can also use `--ui-port` and `--pubsub-port`. Use `--ui-port 0` to
select a free loopback port. If a selected nonzero UI port is in use, FireRust
selects a free port and prints the new URL. Use `--no-ui` to disable the
console.

You can set the Pub/Sub and console ports in `firebase.json`:

```json
{
  "emulators": {
    "pubsub": { "host": "127.0.0.1", "port": 8085 },
    "ui": { "host": "127.0.0.1", "port": 4000 }
  }
}
```

## FireRust console

If you use `--ui-port 0`, open the URL that FireRust prints. The default URL
is `http://127.0.0.1:4000`.

The console is embedded in the executable. It connects only to loopback
services in the same process. It does not discover cloud credentials or
production endpoints.

The project selector controls the Auth, Firestore, and Pub/Sub views. The
Firestore view also supports named databases.

![Auth console with a synthetic local user](docs/images/console-auth.png)

![Firestore console with synthetic typed fields](docs/images/console-firestore.png)

![Pub/Sub console with a separate synthetic topic](docs/images/console-pubsub.png)

<p align="center">
  <img src="docs/images/console-mobile.png" width="390" alt="FireRust Pub/Sub console at a mobile viewport width">
</p>

These screenshots use synthetic local data and FireRust source. They do not
show published v0.1.5. The Pub/Sub screenshot uses a separate synthetic topic.
The quickstart seed does not create a topic.

The Auth view lists users. It shows the complete local user record and parsed
custom claims.

The Firestore view has collection, document, and field columns. Breadcrumbs
show the current path. You can open a nested collection from its parent
document. You can edit values inline. Each fixed Firestore type appears below
its field name. An invalid value does not change the document.

The Pub/Sub view lists topics, topic settings, and subscriptions. It reads
broker metadata only. It does not pull, acknowledge, reject, or change queued
messages.

**Copy object** copies the document `fields` map as Firestore REST Value JSON
v1. Each value has an explicit type wrapper. Examples include
`integerValue`, `timestampValue`, `bytesValue`, `referenceValue`,
`geoPointValue`, `arrayValue`, and `mapValue`. This format preserves types
that plain JSON cannot safely represent.

**Clone document** requires a destination collection path and document ID. The
operation is atomic. It preserves Firestore field types. It uses a
must-not-exist precondition and cannot overwrite an existing document.
Subcollections are not included by default. Select the separate checkbox to
include them. Import from an external object source is not in the beta scope.

## Durable local data

FireRust uses memory storage by default. The explicit `--in-memory` option has
the same behavior.

Use `--data-dir` to keep acknowledged data:

```sh
firebase-emu --data-dir "./.firebase-emu-data" --no-functions
firebase-emu --data-dir "./local data/firebase" --no-functions
```

Persistence is available in v0.1.5. The GitHub launcher accepts these
arguments.

Do not use `--data-dir` with `--in-memory`. FireRust rejects this
combination. A relative data path starts at the process working directory. It
does not start at the `--config` directory. The startup log prints the
canonical SQLite and object paths.

Do not add an extra `--` to the GitHub `npx` command. The launcher forwards
options directly. FireRust does not read a custom data-directory field from
`firebase.json`.

The data directory contains these items:

- `firebase-emu.sqlite3` in SQLite WAL mode
- an exclusive owner lock
- opaque object files in `blobs/`
- crash-recovery files in `tmp/`

SQLite is compiled into each release binary. You do not need a database server
or an installed SQLite library.

Firestore protobufs are stored without JSON conversion. This preserves 64-bit
integers, timestamps, bytes, references, geopoints, nested values, NaN values,
full resource names, and document timestamps.

FireRust stores Auth users, password material, custom claims, ID and refresh
sessions, revocation state, and Storage metadata. It also stores Pub/Sub
topics, subscriptions, messages, delivery leases, and acknowledgements.

Active Firestore transaction handles remain in the process. Open HTTP
requests, sockets, and incomplete resumable uploads also remain in the
process. FireRust migrates a schema-v1 directory to schema v2 in one
transaction. The migration keeps existing Firestore, Auth, Storage, and
Functions outbox data.

A bounded 128-entry writer queue serializes SQLite transactions on one
dedicated operating-system thread. Up to eight read connections can run at
the same time. Each connection has a 5-second busy timeout and an 8 MiB page
cache target. Acknowledged writes use WAL with `synchronous=FULL`.

Persistent queries first narrow the result in SQLite. They then use the Rust
query evaluator. Complex queries remain scans. FireRust keeps only the
temporary result set in memory. It does not keep an unbounded database mirror
or result cache.

Storage finalization flushes a unique temporary blob. It renames the blob to an
opaque UUID name. It then commits the metadata and event. Overwrite and delete
cleanup starts only after this commit.

At startup, FireRust removes incomplete temporary files and unreferenced
blobs. A missing referenced blob stops startup. A corrupt or newer schema also
stops startup. FireRust does not reset the data. The exclusive lock prevents a
second process from using the same directory.

Supported Firestore and Storage triggers use a durable outbox when Functions
are configured. FireRust recovers pending and in-flight work after restart. It
retries each stable event ID up to five times with bounded backoff. Functions
status and drain report terminal failures.

FireRust immediately retries a failed durable state transition. It can reclaim
an expired five-second delivery lease without a restart. It checks direct
Pub/Sub and schedule work after each burst of 32 or fewer durable deliveries.
It combines queued wake notifications at this boundary. A 10 ms pause between
full bursts limits the sustained dispatch rate without dropping events.

A self-triggering function remains pending and rate-limited. It stays in this
state until you remove the cause or stop the process. Drain cannot complete
while the function continues.

Delivery is at least once. A crash can occur after a handler succeeds but
before the durable acknowledgement. FireRust can then deliver the same event
ID again. If one source event targets several handlers, FireRust can repeat the
complete matching group. FireRust does not promise exactly-once delivery.
`--no-functions` keeps pending work but does not deliver it.

The Firestore ClearData API resets only the requested project and database. It
does not create events during restart recovery. FireRust does not reset,
import, or seed data automatically.

To reset all services, stop the process. Then remove only the data directory
that you selected. To make a backup, first stop the emulator. Copy the complete
directory, including the database, `-wal` and `-shm` files, and `blobs/`.
Do not copy only a live main database file.

## Functions runtime

Functions remain disabled until you select Firebase configuration or a
Functions source.

A release archive has this layout:

```text
firebase-emu[.exe]
functions-runtime/
  adapter.cjs
  package.json
  package-lock.json
  node_modules/        # production adapter dependencies
```

The binary finds the adapter relative to its executable. You can move the
extracted archive. Set `FIREBASE_FUNCTIONS_ADAPTER` to an absolute path only
when you use custom packaging.

A source build can use this setting:

```sh
FIREBASE_FUNCTIONS_ADAPTER="$PWD/functions-runtime/adapter.cjs"
```

Install the locked dependencies for each Functions application. Then select a
Node 18, 20, or 22 executable. Use `FIREBASE_FUNCTIONS_NODE_18`,
`FIREBASE_FUNCTIONS_NODE_20`, or `FIREBASE_FUNCTIONS_NODE_22`. You can also
use `FIREBASE_FUNCTIONS_NODE`.

```sh
FIREBASE_FUNCTIONS_NODE_22=/path/to/node \
  firebase-emu --config /path/to/firebase-project --project demo-local
```

The Rust process reads `firebase.json`, `.firebaserc`, dotenv files, and
legacy runtime configuration. It owns public routing and child-process
lifecycle. It starts one private Node worker for each codebase.

HTTP and callable endpoints use this format:
`http://127.0.0.1:5001/<project>/<region>/<function>`.

FireRust supports these v1 background triggers:

- Firestore create, update, delete, and write
- Storage finalize and delete
- Pub/Sub publish
- schedules

At startup, FireRust registers each v1
`functions.pubsub.topic(...).onPublish(...)` export. Each export gets a local
topic and an independent durable subscription. Registration completes before
the Functions-ready log appears.

SDK publishes flow through the subscription. FireRust acknowledges a handler
only after it succeeds. It delivers the message again after a failure.

The explicit HTTP injection route remains available for compatibility. It
calls handlers directly. One request cannot use both routes, so it cannot
cause two deliveries. Read [FUNCTIONS-CONFIG.md](FUNCTIONS-CONFIG.md) and
[PUBSUB.md](PUBSUB.md).

## Deterministic test setup

All Node workspaces have committed lockfiles. Prepare a clean checkout:

```sh
npm ci --prefix functions-runtime --ignore-scripts
npm ci --prefix compat --ignore-scripts
npm ci --prefix examples/node-app --ignore-scripts
npm ci --prefix examples/web-app --ignore-scripts
npx --prefix functions-runtime playwright install chromium
```

Run the bounded checks:

```sh
cargo fmt --all -- --check
cargo check --all-targets
cargo clippy --all-targets
cargo test --all-targets
npm test --prefix functions-runtime
npm run test:pubsub --prefix functions-runtime
npm test --prefix examples/node-app
npm test --prefix examples/web-app
```

Pull request validation uses the exact pull request head. It starts when you
open, update, reopen, or mark a pull request ready for review. It cancels an
older run for the same pull request. The job has a 30-minute limit.

The `Rust and Functions checks` job runs the locked Rust checks and all Rust
test targets. It builds the release binary. It also tests these functions:

- real Node Admin SDK traffic
- real browser Firebase SDK traffic
- the relocated Functions runtime
- SQLite WAL restart and crash recovery
- browser restart persistence
- durable Functions outbox redelivery at the delivery and acknowledgement
  crash boundary

One failed suite fails the validation job. If the SDK gate fails, the job
prints the emulator logs.

The `Release binaries` workflow builds and tests native packages for Linux
x64 and arm64, macOS x64 and arm64, and Windows x64. It does not run for pull
requests. A maintainer can run it manually. The automatic release workflow can
also call it after the required validation job succeeds.

Set `PLAYWRIGHT_CHROMIUM_EXECUTABLE` to use a specified Chromium executable.
If you do not set it, the browser tests use Playwright Chromium. The Functions
test moves the release binary, adapter, dependencies, and fixtures before
startup. This test prevents fallback to the source checkout.

Run the generic SDK matrix with `./scripts/sdk-compat.sh`. Set
`SDK_COMPAT_INSTALL=1` to install missing locked dependencies. Use the
emulator host and port environment variables to select isolated ports.

The Google-emulator half also requires Java and the pinned Firebase CLI in
`compat/`. You can run the official and browser matrix with the manual CI
workflow.

## Implemented scope

### Firestore

Firestore supports document create, read, update, and delete. It supports
batch reads and writes, scoped reset, nested update masks, partial merge,
numeric increment, array union, field deletion, and server timestamps.

Firestore also supports filters, ordering, limits, offsets, cursors,
projections, transactions used by the tested SDK, and document and query
listeners. Browser REST and WebChannel use the same gRPC store.

### Auth

Auth supports Admin account operations. It supports browser email and password
sign-up, sign-in, profile operations, token lookup, and refresh.

ID sessions expire after one hour. Refresh sessions expire after 30 days.
FireRust removes expired sessions during normal work. Deleting a user
invalidates all sessions for that user.

### Storage

Storage supports Node GCS and browser upload, download, list, and delete. It
also supports CORS, CRC32C metadata, and resumable chunks.

An abandoned resumable session expires after one hour. FireRust removes
expired sessions during normal work.

### Pub/Sub

Pub/Sub supports the real `google.pubsub.v1.Publisher` and
`google.pubsub.v1.Subscriber` gRPC APIs. It supports topic and subscription
create, read, update, delete, and list operations. It also supports publish,
unary Pull, StreamingPull, acknowledge, deadline extension, nack and
redelivery, per-subscription fanout, and restart recovery.

The tested Node clients are `@google-cloud/pubsub` 4.11.0 from the current
`upload-functions` lock and 2.19.4 from the current
`peakflo-web/functions` lock.

Set `PUBSUB_EMULATOR_HOST=127.0.0.1:8085`. FireRust does not use credentials
or production endpoints. Read [PUBSUB.md](PUBSUB.md) for an SDK example and
the limits.

## Deliberate limits

Firestore does not implement Security Rules, production transaction isolation,
composite-index enforcement, aggregation queries, partition queries, or
maximum, minimum, and array-remove transforms.

Storage does not implement resumable retry recovery, IAM, or signed-URL
verification. Auth is not a complete production identity service.

Pub/Sub does not implement push delivery, IAM, schemas, filters, snapshots,
seek, dead-letter policies, ordering guarantees, exactly-once delivery, or
Functions v2 and CloudEvent Pub/Sub triggers. FireRust rejects unsupported
Pub/Sub configuration. It does not silently discard it.

FireRust does not implement RTDB, Eventarc, task queues, or analytics.

The final pre-cleanup validation historically passed 45 Rust tests, a 68-check
SDK matrix, and two real-application compatibility suites with 18 and 11
checks. These results do not apply automatically to this revision. Read
[compat-sdk/README.md](compat-sdk/README.md) for current commands and recorded
results.

## License and product names

Project code is available under the Apache License 2.0. Reduced Google API
Protocol Buffer definitions keep the Google copyright and Apache notices.
Read [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Firebase and Google product names are trademarks of their owners. Their use
does not show endorsement.

## Support

[Report a bug](https://github.com/dimavedenyapin/firebase-emu/issues/new/choose).
Include the version, operating system, SDK version, and a small synthetic
reproduction. Read the [release policy](docs/RELEASING.md) and
[changelog](CHANGELOG.md).
