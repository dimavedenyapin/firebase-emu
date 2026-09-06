# Firebase emulator in Rust

One loopback-only, in-memory process serves Firestore gRPC and browser
WebChannel (8080), Auth REST (9099), Storage REST (9199), and optional Firebase
Functions (5001). It is intended for local development and tests with `demo-`
projects, not production traffic.

## Install and run

Release archives are built natively for:

- macOS 13+ on Intel (`x86_64-apple-darwin`) and Apple Silicon
  (`aarch64-apple-darwin`)
- glibc-based Linux on x64 (`x86_64-unknown-linux-gnu`) and arm64
  (`aarch64-unknown-linux-gnu`)
- 64-bit Windows (`x86_64-pc-windows-msvc`)

After a `v0.1.0` release is published, Node 18+ users can run its verified
prebuilt asset without installing Rust:

```sh
npx --yes github:dimavedenyapin/firebase-emu#v0.1.0 -- --no-functions
```

The launcher downloads the matching GitHub Release archive, verifies it
against the release SHA-256 manifest, caches it per version and target, and
then forwards arguments without shell interpolation. An npm-registry package
has not been published.

Build from source with stable Rust when developing:

```sh
cargo build --locked --release
GCLOUD_PROJECT=demo-sdk-compat ./target/release/firebase-emu --no-functions
```

`FIREBASE_EMU_HOST` selects a loopback IP. `FIRESTORE_EMU_PORT`,
`FIREBASE_AUTH_EMU_PORT`, and `FIREBASE_STORAGE_EMU_PORT` change the three
service ports. Data is lost when the process stops.

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
