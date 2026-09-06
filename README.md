# Firebase emulator in Rust

One in-memory process serves Firestore gRPC and browser WebChannel on port 8080, Auth REST on port 9099, Storage REST on port 9199, and optional Firebase Functions on port 5001. All public and private runtime listeners bind to loopback addresses. Use a `demo-` project for SDK tests.

## Build and run

```sh
cargo build --release
GCLOUD_PROJECT=demo-sdk-compat ./target/release/firebase-emu
```

The binary is `target/release/firebase-emu`. `FIREBASE_EMU_HOST` selects a loopback IP address. Set `FIRESTORE_EMU_PORT`, `FIREBASE_AUTH_EMU_PORT`, and `FIREBASE_STORAGE_EMU_PORT` to change ports. Data is lost when the process stops.

Functions remain disabled when no Firebase configuration is selected, preserving the original three-service startup. To enable them, run from a directory containing `firebase.json`, pass its project directory (or the file itself), or set `FIREBASE_EMU_CONFIG_DIR`:

```sh
FIREBASE_FUNCTIONS_NODE_22=/path/to/node-22 \
  ./target/release/firebase-emu \
  --config /path/to/peakflo-web \
  --project demo-peakflo
```

The Rust process reads `firebase.json` Functions source/codebase/runtime and emulator ports, validates a local-only `.firebaserc` project, owns public routing and child lifecycle, and starts one private Node process per codebase. JavaScript does not execute natively in Rust. Install each Functions source's locked npm dependencies first. Select matching Node executables with `FIREBASE_FUNCTIONS_NODE_18`, `_20`, or `_22` (or the shared `FIREBASE_FUNCTIONS_NODE`). Node 18, 20, and 22 are supported and runtime mismatches fail startup.

HTTP and callable endpoints use `http://127.0.0.1:5001/<project>/<region>/<function>`. The callable path is handled by the installed Firebase Functions SDK, including callable envelopes, CORS, local Auth context, `HttpsError`, rejected promises, and malformed requests. Plain handlers such as `upload-functions` can be declared in a source-local `.firebase-emu-functions.json`; see `functions-runtime/fixtures/plain`. Set `FIREBASE_FUNCTIONS_TARGET` before startup to load a single target when an application's entry point honors target filtering. This is important for applications with unsafe or production-oriented top-level initialization.

Supported background triggers are Firebase Functions v1 Firestore document create/update/delete/write, Storage finalize/delete, Pub/Sub topic publish, and scheduled Pub/Sub functions. Firestore and Storage mutations dispatch automatically. Pub/Sub and schedules are deliberately local injection APIs rather than a cloud client or production call:

```sh
curl -X POST http://127.0.0.1:5001/__/functions/pubsub/my-topic \
  -H 'content-type: application/json' -d '{"data":{"id":"local"},"attributes":{}}'
curl -X POST http://127.0.0.1:5001/__/functions/schedule/myScheduledFunction
curl http://127.0.0.1:5001/__/functions/drain
```

`drain` waits for the ordered trigger queue and returns failures, which gives tests deterministic completion. A 1,000-event busy-chain guard stops trigger loops. Auth, RTDB, Eventarc, task queue, analytics, and Functions v2/CloudEvent trigger definitions are not implemented. There is no Pub/Sub service emulator on port 8085; use the local injection endpoint. See `FUNCTIONS-CONFIG.md` for precedence, legacy `functions.config()`, dotenv handling, multi-codebase behavior, and secret safeguards.

Run the Functions compatibility checks with an explicit supported Node executable:

```sh
FIREBASE_FUNCTIONS_NODE_22=/path/to/node-22 ./scripts/functions-compat.sh
```

This builds the release binary, runs real Firebase Functions v3/Admin v9/browser SDK protocol tests, then exercises callable/HTTP endpoints and all supported triggers through the Rust binary, including a two-codebase process lifecycle fixture. It also runs the deterministic callable, HTTP, and Firestore trigger fixture against pinned Firebase CLI 14.17.0 as the official baseline. The baseline uses an isolated home and a `demo-` project, so it does not discover local cloud credentials.

The checked-out `peakflo-web/functions` package was inspected but not started. Its compiled `dist` directory and `node_modules` are absent; even a target-limited Node 22 load stops safely at `Cannot find module 'glob'` before application initialization. Reproduce from the parent workspace with `env -i PATH=/path/to/node22/bin FUNCTION_TARGET=onCompanyConfigWrite GCLOUD_PROJECT=demo-peakflo GOOGLE_CLOUD_PROJECT=demo-peakflo FIREBASE_CONFIG='{"projectId":"demo-peakflo"}' FIRESTORE_EMULATOR_HOST=127.0.0.1:18080 FIREBASE_AUTH_EMULATOR_HOST=127.0.0.1:19099 node functions/index.js`. Installing/building that separate application and reviewing its database initialization remains with the paused application test worker; the emulator fixture itself has no skipped checks.

An optional official-emulator baseline for the same callable, HTTP, and Firestore fixture is included. Run with `FIREBASE_FUNCTIONS_OFFICIAL_BASELINE=1` and set `FIREBASE_BIN` when a compatible Firebase CLI and Java runtime are installed. The baseline is comparison-only; the Rust implementation never starts or proxies to the official emulator.

The build uses tonic 0.12 and prost 0.13 with local proto files. The build includes a vendored `protoc`. Auth and Storage use axum 0.8 with tower-http CORS. Firestore browser routes use the axum 0.7 alias required by tonic.

## Strict SDK matrix

Install the application dependencies from their lockfiles once. The checked-in
`compat/` tool directory intentionally has no lockfile and is distributed with
its pinned installation in this workspace.

```sh
npm ci --prefix examples/node-app --ignore-scripts
npm ci --prefix examples/web-app --ignore-scripts
```

Use Java 21 and an installed Playwright Chromium browser. Set `SDK_COMPAT_JAVA_HOME` or `PLAYWRIGHT_CHROMIUM_EXECUTABLE` if needed. The script can use the local Homebrew Java 21 and Playwright browser cache.

```sh
./scripts/sdk-compat.sh
```

This command checks installed SDK versions and lockfiles, runs unit tests, builds the browser app and Rust release, and runs strict Node and actual browser tests against Google and Rust emulators. It starts isolated emulator processes on ports 8080, 9099, and 9199. Keep these ports free before the run. No production service is used.

Every required check must be present once and pass. Missing, duplicate, failed, or blocked checks cause a nonzero exit. `target/sdk-compat/summary.json` contains command results and individual checks. The runner also keeps logs in that directory.

| SDK | Required version |
| --- | --- |
| Firebase Admin Node | 11.11.1 |
| Direct Google Cloud Firestore | 7.11.6 |
| Direct Google Cloud Storage | 7.7.0 |
| Node Peakflo schema | GitHub v5.11.62, commit `03a381c70f841b731d95adf01fb63b3c3b13cd24` |
| Firebase browser SDK | 12.12.1 |
| React and React DOM | 16.14.0 |
| Browser Peakflo schema | 4.11.58 |

The Node app imports `peakflo-schema` from the GitHub package. The browser app uses the published `@peakflo/peakflo-schema` package and its browser-safe enum module. These versions match the supplied `upload-functions` reference at `edeab8d9b6dae3a01077640e24c2178114c43c9d` and `peakflo-web` reference at `33390be512c6f4c531d5e10ec7bdf92fbe217523`.

## Supported test scope

Firestore supports document CRUD, batch reads and writes, nested update masks, partial merge, numeric increment, array union, server timestamps, filters, ordering, limits, offsets, cursors, scoped reset, and live listeners. Browser REST and WebChannel share the gRPC document store. The SDK matrix uses `@google-cloud/firestore` 7.11.6 to check transforms together with deletion and nested update-mask preservation, as well as queries and a listener update after the first response. The browser listener receives an update from a separate Firebase client.

Auth supports Admin account operations and browser email/password sign-up, sign-in, sign-out, token lookup, and refresh. Browser JWT issuer and audience claims use the selected CLI/configured project, while project-scoped Admin routes keep their URL project namespace. Storage supports Node GCS operations and browser upload, download URL, download, list, and delete with CORS and CRC32C metadata.

## Limits

This is a local test service. It does not implement Security Rules, production transaction isolation, composite index enforcement, Firestore maximum/minimum/array-remove transforms, aggregation queries, or partition queries. Unsupported transforms return an error. Storage does not implement retryable multi-chunk append, IAM, or signed URL verification. Auth does not reproduce the full production identity service. These limits are outside the required SDK matrix; no required check is skipped.

See `compat-sdk/README.md` for the exact verified results and binary checksum. The supplied production application test suite was not available. The verified scope is the pinned Node and React examples.
