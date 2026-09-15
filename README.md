# FireRust

<p align="center">
  <img src="docs/images/firerust-logo.png" width="320" alt="FireRust flame and crab logo">
</p>

FireRust is a local Firebase emulator for development and tests. One Rust
process serves Firestore, Auth, Storage, Pub/Sub, and a browser console. An
optional Node worker runs Firebase Functions.

Lower memory use is a primary goal. A historical v0.1.3 test measured
68.1–68.2 MB of idle process-tree RSS for the Rust emulator. The official suite
used 735.3–781.7 MB in the same test. Read the [method and
limits](docs/BENCHMARKS.md) before you compare these values.

FireRust is the user-facing project name. The executable and command remain
`firebase-emu` for compatibility. The repository, package, crate, environment
variables, data formats, protocols, and release URLs also keep their existing
names.

FireRust is an independent public beta. It is not affiliated with Google. Use
synthetic data and `demo-` projects only.

## Quickstart

The latest verified release is
[v0.1.5](https://github.com/dimavedenyapin/firebase-emu/releases/tag/v0.1.5).
It was published at 2026-09-13 06:44:18 UTC from commit
[`016f9fd`](https://github.com/dimavedenyapin/firebase-emu/commit/016f9fd7599c5a9fc2461c3e8d0ed5d040d35d17).
Use Node 22 for this path.

1. Start the emulator. You do not need Rust.

   ```sh
   npx --yes github:dimavedenyapin/firebase-emu#v0.1.5 --project demo-local --ui-port 0 --no-functions
   ```

2. In a second terminal, seed synthetic example data from this repository.

   ```sh
   python3 examples/quickstart/seed.py
   ```

3. Open the loopback console URL from the startup output.

4. Follow the [minimal example](examples/quickstart/README.md) for the expected
   user and document.

The FireRust rebrand is newer than v0.1.5. The published v0.1.5 console can
show the earlier name. This documentation change does not replace or republish
that release.

## Services, support, and limits

| Area | Beta support | Main limits | Details |
| --- | --- | --- | --- |
| Firestore | CRUD, batches, queries, selected transforms, transactions, listeners | No Security Rules, production isolation, composite indexes, aggregation, or partition queries | [Compatibility](docs/COMPATIBILITY.md), [notes](FIRESTORE-NOTES.md) |
| Auth | Admin users, email/password, profiles, claims, refresh | Not a production identity service | [Compatibility](docs/COMPATIBILITY.md) |
| Storage | Node and browser CRUD, resumable chunks, CRC32C | No IAM, signed-URL verification, or resumable retry recovery | [Compatibility](docs/COMPATIBILITY.md), [notes](STORAGE-NOTES.md) |
| Pub/Sub | Topic and subscription CRUD, publish, pull, streaming, ACK, nack, restart | No push, filters, ordering, or exactly-once delivery | [Pub/Sub](PUBSUB.md) |
| Functions | HTTP, callable, and supported v1 background triggers | Node worker required; no v2 Pub/Sub CloudEvents | [Functions](FUNCTIONS-CONFIG.md) |
| Console | Auth, typed Firestore, clone and copy, Pub/Sub inspection | Loopback only; no external object import | [Console](docs/CONSOLE.md) |
| Persistence | SQLite WAL, disk objects, durable event recovery | One directory owner; at-least-once events | [Persistence](docs/PERSISTENCE.md) |

FireRust does not implement RTDB, Eventarc, task queues, or analytics. Read the
full [compatibility table](docs/COMPATIBILITY.md) before you migrate tests.

## Install and run

Release archives have native smoke tests for these baselines:

| Operating system | Architecture | Rust target |
| --- | --- | --- |
| macOS 15 | Intel | `x86_64-apple-darwin` |
| macOS 15 | Apple Silicon | `aarch64-apple-darwin` |
| Ubuntu 24.04, glibc 2.39 | x64 | `x86_64-unknown-linux-gnu` |
| Ubuntu 24.04, glibc 2.39 | arm64 | `aarch64-unknown-linux-gnu` |
| Windows Server 2022 | x64 | `x86_64-pc-windows-msvc` |

Older operating systems and libc versions are not verified.

The GitHub launcher downloads the correct archive. It verifies the archive
against the release SHA-256 manifest and caches it by version and target.
There is no npm-registry package. Use the GitHub URL in the
[Quickstart](#quickstart) command. Do not use `npx firebase-emu-rs`.

Build from source with stable Rust:

```sh
cargo build --locked --release
GCLOUD_PROJECT=demo-sdk-compat ./target/release/firebase-emu --no-functions
```

Use these variables to change loopback listeners:

- `FIREBASE_EMU_HOST`
- `FIRESTORE_EMU_PORT`
- `FIREBASE_AUTH_EMU_PORT`
- `FIREBASE_STORAGE_EMU_PORT`
- `PUBSUB_EMULATOR_PORT`
- `FIREBASE_UI_EMU_PORT`

You can also set normal emulator ports in `firebase.json`. Use
`--ui-port 0` to select a free console port. Use `--no-ui` to disable the
console. Use `--pubsub-port` to override the Pub/Sub port.

The default ports are Functions 5001, Firestore 8080, Auth 9099, Storage 9199,
Pub/Sub 8085, and console 4000.

## Historical memory measurement

These values are historical v0.1.3 results. They are not current-version
benchmarks.

| Idle process-tree RSS | Rust emulator | Official suite |
| --- | ---: | ---: |
| Range across runs | **68.1–68.2 MB** | **735.3–781.7 MB** |

The Rust runs used an empty SQLite data directory. The official runs used
memory storage. The process tree included child workers and excluded the load
driver. Summed RSS can count shared pages more than once. This comparison does
not measure startup time or speed. See [all conditions and evidence
limits](docs/BENCHMARKS.md).

## FireRust console

The console is embedded in the executable. It connects only to services in the
same loopback process. The project selector controls Auth, Firestore, and
Pub/Sub. Firestore also supports named databases.

![Firestore console with synthetic typed fields](docs/images/console-firestore.png)

<p align="center">
  <img src="docs/images/console-mobile.png" width="390" alt="FireRust Pub/Sub console at a mobile viewport width">
</p>

The screenshots use synthetic data and newer FireRust source. They do not show
published v0.1.5. Read the [console guide](docs/CONSOLE.md) for field editing,
copy, clone, and non-consuming Pub/Sub inspection.

## Keep local data

- Memory storage is the default. `--in-memory` selects it explicitly.
- Use `firebase-emu --data-dir "./.firebase-emu-data" --no-functions` to keep
  acknowledged data.
- Do not use `--data-dir` with `--in-memory`.
- Stop FireRust before you copy or remove a selected data directory.
- Back up the complete directory. Do not copy only the live SQLite file.

The GitHub launcher accepts these options without an extra `--`. Read
[persistence and recovery](docs/PERSISTENCE.md) for paths, locks, storage,
outbox delivery, reset, and backup rules.

## Run Functions

- Install each Functions application's locked dependencies.
- Select Node 18, 20, or 22 with `FIREBASE_FUNCTIONS_NODE_18`,
  `FIREBASE_FUNCTIONS_NODE_20`, or `FIREBASE_FUNCTIONS_NODE_22`.
- You can use `FIREBASE_FUNCTIONS_NODE` as a general override.
- Pass `--config` or a Functions source. `--no-functions` disables workers.
- Use `FIREBASE_FUNCTIONS_ADAPTER` only for a custom adapter path.

```sh
FIREBASE_FUNCTIONS_NODE_22=/path/to/node \
  firebase-emu --config /path/to/firebase-project --project demo-local
```

HTTP and callable endpoints use
`http://127.0.0.1:5001/<project>/<region>/<function>`. Read the
[Functions configuration guide](FUNCTIONS-CONFIG.md) for supported files,
precedence, triggers, worker isolation, and Pub/Sub registration.

## Test and contribute

Use stable Rust, Node 22, committed lockfiles, and isolated loopback ports. See
[testing and CI](docs/TESTING.md) for clean setup commands, local checks, SDK
tests, and exact-head CI behavior. See [Contributing](CONTRIBUTING.md) before
you open a pull request.

Historical validation results do not apply automatically to new source. The
current test commands and recorded SDK results are in
[compat-sdk/README.md](compat-sdk/README.md).

## Security, license, and support

FireRust does not implement Security Rules or production transaction
isolation. It is not a safe boundary for untrusted data or users. A successful
local test does not prove production compatibility. Read
[Security](SECURITY.md) and [Compatibility](docs/COMPATIBILITY.md).

Project code is available under Apache License 2.0. Reduced Google API Protocol
Buffer definitions keep their Google copyright and Apache notices. Read
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Firebase and Google product
names are trademarks of their owners. Their use does not show endorsement.

[Report a bug](https://github.com/dimavedenyapin/firebase-emu/issues/new/choose)
with the version, operating system, SDK version, and a small synthetic
reproduction. See [Troubleshooting](docs/TROUBLESHOOTING.md), the
[release policy](docs/RELEASING.md), and the [changelog](CHANGELOG.md).
