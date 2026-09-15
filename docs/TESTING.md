# FireRust testing and CI

Use stable Rust and Node 22. Use isolated loopback ports and synthetic
`demo-` projects. Do not use cloud credentials.

## Prepare a clean checkout

All Node workspaces have committed lockfiles.

```sh
npm ci --prefix functions-runtime --ignore-scripts
npm ci --prefix compat --ignore-scripts
npm ci --prefix examples/node-app --ignore-scripts
npm ci --prefix examples/web-app --ignore-scripts
npx --prefix functions-runtime playwright install chromium
```

## Run local checks

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

Run the generic SDK matrix with:

```sh
./scripts/sdk-compat.sh
```

Set `SDK_COMPAT_INSTALL=1` to install missing locked dependencies. Use the
emulator host and port environment variables to select isolated ports.

The Google-emulator half also requires Java and the pinned Firebase CLI in
`compat/`. The manual CI workflow can run the official and browser matrix.
See the [SDK matrix record](../compat-sdk/README.md).

Set `PLAYWRIGHT_CHROMIUM_EXECUTABLE` to use a specified Chromium executable.
If you do not set it, browser tests use Playwright Chromium.

## Pull request validation

Pull request validation uses the exact pull request head. It starts when you
open, update, reopen, or mark a pull request ready for review. It cancels an
older run for the same pull request. The job has a 30-minute limit.

The `Rust and Functions checks` job does this work:

- installs locked Node dependencies
- runs Rust format, check, lint, and all-target tests
- builds the release binary
- tests real Node Admin and browser Firebase SDK traffic
- tests real Pub/Sub streaming, fanout, bounds, and restart behavior
- tests the relocated Functions runtime
- tests SQLite WAL and browser restart persistence
- tests durable Functions outbox crash recovery

One failed suite fails the job. If an SDK gate fails, CI prints the emulator
logs.

The Functions test moves the release binary, adapter, dependencies, and
fixtures before startup. This prevents fallback to the source checkout.

## Release workflow

The `Release binaries` workflow builds and tests native packages for Linux
x64 and arm64, macOS x64 and arm64, and Windows x64. It does not run for pull
requests.

A maintainer can run it manually. The automatic release workflow can call it
only after the required validation job succeeds. Read the
[release process](RELEASING.md) for tag and publication controls.
