# Local Functions configuration

`src/functions_config.rs` discovers Firebase Functions configuration without
calling Firebase, Google Cloud, metadata services, or secret managers. The
runtime must pass its parsed command-line overrides and an environment snapshot
to `functions_config::load`.

Precedence is command-line override, environment, local project files, then
defaults. Supported environment controls are:

- `FIREBASE_EMU_PROJECT`, then `GCLOUD_PROJECT`, then `GOOGLE_CLOUD_PROJECT`
- `FIREBASE_EMU_HOST`
- `FIREBASE_FUNCTIONS_EMU_PORT`, `FIRESTORE_EMU_PORT`,
  `FIREBASE_AUTH_EMU_PORT`, `FIREBASE_STORAGE_EMU_PORT`,
  `FIREBASE_DATABASE_EMU_PORT`, and `PUBSUB_EMULATOR_PORT`
- `FIREBASE_FUNCTIONS_SOURCE`, `FIREBASE_FUNCTIONS_CODEBASE`, and
  `FIREBASE_FUNCTIONS_RUNTIME`
- `FIREBASE_EMU_RUNTIME_CONFIG`, containing a local JSON object

The loader accepts the object and array forms of `firebase.json` `functions`,
including `source`, `codebase`, and `runtime`. If runtime is absent there, the
source package's `engines.node` is used, followed by Node 20. Node 18, 20, and 22
are accepted. Emulator ports come from the corresponding `firebase.json`
entries before falling back to 5001, 8080, 9099, 9199, 9000, and 8085.

Only a lowercase `demo-...` project is allowed. `.firebaserc` aliases are
resolved, but an alias targeting a real project is rejected. A committed
`0.0.0.0` Functions host is safely narrowed to `127.0.0.1`; a command-line or
environment non-loopback host is rejected.

For legacy `firebase-functions` v1 `functions.config()`, per-codebase
`.runtimeconfig.json` objects are merged and supplied as
`CLOUD_RUNTIME_CONFIG`. Conflicting values across codebases fail startup.
Nested objects are preserved. A malformed or non-object config fails startup.

For dotenv-based applications, only `.env.<demo-project>` and `.env.local` in
each Functions source are loaded, in that order. Matching explicitly supplied
environment variables override file values. Generic `.env` is deliberately not
loaded because existing application checkouts may contain stage or production
credentials. Arbitrary parent-process environment variables are not propagated.

The child environment includes local emulator hosts plus `FIREBASE_CONFIG` with
the demo project ID, `<project>.appspot.com` bucket, and local Realtime Database
URL. No Application Default Credentials or production configuration is read.
