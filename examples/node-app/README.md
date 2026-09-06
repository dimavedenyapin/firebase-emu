# Node.js SDK compatibility app

This ESM example exercises the exact SDK versions resolved by the supplied `upload-functions` reference lockfile through their supported package entry points:

- `firebase-admin/app` and `firebase-admin/auth` 11.11.1 (resolved from `^11.8.0`)
- `@google-cloud/firestore` 7.11.6 directly (resolved from `^7.3.1`, not through Admin)
- `@google-cloud/storage` 7.7.0 directly
- `peakflo-schema`, GitHub package `peakflo/peakflo-schema#v5.11.62`

The supplied reference commits are `peakflo-web@33390be512c6f4c531d5e10ec7bdf92fbe217523` and `upload-functions@edeab8d9b6dae3a01077640e24c2178114c43c9d`. The latter imports `peakflo-schema` from GitHub tag `v5.11.62`, resolved in its lockfile to commit `03a381c70f841b731d95adf01fb63b3c3b13cd24`. The checked-out tag identifies itself as `@peakflo/peakflo-schema` 5.11.62; the dependency name preserves the application's real unscoped import spelling. Its tested `lib/index.js` and currency-schema artifact match the reference checkout byte-for-byte.

Install and run unit/import checks:

```sh
npm ci
npm test
```

Run the same integration scenario against either Google's emulators or the Rust mock by changing only endpoints:

```sh
GCLOUD_PROJECT=demo-node-sdk \
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 \
FIREBASE_AUTH_EMULATOR_HOST=127.0.0.1:9099 \
STORAGE_EMULATOR_HOST=http://127.0.0.1:9199 \
TARGET_NAME=rust npm run test:integration
```

`FIREBASE_STORAGE_EMULATOR_HOST=127.0.0.1:9199` is accepted as a fallback. All configuration is restricted to loopback and `demo-` project IDs to avoid accidental production writes. The integration run prints one JSON result and exits nonzero if any capability fails or is blocked. `npm run probe` emits the same measured JSON without converting incompatibilities into a successful compatibility claim.

Partial merge, where/order/limit, and realtime listener checks each create and clean up their own data and have hard time limits. This keeps one missing Rust feature from blocking measurement of the other two.

See [UNSUPPORTED.md](./UNSUPPORTED.md) for deliberately excluded compatibility calls.
