# Node.js SDK compatibility app

This synthetic ESM example exercises locked public SDK releases through their
supported entry points: Firebase Admin 11.11.1, direct Google Cloud Firestore
7.11.6, and direct Google Cloud Storage 7.7.0. It contains no application or
customer data.

Its strict result has 19 assertions: 17 emulator operations plus two generic
input-validation checks.

```sh
npm ci --ignore-scripts
npm test
GCLOUD_PROJECT=demo-node-sdk \
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 \
FIREBASE_AUTH_EMULATOR_HOST=127.0.0.1:9099 \
STORAGE_EMULATOR_HOST=http://127.0.0.1:9199 \
TARGET_NAME=rust npm run test:integration
```

Configuration rejects non-loopback endpoints and non-`demo-` projects. The
strict integration run fails for missing, failed, or blocked capabilities.
Partial merge/transforms, query, and listener checks own their fixtures and
cleanup. See [UNSUPPORTED.md](UNSUPPORTED.md) for deliberate limits.
