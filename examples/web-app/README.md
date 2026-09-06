# Firebase browser demo

This synthetic React 16 app uses Firebase 12.12.1 directly against loopback
emulators. It contains no application or customer code.

Its strict result has 15 assertions: 13 emulator operations plus two generic
input-validation checks.

```sh
npm ci --ignore-scripts
npx playwright install chromium
npm test
npm run build
npm run dev
npm run test:integration
```

The default project is `demo-rust-emu`. Configure loopback endpoints with
`VITE_FIRESTORE_EMULATOR_HOST`, `VITE_FIREBASE_AUTH_EMULATOR_HOST`,
`VITE_FIREBASE_STORAGE_EMULATOR_HOST`, and
`VITE_FIREBASE_STORAGE_EMULATOR_PORT`. The strict probe fails for every failed
or blocked capability and uses the normal Firebase browser transport—there is
no Node proxy. Unit tests use fake clients and are not backend compatibility
evidence. See [CAPABILITIES.md](CAPABILITIES.md).
