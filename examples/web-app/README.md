# Firebase browser demo

This React 16 app uses the modular Firebase SDK directly. It follows `peakflo-web` commit `33390be512c6f4c531d5e10ec7bdf92fbe217523`, `src/initializeFirebase.js`. Versions match its lock file: Firebase 12.12.1, React 16.14.0, Vite 5.4.21, and `@peakflo/peakflo-schema` 4.11.58. The app validates a published mass-action enum through the browser-safe `@peakflo/peakflo-schema/lib/schemas/massAction/type.massAction` deep import. Remote Config, Analytics, Functions, and RTDB are outside this test.

```sh
npm ci
npx playwright install chromium
npm test
npm run build
npm run dev
npm run probe
npm run test:integration
```

Start the emulators before the probe. All endpoints must use loopback HTTP. The default project is `demo-rust-emu`; `VITE_FIREBASE_projectId` can select another `demo-` project. Configure ports with `VITE_FIRESTORE_EMULATOR_HOST`, `VITE_FIREBASE_AUTH_EMULATOR_HOST` (HTTP URL), `VITE_FIREBASE_STORAGE_EMULATOR_HOST`, and `VITE_FIREBASE_STORAGE_EMULATOR_PORT`. The probe emits deterministic per-capability `passed`, `failed`, or `blocked` markers as JSON; strict mode exits nonzero unless every capability passes.

The unit tests use fake clients. They test UI state, input checks, and error display. They do not prove SDK compatibility. The probe runs real Chromium with the real browser SDK. It emits JSON results. `probe` returns zero when the test harness runs, even if capabilities fail. `test:integration` returns one if any capability fails or is blocked, and two for a harness failure. A blocked result identifies a failed prerequisite. Firestore requests use the normal browser transport; no Node proxy or transport replacement is used. Each service call has a timeout. Browser shutdown cancels pending SDK work.

Use an isolated emulator process for each run. The probe creates one user and may leave data after a failed operation. It does not reset unrelated data. Official emulator tests require test-only Auth, Firestore, and Storage rules that allow these operations.

See [CAPABILITIES.md](./CAPABILITIES.md) for the explicit Google-emulator and Rust-mock expectations.
