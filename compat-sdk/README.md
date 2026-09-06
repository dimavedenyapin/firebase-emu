# SDK compatibility matrix

The current public matrix contains 68 required assertions: 60 emulator I/O
capabilities (17 Node and 13 browser checks against both targets) plus eight
repeated generic input-validation checks (two Node and two browser checks per
target). The validators replace the former private-schema assertions; they
preserve harness coverage but are not emulator compatibility evidence. It uses
only synthetic fixtures and locked public SDK dependencies. Missing,
duplicate, failed, or blocked checks make the runner fail.

Run from a clean checkout:

```sh
npm ci --prefix compat --ignore-scripts
npm ci --prefix examples/node-app --ignore-scripts
npm ci --prefix examples/web-app --ignore-scripts
npx --prefix examples/web-app playwright install chromium
./scripts/sdk-compat.sh
```

Use Java 21 for the Google Firestore emulator. Set `FIREBASE_BIN`,
`SDK_COMPAT_JAVA_HOME`, or `PLAYWRIGHT_CHROMIUM_EXECUTABLE` when automatic
discovery is inappropriate. `SDK_COMPAT_INSTALL=1` asks the runner to install
missing dependencies from committed lockfiles. Host and port environment
variables may select isolated loopback ports for a Rust-only target run.

The last pre-cleanup run on 2026-09-06 passed the then-current 68-check matrix,
45 Rust tests, 5 Node unit tests, 15 browser unit tests, and the browser/release
builds. Separate historical application runs passed 18/18 and 11/11 checks.
Those results describe the previous revision and are not reused as validation
for this one.

Fresh results for this revision must be generated with the commands above and
are written to `target/sdk-compat/summary.json`; generated results are not
committed. The covered operations include Firestore merge/transforms,
where/order/limit and live listeners, browser and Admin Auth, and Node/browser
Storage operations. See `unsupported.json` and the root README for deliberate
limits.

Local cleanup validation on 2026-09-06 used an isolated release build and
ports 28080/28099/28199. The Rust target passed all 34/34 assertions (19 Node,
15 browser): 30 emulator I/O checks and four generic fixture checks. The same
revision passed Rust 47/47, Functions adapter 10/10, relocated full-binary 1/1,
Node unit 5/5, browser unit 15/15, and matrix-gate 1/1; browser and Rust release
builds also passed. The Google half was not rerun locally because the existing
interactive stack owns its default ports; the native CI and manual compatibility
workflows provide clean runners.
