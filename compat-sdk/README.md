# Verified SDK matrix

Final run: 2026-09-06T11:55:44.265Z.

| Target | Node | Browser | Failed | Blocked |
| --- | ---: | ---: | ---: | ---: |
| Google Firebase emulators | 19/19 | 15/15 | 0 | 0 |
| Rust release | 19/19 | 15/15 | 0 | 0 |

All 68 required SDK checks passed. All 13 runner cases passed. No checks were skipped or marked unsupported.

- Rust regression tests: 41/41 passed.
- Node unit tests: 5/5 passed.
- Browser unit tests: 15/15 passed.
- Matrix gate regression test: 1/1 passed.
- Browser build and Rust release build: passed.
- Installed SDK versions and lockfiles: passed.

The browser listener test receives a write from a separate Firebase client. Both SDK suites check partial merge, where/order/limit, and live listener updates. Auth and Storage checks use actual SDK requests. The browser test runs in Chromium through Playwright, with no Admin proxy.

Binary: `target/release/firebase-emu` (5714160 bytes).

SHA-256: `8ea2dd7ba0628299b0bc2127d8150ce5f7c4cb28f8921ab18886058365971d70`.

Functions integration is saved in commits `b345db2`, `6a58689`, and `eaf0150`; the recovered three-service base is `f10424a`.

Run from the repository root:

```sh
./scripts/sdk-compat.sh
```

The runner checks the exact versions in `check-versions.mjs`, requires each check in `required-checks.json` once, and returns a nonzero exit for any failure, missing check, duplicate check, or blocked check. Unit tests use local test doubles. Integration tests use the real SDKs and emulator processes.

Full command output and individual results are in `target/sdk-compat/summary.json`. The task artifact also preserves this JSON and the final report. The service is restricted to loopback and the SDK examples require demo projects. No production repository was changed and nothing was published.

The supplied production application test suite was not available. These results prove the required pinned example matrix. See the root README for features outside that matrix.
