# Changelog

## Unreleased

- Adopt the FireRust user-facing name and supplied logo in the README and embedded console.
- Keep the `firebase-emu` command, package/crate names, repository URL, environment
  variables, data formats, and release paths compatible.

## 0.1.5 — 2026-09-13

- Prepare public beta documentation, historical benchmark disclosure and support files.
- Add security and release acceptance controls. See the launch report for validation.
- Add a contributor guide, issue and pull request templates, and a
  troubleshooting page with a minimal support policy.
- Fix the `examples/quickstart` seed script. It still never overwrites
  existing data. It now prints a clear message and exits cleanly on a re-run
  or on an unreachable emulator, instead of a raw program error.

## 0.1.4 — 2026-09-10

- Embedded Auth, Firestore and Pub/Sub console.
- Typed inline Firestore field editing, safe document cloning and typed object copying.
- Automatic UI port selection and fallback when the requested port is occupied.

## Earlier releases

See [GitHub Releases](https://github.com/dimavedenyapin/firebase-emu/releases) for release assets and source history.
