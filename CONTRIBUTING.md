# Contributing

Open an issue for a proposed API or behavior change. Describe the SDK call, expected
behavior and a small reproduction with synthetic data. Report vulnerabilities privately
through [SECURITY.md](SECURITY.md).

Use stable Rust and Node 22. Install locked dependencies using the commands in the
README. Run `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets --no-deps`,
`cargo test --locked --all-targets`, and the affected Node/browser suites. Run
`python3 test/release_automation_test.py` for release changes.

Use a branch and pull request. Explain the behavior change, tests and limits. Add a
regression test when fixing a protocol or data-integrity bug. Compare against an
actual Firebase SDK; do not replace integration evidence with mocked responses.
Use isolated loopback ports and task-owned data directories. Do not use cloud credentials.
Do not commit data directories, generated binaries, private app source or local paths.

Contributions are under Apache-2.0. Preserve third-party notices. Be respectful and
keep reviews focused on the code. The maintainer may decline changes outside beta scope.
