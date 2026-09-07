# Third-party notices

The reduced Protocol Buffer definitions under `proto/google/firestore/v1`,
`proto/google/rpc`, and `proto/google/type` are modified from the
[`googleapis/googleapis`](https://github.com/googleapis/googleapis) public API
definitions. Copyright Google LLC; licensed under Apache License 2.0. The
repository's `LICENSE` contains the applicable license text. Each modified
definition carries its upstream notice.

`proto/google/firestore/emulator/v1/firestore_emulator.proto` is a small,
independently written compatibility declaration for the emulator control RPC;
it is not presented as a vendored Google source file.

Rust and npm dependencies retain their own licenses and notices in their
distributed packages. `Cargo.lock` and npm lockfiles record exact dependency
versions.

Release binaries link the SQLite C library through the `bundled` feature of
`libsqlite3-sys`/`rusqlite`; SQLite is in the public domain. `rusqlite`,
`libsqlite3-sys`, and `fs2` retain their upstream licenses recorded by
`Cargo.lock`.
