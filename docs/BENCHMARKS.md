# Recorded resource measurements

These historical results were collected before the public beta preparation.
They measure v0.1.3, not the current launch branch. Decimal MB = 1,000,000 bytes.

| Emulator process tree | Rust v0.1.3 | Official suite |
| --- | ---: | ---: |
| Idle RSS, range across runs | 68.1–68.2 MB | 735.3–781.7 MB |
| Peak RSS under load, range across runs | 110.2–110.6 MB | 2,345.6–2,435.9 MB |
| Recorded startup range | 0.01–0.42 s | approximately 16 s |

The recorded idle reduction is about 91%. The official peak includes a transient
Node process spike in each repetition. It is not a measure of steady Java heap use.
Do not present the peak difference as a general memory or throughput guarantee.
Startup is not a headline claim. One official repetition records a negative
startup value (-43.78 s), so the startup metric is not reliable. Do not use it
to compare implementations.

## Conditions

- Three repetitions per implementation; six runs completed with zero workload failures.
- Same Functions fixture: firebase-functions 3.24.1 and firebase-admin 9.12.0.
- Official Firebase tools 15.15.0, Java 21, cached emulator JARs.
- Rust used an empty SQLite data directory per run; the official suite used memory storage.
- UI disabled. Emulator tree includes its child workers; the workload driver is excluded.
- Each workload: 500 writes, 1,600 reads, 150 queries, 31 Auth operations,
  13 Storage operations with 401,408 bytes, 30 callables, 30 HTTP calls,
  60 triggers, and 10 Pub/Sub operations. All expected markers were observed.
- Isolated ports: 18080, 18099, 18199, 15001, and 18085.
- Summed RSS can count shared pages more than once. CPU was secondary, not a throughput test.

The source is the saved benchmark task report dated 2026-09-09. During launch
preparation the per-repetition raw samples were found in the local benchmark
workspace and checked against this table. Rust idle medians were 0.0681,
0.0682 and 0.0681 GB. Rust load maxima were 0.1106, 0.1102 and 0.1103 GB.
Official idle medians were 0.7353, 0.7607 and 0.7817 GB. Official load maxima
were 2.3456, 2.4311 and 2.4359 GB. All six runs match the ranges above.
The saved task artifact (report, summary, commands log) could not be retrieved,
and the runner script points to a release-cache Apple Silicon (aarch64) binary.
Machine model, OS build and exact binary hash therefore remain unverified here.
These figures are published with that evidence limit.

A separate 90-second, 81-sample application snapshot recorded a total stack RSS
of 259.9 MB median / 263.2 MB peak. This includes frontend and backend processes
and is not comparable to emulator-only RSS. It is not used in the headline table.

## Reproduce new measurements

The repository includes [a measurement harness](../scripts/bench/README.md).
Install locked example and Functions dependencies, build the chosen revision,
then follow that guide. Record the commit, binary checksum, machine/OS/RAM,
Node/Java versions, storage mode, readiness definition and raw samples. Use at
least three runs per implementation with the same fixture and quiet machine.
The harness commands are a starting point; they use different ports and a
different runner from the historical comparison, so they do not reproduce it
exactly.
