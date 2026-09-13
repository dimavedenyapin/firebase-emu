# Public beta announcement — DRAFT

Do not post this text. It is a draft. It waits for final launch
checks and the selected release. The current published release is
v0.1.4. It does not contain the new security patches from the launch
branch. Do not present unmerged fixes as part of v0.1.4.

## Announcement draft

A public beta of a Rust Firebase emulator is available. It runs on
your local machine. It supports local development and automated tests.

It serves Firestore, Auth, Storage, and Pub/Sub. It includes an
embedded browser console. It supports optional Node-based Firebase
Functions. SQLite persistence keeps local data across restarts.

This is an independent project. It is not affiliated with, sponsored
by, or endorsed by Google. It does not implement the full Firebase
API. It does not promise production parity.

To try it, use Node 22 and run:

```sh
npx --yes github:dimavedenyapin/firebase-emu#v0.1.4 --project demo-local --ui-port 0 --no-functions
```

Open the console URL that startup prints. To keep data between runs,
add `--data-dir ./.firebase-emu-data`. Use only synthetic demo data.
See the repository for SDK connection instructions.

Scope and limits: no Security Rules. No production transaction
isolation. No composite indexes, aggregation, or partition queries.
Local emulator tokens only. No IAM or signature checks. Pub/Sub has
no push delivery, filters, dead-letter policies, message ordering, or
exactly-once delivery. Functions need a Node worker. See the
compatibility table before you adopt it.

Recorded resource figures come from a historical v0.1.3 comparison.
Idle process-tree RSS was 68.1–68.2 MB against 735.3–781.7 MB for the
official suite. Load peak RSS was 110.2–110.6 MB against 2,345.6–
2,435.9 MB. These figures are not a speed or memory guarantee. The
Rust runs used an empty SQLite data directory per run. The official
runs used memory storage. Summed RSS can count shared pages more than
once. Machine model, OS build, and exact binary hash remain
unverified. Full conditions and evidence limits are in the benchmark
page.

Screenshots show the local console with synthetic demo data:

- `docs/images/console-auth.png` — local user record
- `docs/images/console-firestore.png` — typed document fields
- `docs/images/console-pubsub.png` — topic inspection

Repository: https://github.com/dimavedenyapin/firebase-emu
Feedback: send a small synthetic reproduction with your release, OS,
and SDK versions through GitHub Issues.

## Short post — DRAFT

Public beta: a Rust Firebase emulator with Firestore, Auth, Storage,
Pub/Sub, an embedded console, and optional Node Functions. Local
SQLite persistence. Five native platforms. Independent project. No
Security Rules and no production parity. Details and limits:
https://github.com/dimavedenyapin/firebase-emu

## Before publication

Confirm the final release tag and acceptance results in the launch
report. If a new release includes the security fixes, update the npx
command. Keep this file marked as draft until release selection.
