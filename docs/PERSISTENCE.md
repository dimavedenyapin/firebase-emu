# FireRust persistence and recovery

FireRust uses memory storage by default. The explicit `--in-memory` option
has the same behavior.

Use a dedicated directory to keep acknowledged data:

```sh
firebase-emu --data-dir "./.firebase-emu-data" --no-functions
firebase-emu --data-dir "./local data/firebase" --no-functions
```

Persistence is available in v0.1.5. The GitHub launcher accepts these
arguments. Do not add an extra `--` to its `npx` command.

## Path and owner rules

- Do not use `--data-dir` with `--in-memory`. FireRust rejects this
  combination.
- A relative path starts at the process working directory. It does not start at
  the `--config` directory.
- FireRust does not read a custom data-directory field from `firebase.json`.
- The startup log prints the canonical SQLite and object paths.
- An exclusive lock prevents a second process from using the same directory.

The directory contains `firebase-emu.sqlite3` in SQLite WAL mode. It also
contains an owner lock, opaque object files in `blobs/`, and recovery files
in `tmp/`.

SQLite is compiled into each release binary. You do not need a database server
or an installed SQLite library.

## Durable and process-local state

Firestore stores complete protobufs without JSON conversion. This keeps 64-bit
integers, timestamps, bytes, references, geopoints, nested values, NaN values,
full resource names, and document timestamps.

FireRust stores these items:

- Auth users, password material, custom claims, sessions, and revocation state
- Storage metadata and finalized object bytes
- Pub/Sub topics, subscriptions, messages, leases, and acknowledgements
- pending and in-flight Functions outbox events

Auth ID sessions expire after one hour. Refresh sessions expire after 30 days.
FireRust removes expired sessions during normal work. Deleting a user
invalidates all sessions for that user.

An abandoned Storage resumable session expires after one hour. FireRust
removes expired sessions during normal work. An incomplete resumable upload is
process-local and is discarded after restart.

Active Firestore transaction handles are process-local. Open HTTP requests,
sockets, and incomplete resumable uploads are also process-local.

FireRust migrates a schema-v1 directory to schema v2 in one transaction. The
migration keeps Firestore, Auth, Storage, and Functions outbox data.

## SQLite access

A bounded 128-entry queue sends writes to one operating-system thread. Up to
eight blocking read connections can run at the same time. Each connection has
a 5-second busy timeout and an 8 MiB page-cache target. Acknowledged writes use
WAL with `synchronous=FULL`.

Persistent queries first narrow data in SQLite. They then use the Rust query
evaluator. Complex queries remain scans. FireRust keeps only the temporary
result set in memory. It does not keep an unbounded database mirror or result
cache.

## Storage files

Storage finalization flushes a unique temporary blob. It renames the blob to an
opaque UUID name. It then commits metadata and the event. Overwrite and delete
cleanup starts only after this commit.

At startup, FireRust removes incomplete temporary files and unreferenced blobs.
A missing referenced blob stops startup. A corrupt or newer schema also stops
startup. FireRust does not reset the data.

## Functions outbox

Supported Firestore and Storage triggers use a durable outbox when Functions
are configured. FireRust recovers pending and in-flight events after restart.
It retries each stable event ID up to five times with bounded backoff. Functions
status and drain report terminal failures.

FireRust immediately retries a failed durable state transition. It can reclaim
an expired five-second delivery lease without a restart.

FireRust checks direct Pub/Sub and schedule work after a burst of 32 or fewer
durable deliveries. It combines queued wake notifications at this boundary. A
10 ms pause between full bursts limits the sustained dispatch rate. It does not
drop or fail an event because of this limit.

A self-triggering function remains pending and rate-limited until you remove
the cause or stop the process. Drain cannot complete while the function
continues.

Delivery is at least once. A crash can occur after a handler succeeds but
before the durable acknowledgement. FireRust can then deliver the same event
ID again.

One source event can target several handlers. If delivery repeats, FireRust can
repeat the complete matching group. FireRust does not promise exactly-once
delivery. `--no-functions` keeps pending work but does not deliver it.

## Reset and backup

The Firestore ClearData API resets only the requested project and database. It
does not create events during restart recovery. FireRust does not reset,
import, or seed data automatically.

To reset all services:

1. Stop the process that owns the directory.
2. Confirm the selected data-directory path from the startup log.
3. Remove only that directory.

To make a backup:

1. Stop FireRust.
2. Copy the complete directory.
3. Include the database, any `-wal` and `-shm` files, and `blobs/`.

Do not copy only a live main database file.
