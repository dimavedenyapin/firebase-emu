# Beta compatibility

Tested behavior is narrower than the complete Firebase API. Use the official
emulators for Security Rules and unsupported features. No production parity is promised.

| Service | Supported beta paths | Main limits |
| --- | --- | --- |
| Firestore | CRUD, batches, masks, filters, ordering, cursors, selected transforms, gRPC and browser listeners | No Security Rules, production isolation, composite indexes, aggregation or partition queries; no max/min/array-remove transforms |
| Auth | Admin user operations, email/password, profile, refresh and custom claims | Not a complete identity service; local emulator tokens |
| Storage | Node/browser upload, download, listing, delete and resumable chunks | No IAM, signature verification or resumable retry recovery |
| Pub/Sub | Topic/subscription CRUD, publish, pull/streaming, ACK, nack, fanout and restart | No push, filters, dead-letter policies, ordering or exactly-once delivery |
| Functions | HTTP/callable and v1 Firestore, Storage, Pub/Sub and schedule triggers | Node worker required; no v2 Pub/Sub CloudEvents |
| Console | Auth inspection, typed Firestore editing, cloning/copying, Pub/Sub inspection | No external object import; Pub/Sub inspection does not consume messages |
| Persistence | SQLite WAL, disk objects, restart and durable event recovery | One directory owner; at-least-once events; back up the whole stopped data directory |

Recorded SDK coverage includes Firebase Web 12.12.1, firebase-admin 9.12.0,
11.11.1 and 13.10.0, @google-cloud/firestore 7.11.6, firebase-functions 3.24.1,
and @google-cloud/pubsub 2.19.4 and 4.11.0. These are tested versions, not a
promise for all versions between them. Lockfiles and CI define each current
test environment. Use Node 22 for the documented beta path.

Native baselines: macOS 15 Intel/Apple Silicon; Ubuntu 24.04 glibc 2.39 x64/arm64;
Windows Server 2022 x64. Older systems have not been established as supported.
RTDB, Eventarc, task queues and analytics are outside scope.

See [Functions](../FUNCTIONS-CONFIG.md), [Pub/Sub](../PUBSUB.md), and the
[SDK matrix](../compat-sdk/README.md) for detailed checks.
