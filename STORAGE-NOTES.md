# Storage integration notes

The Storage router uses axum 0.8, tower-http CORS, and the shared loopback HTTP listener. It supports GCS JSON routes, Firebase `/v0` routes, and local signed-URL-style requests.

Browser multipart and resumable uploads preserve bytes. Node SDK downloads receive CRC32C metadata and headers. Browser download URLs, object listing, pagination, and delete use the same object store as GCS clients. The Auth/Storage worker also checked cross-client binary uploads and a 600 KB browser resumable upload.

Retryable multi-chunk append, IAM, and signed URL signature or expiry checks remain outside the verified scope. Exact combined SDK results are in `compat-sdk/README.md`.

Persistent mode stores only metadata in SQLite and immutable bytes in opaque
UUID-named blob files. Object names are never filesystem paths. Finalization
uses temp-write, file sync, rename, and metadata/outbox commit ordering; startup
removes incomplete and orphaned files. Incomplete resumable sessions are
discarded on restart. Windows uses unique destination names because replacing
open files is not portable; delayed old-file cleanup is harmless and startup
reconciliation retries it.
