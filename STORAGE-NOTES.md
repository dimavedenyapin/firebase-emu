# Storage integration notes

The Storage router uses axum 0.8, tower-http CORS, and the shared loopback HTTP listener. It supports GCS JSON routes, Firebase `/v0` routes, and local signed-URL-style requests.

Browser multipart and resumable uploads preserve bytes. Node SDK downloads receive CRC32C metadata and headers. Browser download URLs, object listing, pagination, and delete use the same object store as GCS clients. The Auth/Storage worker also checked cross-client binary uploads and a 600 KB browser resumable upload.

Retryable multi-chunk append, IAM, and signed URL signature or expiry checks remain outside the verified scope. Exact combined SDK results are in `compat-sdk/README.md`.
