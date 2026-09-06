# Firestore integration notes

The shared Firestore service uses tonic 0.12 and prost 0.13. `build.rs` compiles the local wire-compatible proto subset with vendored `protoc`. The axum07 alias adds HTTP reset, browser REST, and WebChannel routes to the gRPC service on the same loopback port.

The service supports nested update masks, field deletion, quoted field paths, map replacement, field/unary/composite filters, orderBy, limit, offset, cursors, projections, and live document/query subscriptions. Writes, deletes, and scoped reset notify subscribers. Browser and gRPC clients use the same document store.

The strict Node SDK checks use direct `@google-cloud/firestore` 7.11.6. The strict browser checks use Firebase 12.12.1. Both must pass partial merge, where/order/limit, and a live listener update. Exact combined results are in `compat-sdk/README.md`.

Firestore field transforms remain explicitly rejected. Production transaction isolation, Security Rules, composite index enforcement, aggregation, and partition queries are outside the verified scope.
