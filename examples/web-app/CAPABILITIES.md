# Browser test scope

All 15 required browser checks must pass on Google and Rust. They cover valid and invalid schema input, Auth sign-up/sign-out/sign-in, Firestore CRUD, partial merge, where/order/limit, a live listener update from a separate client, and Storage upload/download/list/delete.

The actual Firebase browser SDK connects directly to each emulator. No Admin proxy is used. A failed prerequisite is reported as blocked and causes strict tests to fail.
