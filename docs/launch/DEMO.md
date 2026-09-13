# Two-minute demonstration script — DRAFT

This script is a draft. It waits for final launch checks and the
selected release. Use only the synthetic demo fixture from
`examples/quickstart/seed.py`. Use launch-approved release bytes.
Do not show real tenant data, private local paths, or credentials.
Functions need a Node worker. They are not enabled in this short
demonstration.

Total time is about 120 seconds.

## 0:00–0:15 — Start

1. Start the emulator with project `demo-local`.
2. Use `--ui-port 0` and `--no-functions`.
3. Use a new empty data directory.
4. Run the seed script.
5. Show the console URL that startup prints.

## 0:15–0:35 — Auth

6. Open the Auth view.
7. Show the user `developer@example.test`.
8. Show its local record and empty custom claims.

## 0:35–1:00 — Firestore

9. Open the Firestore view.
10. Open the collection `products`.
11. Open the document `starter`.
12. Show the text, integer, and nested map fields.
13. Edit the `stock` integer and save it.
14. Clone the document to `products/starter-copy`.
15. Exclude subcollections from the clone.

## 1:00–1:20 — Copy and Pub/Sub

16. Use Copy object on the document.
17. Explain that explicit type wrappers preserve Firestore values.
18. Open the Pub/Sub view.
19. Show the empty-topic state. The quickstart seed does not create a topic.
20. Explain that inspection does not consume messages. The Pub/Sub screenshot
    uses a separate synthetic topic setup and is not quickstart seed evidence.

## 1:20–1:45 — Restart

21. Stop the emulator process.
22. Start it again with the same data directory.
23. Show that the edited stock value is still present.

## 1:45–2:00 — Limits

24. Show the compatibility table.
25. State that Security Rules are not supported.
26. State that production parity is not promised.
27. Give the issue-report link.

A screenshot is useful for the README. A recorded video is optional.
