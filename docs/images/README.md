# Launch images — DRAFT

These screenshots are launch materials only. They are not published.

All three images show the local console. They use synthetic demo data
only. They come from an isolated loopback emulator. The binary was a
local build of base commit `0a3e40f`, which is also tagged `v0.1.4`.
It was not a downloaded release archive, and it did not contain the
public-beta integration patches. A final integration browser run found
no material UI behavior change; see `docs/launch/READINESS.md` for that
separate evidence. Do not treat these images as published-archive evidence.

- `console-auth.png` — Authentication view. It shows the synthetic
  user `developer@example.test` and its local record.
- `console-firestore.png` — Firestore view. It shows the document
  `products/starter` with integer, text, and nested map fields.
- `console-pubsub.png` — Pub/Sub view. It shows the topic
  `demo-orders` and its subscription. This topic used a separate
  synthetic setup. The quickstart seed does not create it. Inspection
  does not consume messages.

The README worker can link these files with their relative paths.
Keep them marked as draft until final launch checks pass.
