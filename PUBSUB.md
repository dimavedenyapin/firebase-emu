# Pub/Sub emulator

The binary exposes the real `google.pubsub.v1.Publisher` and
`google.pubsub.v1.Subscriber` gRPC services on `127.0.0.1:8085` by default. It
never connects to Google Cloud and never loads ambient cloud credentials.

Configure a client with the standard emulator variable:

```sh
export GCLOUD_PROJECT=demo-local
export PUBSUB_EMULATOR_HOST=127.0.0.1:8085
firebase-emu --project demo-local --no-functions
```

The host and port also follow `firebase.json` `emulators.pubsub.host`/`port`,
`FIREBASE_EMU_HOST`, `PUBSUB_EMULATOR_PORT`, and the `--host`/`--pubsub-port`
CLI overrides. Only loopback hosts are accepted.

## Node SDK example

This works with the actual app-compatible `@google-cloud/pubsub` 4.11.0 and
2.19.4 clients:

```js
const {PubSub} = require('@google-cloud/pubsub');
const client = new PubSub({projectId: 'demo-local'});

const [topic] = await client.topic('jobs').get({autoCreate: true});
const [subscription] = await topic.subscription('worker').get({autoCreate: true});

subscription.on('message', message => {
  console.log(message.data, message.attributes, message.id);
  message.ack();
});

await topic.publishMessage({
  data: Buffer.from([0, 1, 2, 255]),
  attributes: {source: 'local'},
});
```

The legacy `topic.publish(data, attributes)` API is also exercised by the
integration tests.

## Supported behavior

- Topic and subscription create/get/list/delete, pagination, and topic
  subscription listing.
- Atomic publish with opaque bytes, string attributes, stable message IDs, and
  server publish timestamps. A publish with no subscriptions succeeds normally.
- Independent fanout to every subscription that exists at the publish commit.
  ACKing one subscription never consumes another subscription's copy.
- Unary Pull and bidirectional StreamingPull, including flow control by message
  count and bytes, ACK, ModifyAckDeadline, and nack with a zero-second deadline.
- Ack-deadline expiry, disconnect release, cancellation, and redelivery with the
  original message ID and publish timestamp.
- Firebase Functions v1 `topic(...).onPublish` startup discovery. The runtime
  creates one internal subscription per handler, sends the normal base64 data,
  attributes, message ID, timestamp, and v1 context envelope, and ACKs only
  after a successful invocation.
- In-memory operation by default. With `--data-dir`, topics, subscriptions,
  messages, fanout delivery state, leases, and ACK state use the existing
  SQLite WAL writer. Publish returns only after the fanout transaction commits.
  A restart retains pending deliveries, respects a still-current absolute lease,
  makes expired leases available again, and never resurrects ACKed messages.

Delivery is at least once. A crash after a handler or subscriber side effect but
before its ACK is durable can repeat the same message. There is no production
ordering or exactly-once promise.

## Bounds and deliberate gaps

A message and an entire publish request are each limited to 10 MiB. Pending
fanout is limited to 10,000 delivery records and 64 MiB of delivery bytes. A
publish that would cross either bound fails atomically with `RESOURCE_EXHAUSTED`.
Unacknowledged messages expire after 24 hours; cleanup during broker operations
removes their delivery records and bounds persistent message growth. Streaming
output is bounded and honors the client's outstanding-message and byte limits.
The SQLite writer queue and read permits retain the bounds documented in the
main README.

Local HTTP push is not needed by either inspected application and is not
implemented. Creating a push, filtered, dead-letter, ordered, exactly-once,
BigQuery, or Cloud Storage subscription fails explicitly. IAM, schemas,
snapshots/seek, detach, topic updates, and ordering guarantees also return
`UNIMPLEMENTED`; they do not report false success. Functions v2 Pub/Sub exports
are outside current scope because neither inspected application uses them.
This is a focused local emulator, not a full Cloud Pub/Sub parity claim.

The retained explicit Functions injection endpoint is a compatibility control:
it invokes matching handlers directly and does not also publish to the broker.
Consequently, a real SDK publish and an explicit injection are distinct events,
with no hidden alternate path that duplicates either one.
