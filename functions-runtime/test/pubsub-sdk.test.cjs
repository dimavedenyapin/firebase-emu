'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {spawn} = require('node:child_process');
const {createServer} = require('node:net');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const repository = path.resolve(__dirname, '../..');
const binary = process.env.FIREBASE_EMU_BIN || path.join(repository, 'target/release/firebase-emu');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', error => error ? reject(error) : resolve()));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}

async function waitPort(port) {
  const started = Date.now();
  while (Date.now() - started < 20000) {
    try {
      await new Promise((resolve, reject) => {
        const socket = require('node:net').connect(port, '127.0.0.1');
        socket.once('connect', () => { socket.destroy(); resolve(); });
        socket.once('error', reject);
      });
      return;
    } catch { await delay(25); }
  }
  throw Error(`port ${port} did not open`);
}

async function start(dataDir) {
  const ports = await Promise.all(Array.from({length: 4}, freePort));
  const [firestore, auth, storage, pubsub] = ports;
  let stderr = '';
  const args = ['--no-functions', '--pubsub-port', String(pubsub)];
  if (dataDir) args.push('--data-dir', dataDir);
  const child = spawn(binary, args, {
    env: {...process.env, FIRESTORE_EMU_PORT: String(firestore), FIREBASE_AUTH_EMU_PORT: String(auth), FIREBASE_STORAGE_EMU_PORT: String(storage)},
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  child.stderr.on('data', value => { stderr += value; });
  await waitPort(pubsub).catch(error => { throw Error(`${error.message}\n${stderr}`); });
  return {
    child, host: `127.0.0.1:${pubsub}`, stderr: () => stderr,
    async stop() {
      if (child.exitCode === null) {
        child.kill('SIGTERM');
        await new Promise(resolve => child.once('exit', resolve));
      }
      assert.ok(child.exitCode === 0 || child.signalCode === 'SIGTERM', stderr);
    },
  };
}

function received(subscription, action = message => message.ack(), timeout = 15000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error(`timed out receiving on ${subscription.name}`)), timeout);
    const onError = error => { clearTimeout(timer); reject(error); };
    subscription.once('error', onError);
    subscription.once('message', async message => {
      clearTimeout(timer);
      subscription.removeListener('error', onError);
      try { await action(message); resolve(message); } catch (error) { reject(error); }
    });
  });
}

test('real @google-cloud/pubsub 4.11.0 and 2.19.4 SDK behavior', {timeout: 180000}, async t => {
  assert.ok(fs.existsSync(binary), `release binary missing: ${binary}`);
  const server = await start();
  t.after(() => server.stop());
  process.env.PUBSUB_EMULATOR_HOST = server.host;
  process.env.GCLOUD_PROJECT = 'demo-pubsub-sdk';
  const {PubSub} = require('@google-cloud/pubsub');
  assert.equal(require('@google-cloud/pubsub/package.json').version, '4.11.0');
  const pubsub = new PubSub({projectId: 'demo-pubsub-sdk'});
  t.after(() => pubsub.close());

  const [topic] = await pubsub.topic('events-main').get({autoCreate: true});
  await pubsub.topic('events-page-b').get({autoCreate: true});
  await pubsub.topic('events-page-c').get({autoCreate: true});
  const [pageOne, next] = await pubsub.getTopics({autoPaginate: false, pageSize: 2});
  assert.equal(pageOne.length, 2);
  assert.ok(next?.pageToken);
  const [pageTwo] = await pubsub.getTopics({...next, autoPaginate: false});
  assert.equal(pageTwo.length, 1);
  const [first] = await topic.subscription('consumer-one').get({autoCreate: true});
  const [second] = await topic.subscription('consumer-two').get({autoCreate: true});
  const firstMessage = received(first);
  const secondMessage = received(second);
  const id = await topic.publishMessage({data: Buffer.from([0, 1, 255]), attributes: {tenant: 'fanout'}});
  const [a, b] = await Promise.all([firstMessage, secondMessage]);
  assert.equal(a.id, id);
  assert.equal(b.id, id);
  assert.deepEqual(a.data, Buffer.from([0, 1, 255]));
  assert.deepEqual(a.attributes, {tenant: 'fanout'});

  let attempts = 0;
  let firstPublishTime;
  const nacked = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('nack was not redelivered')), 15000);
    first.on('message', message => {
      if (message.attributes.case !== 'nack') return message.ack();
      attempts += 1;
      if (attempts === 1) {
        firstPublishTime = message.publishTime.getTime();
        message.nack();
      } else {
        assert.equal(message.publishTime.getTime(), firstPublishTime);
        message.ack(); clearTimeout(timer); resolve(message.id);
      }
    });
  });
  const nackId = await topic.publish(Buffer.from('legacy-publish'), {case: 'nack'});
  assert.equal(await nacked, nackId);
  first.removeAllListeners('message');

  const [disconnect] = await topic.createSubscription('disconnect-sub');
  const disconnected = received(disconnect, async () => { await disconnect.close(); });
  const disconnectId = await topic.publishMessage({data: Buffer.from('disconnect')});
  assert.equal((await disconnected).id, disconnectId);
  const reconnect = pubsub.subscription('disconnect-sub');
  assert.equal((await received(reconnect)).id, disconnectId);
  await reconnect.close();

  const [unary] = await topic.createSubscription('unary-sub');
  const subscriberClient = await pubsub.getClientAsync_({client: 'SubscriberClient'});
  const unaryId = await topic.publishMessage({data: Buffer.from('deadline')});
  const [pulled] = await subscriberClient.pull({subscription: unary.name, maxMessages: 1, returnImmediately: true});
  assert.equal(pulled.receivedMessages[0].message.messageId, unaryId);
  const ackId = pulled.receivedMessages[0].ackId;
  await subscriberClient.modifyAckDeadline({subscription: unary.name, ackIds: [ackId], ackDeadlineSeconds: 1});
  await delay(1200);
  const [expired] = await subscriberClient.pull({subscription: unary.name, maxMessages: 1, returnImmediately: true});
  assert.equal(expired.receivedMessages[0].message.messageId, unaryId);
  await subscriberClient.acknowledge({subscription: unary.name, ackIds: [expired.receivedMessages[0].ackId]});

  const [attached] = await topic.getSubscriptions();
  assert.ok(attached.some(value => value.name === first.name));
  const otherProject = new PubSub({projectId: 'demo-other-project'});
  t.after(() => otherProject.close());
  await otherProject.topic('other-topic').get({autoCreate: true});
  assert.deepEqual((await otherProject.getTopics())[0].map(value => value.name), ['projects/demo-other-project/topics/other-topic']);

  await assert.rejects(
    () => topic.createSubscription('push-sub', {pushEndpoint: 'http://127.0.0.1:1'}),
    error => error.code === 12,
  );
  await assert.rejects(
    () => topic.createSubscription('exactly-once-sub', {enableExactlyOnceDelivery: true}),
    error => error.code === 12,
  );
  await assert.rejects(
    () => pubsub.createTopic('x'),
    error => error.code === 3,
  );

  const [largeTopic] = await pubsub.createTopic('large-message-topic');
  const [largeSubscription] = await largeTopic.createSubscription('large-message-sub');
  const largeReceived = received(largeSubscription, message => message.ack(), 30000);
  const largeData = Buffer.alloc(5 * 1024 * 1024, 0x5a);
  const largeId = await largeTopic.publishMessage({data: largeData});
  const deliveredLarge = await largeReceived;
  assert.equal(deliveredLarge.id, largeId);
  assert.deepEqual(deliveredLarge.data, largeData);
  await largeSubscription.close();

  const [capacity] = await pubsub.createTopic('capacity-topic');
  await capacity.createSubscription('capacity-one');
  await capacity.createSubscription('capacity-two');
  const grpc = require('@grpc/grpc-js');
  const protoLoader = require('@grpc/proto-loader');
  const definition = protoLoader.loadSync(path.join(repository, 'proto/google/pubsub/v1/pubsub.proto'), {
    includeDirs: [path.join(repository, 'proto'), path.join(repository, 'functions-runtime/node_modules/google-gax/build/protos')],
  });
  const RawPublisher = grpc.loadPackageDefinition(definition).google.pubsub.v1.Publisher;
  const rawPublisher = new RawPublisher(server.host, grpc.credentials.createInsecure());
  const rawPublish = messages => new Promise((resolve, reject) => rawPublisher.publish(
    {topic: capacity.name, messages},
    (error, response) => error ? reject(error) : resolve(response),
  ));
  let accepted = 0;
  let capacityError;
  for (let batch = 0; batch < 6; batch += 1) {
    try {
      const response = await rawPublish(Array.from({length: 1000}, () => ({data: Buffer.from('x')})));
      accepted += response.messageIds.length;
    } catch (error) {
      capacityError = error;
      break;
    }
  }
  assert.ok(accepted >= 4000, `accepted only ${accepted} messages before capacity`);
  assert.equal(capacityError?.code, 8);
  rawPublisher.close();

  const PubSub2 = require('pubsub-v2').PubSub;
  assert.equal(require('pubsub-v2/package.json').version, '2.19.4');
  const old = new PubSub2({projectId: 'demo-pubsub-v2'});
  t.after(() => old.close());
  const [oldTopic] = await old.createTopic('legacy-topic');
  const [oldSubscription] = await oldTopic.createSubscription('legacy-sub');
  const oldReceived = received(oldSubscription);
  const oldId = await oldTopic.publish(Buffer.from('sdk-2.19.4'), {version: 'old'});
  assert.equal((await oldReceived).id, oldId);
  await oldSubscription.close();
  await Promise.all([first.close(), second.close(), unary.close()]);
});

test('real process restart preserves pending and never resurrects ACKed messages', {timeout: 90000}, async t => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'firebase-emu-pubsub-process-'));
  t.after(() => fs.rmSync(dataDir, {recursive: true, force: true}));
  let server = await start(dataDir);
  process.env.PUBSUB_EMULATOR_HOST = server.host;
  const {PubSub} = require('@google-cloud/pubsub');
  let pubsub = new PubSub({projectId: 'demo-pubsub-restart'});
  const [topic] = await pubsub.createTopic('restart-topic');
  await topic.createSubscription('restart-sub');
  const id = await topic.publishMessage({data: Buffer.from('pending')});
  await pubsub.close();
  await server.stop();

  server = await start(dataDir);
  process.env.PUBSUB_EMULATOR_HOST = server.host;
  pubsub = new PubSub({projectId: 'demo-pubsub-restart'});
  const subscription = pubsub.subscription('restart-sub');
  assert.equal((await received(subscription)).id, id);
  await subscription.close();
  await pubsub.close();
  await server.stop();

  server = await start(dataDir);
  t.after(() => server.stop());
  process.env.PUBSUB_EMULATOR_HOST = server.host;
  pubsub = new PubSub({projectId: 'demo-pubsub-restart'});
  t.after(() => pubsub.close());
  const client = await pubsub.getClientAsync_({client: 'SubscriberClient'});
  const [empty] = await client.pull({subscription: 'projects/demo-pubsub-restart/subscriptions/restart-sub', maxMessages: 1, returnImmediately: true});
  assert.deepEqual(empty.receivedMessages || [], []);
});
