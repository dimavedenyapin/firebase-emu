'use strict';
const assert = require('node:assert/strict');
const {execFileSync, spawn} = require('node:child_process');
const {createServer} = require('node:net');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const repository = path.resolve(__dirname, '../..');
const binary = path.resolve(process.env.FIREBASE_EMU_BIN || path.join(repository, 'target/release/firebase-emu'));
const node22 = path.resolve(process.env.FIREBASE_FUNCTIONS_NODE_22 || process.execPath);

async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', error => error ? reject(error) : resolve()));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
async function waitFor(predicate, label, timeout = 30000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error(`timed out waiting for ${label}`);
}
async function stop(child, crash = false) {
  if (!child || child.exitCode !== null) return;
  child.kill(crash && process.platform !== 'win32' ? 'SIGKILL' : 'SIGTERM');
  await new Promise(resolve => child.once('exit', resolve));
}

(async () => {
  assert.ok(fs.existsSync(binary), `release binary missing: ${binary}`);
  assert.equal(execFileSync(node22, ['-p', "process.versions.node.split('.')[0]"], {encoding: 'utf8'}).trim(), '22');
  const [functionsPort, firestorePort, authPort, storagePort, databasePort, pubsubPort] = await Promise.all(Array.from({length: 6}, freePort));
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'firebase durable outbox '));
  const dataDir = path.join(temporary, 'data with spaces');
  const eventLog = path.join(temporary, 'events.jsonl');
  const baseEnvironment = {
    ...process.env,
    FIREBASE_FUNCTIONS_ADAPTER: path.join(repository, 'functions-runtime/adapter.cjs'),
    FIREBASE_FUNCTIONS_NODE_22: node22,
    FIREBASE_FUNCTIONS_EMU_PORT: String(functionsPort),
    FIRESTORE_EMU_PORT: String(firestorePort),
    FIREBASE_AUTH_EMU_PORT: String(authPort),
    FIREBASE_STORAGE_EMU_PORT: String(storagePort),
    FIREBASE_DATABASE_EMU_PORT: String(databasePort),
    PUBSUB_EMULATOR_PORT: String(pubsubPort),
    FIXTURE_EVENT_LOG: eventLog,
    FIREBASE_EMU_TEST_TRIGGER_LOOP_LIMIT: '40',
  };
  let stderr = '';
  let child;
  const start = async ackDelay => {
    stderr = '';
    const environment = {...baseEnvironment};
    if (ackDelay) environment.FIREBASE_EMU_TEST_OUTBOX_ACK_DELAY_MS = String(ackDelay);
    child = spawn(binary, ['--config', path.join(repository, 'functions-runtime/fixtures'), '--project', 'demo-functions-cli', '--data-dir', dataDir], {
      cwd: repository, env: environment, stdio: ['ignore', 'ignore', 'pipe'],
    });
    child.stderr.on('data', chunk => { stderr += chunk; });
    await waitFor(() => stderr.includes(`Functions emulator ready on 127.0.0.1:${functionsPort}`) || child.exitCode !== null, 'Functions readiness');
    assert.equal(child.exitCode, null, stderr);
  };
  const records = () => fs.existsSync(eventLog)
    ? fs.readFileSync(eventLog, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line))
    : [];
  try {
    await start(30000);
    process.env.FIRESTORE_EMULATOR_HOST = `127.0.0.1:${firestorePort}`;
    process.env.GCLOUD_PROJECT = 'demo-functions-cli';
    const admin = require('firebase-admin');
    const app = admin.initializeApp({projectId: 'demo-functions-cli'}, `outbox-${Date.now()}`);
    await app.firestore().doc('items/outbox-restart').set({durable: true});
    await waitFor(() => records().filter(record => record.name === 'write').length === 1, 'first handler delivery');
    const eventId = records().find(record => record.name === 'write').value.eventId;
    assert.match(eventId, /^rust-\d+$/);
    await app.delete();
    await stop(child, true); child = null;

    await start(0);
    await waitFor(() => records().filter(record => record.name === 'write').length === 2, 'recovered duplicate delivery');
    const delivered = records().filter(record => record.name === 'write');
    assert.equal(delivered[1].value.eventId, eventId);
    const drain = await fetch(`http://127.0.0.1:${functionsPort}/__/functions/drain`);
    const status = await drain.json();
    assert.equal(drain.status, 200, `${JSON.stringify(status)}\n${stderr}`);
    assert.equal(status.pending, 0);
    assert.ok(status.completed >= 1);
    await stop(child); child = null;

    await start(0);
    await new Promise(resolve => setTimeout(resolve, 750));
    assert.equal(records().filter(record => record.name === 'write').length, 2, 'durable ACK must prevent a third delivery');
    process.env.FIRESTORE_EMULATOR_HOST = `127.0.0.1:${firestorePort}`;
    const loopApp = admin.initializeApp({projectId: 'demo-functions-cli'}, `loop-${Date.now()}`);
    await loopApp.firestore().doc('loop/bounded').set({n: 0});
    await waitFor(() => records().some(record => record.name === 'selfLoop'), 'self-trigger first delivery');
    const direct = await fetch(`http://127.0.0.1:${functionsPort}/__/functions/pubsub/fixture-topic`, {
      method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({data: {fair: true}}),
    });
    assert.equal(direct.status, 202);
    await waitFor(() => records().some(record => record.name === 'topic' && record.value.json?.fair === true), 'direct event fairness');
    await waitFor(async () => {
      const response = await fetch(`http://127.0.0.1:${functionsPort}/__/functions/status`);
      const value = await response.json();
      return value.pending === 0 && value.failures.some(failure => /self-trigger chain limit exceeded/.test(failure.error));
    }, 'bounded self-trigger failure');
    const loopDeliveries = records().filter(record => record.name === 'selfLoop');
    assert.equal(loopDeliveries.length, 40);
    assert.equal(new Set(loopDeliveries.map(record => record.value.eventId)).size, 40);
    await loopApp.delete();
    console.log(JSON.stringify({status: 'passed', deliveries: 2, stableEventId: eventId, selfTriggerDeliveries: loopDeliveries.length, directWorkNotStarved: true, guarantee: 'at-least-once duplicate at delivery/ack crash boundary'}));
  } finally {
    await stop(child).catch(() => {});
    fs.rmSync(temporary, {recursive: true, force: true});
  }
})().catch(error => { console.error(error.stack || error); process.exitCode = 1; });
