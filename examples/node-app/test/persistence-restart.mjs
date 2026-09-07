import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { FieldValue, Firestore, GeoPoint, Timestamp } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';
import { deleteApp, initializeApp } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';

const here = path.dirname(fileURLToPath(import.meta.url));
const repository = path.resolve(here, '../../..');
const binary = path.resolve(process.env.FIREBASE_EMU_BINARY || path.join(repository, 'target/release/firebase-emu'));
const project = 'demo-persistence-restart';
const otherProject = 'demo-persistence-other';
const bucketName = `${project}.appspot.com`;

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(error => error ? reject(error) : resolve(port));
    });
  });
}

const ports = {
  firestore: await freePort(),
  auth: await freePort(),
  storage: await freePort(),
};
const environment = {
  ...process.env,
  GCLOUD_PROJECT: project,
  FIRESTORE_EMULATOR_HOST: `127.0.0.1:${ports.firestore}`,
  FIREBASE_AUTH_EMULATOR_HOST: `127.0.0.1:${ports.auth}`,
  FIREBASE_STORAGE_EMULATOR_HOST: `127.0.0.1:${ports.storage}`,
  STORAGE_EMULATOR_HOST: `http://127.0.0.1:${ports.storage}`,
  FIRESTORE_EMU_PORT: String(ports.firestore),
  FIREBASE_AUTH_EMU_PORT: String(ports.auth),
  FIREBASE_STORAGE_EMU_PORT: String(ports.storage),
};
Object.assign(process.env, {
  GCLOUD_PROJECT: environment.GCLOUD_PROJECT,
  FIRESTORE_EMULATOR_HOST: environment.FIRESTORE_EMULATOR_HOST,
  FIREBASE_AUTH_EMULATOR_HOST: environment.FIREBASE_AUTH_EMULATOR_HOST,
  FIREBASE_STORAGE_EMULATOR_HOST: environment.FIREBASE_STORAGE_EMULATOR_HOST,
  STORAGE_EMULATOR_HOST: environment.STORAGE_EMULATOR_HOST,
});

function waitPort(port, child, timeout = 20_000) {
  const deadline = Date.now() + timeout;
  return new Promise((resolve, reject) => {
    const attempt = () => {
      if (child.exitCode !== null) return reject(new Error(`firebase-emu exited ${child.exitCode}: ${child.stderrText}`));
      const socket = net.connect({ host: '127.0.0.1', port });
      socket.once('connect', () => { socket.destroy(); resolve(); });
      socket.once('error', () => {
        socket.destroy();
        if (Date.now() >= deadline) reject(new Error(`timed out waiting for port ${port}: ${child.stderrText}`));
        else setTimeout(attempt, 25);
      });
    };
    attempt();
  });
}

async function start(args) {
  const child = spawn(binary, [...args, '--no-functions'], {
    cwd: repository,
    env: environment,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stderrText = '';
  child.stderr.on('data', chunk => { child.stderrText += chunk; });
  await Promise.all(Object.values(ports).map(port => waitPort(port, child)));
  return child;
}

async function stop(child, crash = false) {
  if (!child || child.exitCode !== null) return;
  child.kill(crash && process.platform !== 'win32' ? 'SIGKILL' : 'SIGTERM');
  await Promise.race([
    new Promise(resolve => child.once('exit', resolve)),
    delay(10_000).then(() => { child.kill('SIGKILL'); throw new Error('firebase-emu did not stop'); }),
  ]);
}

let appSequence = 0;
function clients(selectedProject = project) {
  const db = new Firestore({
    projectId: selectedProject,
    host: environment.FIRESTORE_EMULATOR_HOST,
    ssl: false,
    ignoreUndefinedProperties: true,
  });
  const storage = new Storage({
    projectId: selectedProject,
    apiEndpoint: environment.STORAGE_EMULATOR_HOST,
    useAuthWithCustomEndpoint: false,
    retryOptions: { autoRetry: false },
  });
  const app = initializeApp({ projectId: selectedProject }, `persistence-${++appSequence}`);
  return {
    db,
    auth: getAuth(app),
    bucket: storage.bucket(bucketName),
    async close() { await Promise.all([db.terminate(), deleteApp(app)]); },
  };
}

async function identity(method, body) {
  const response = await fetch(`http://127.0.0.1:${ports.auth}/identitytoolkit.googleapis.com/v1/${method}?key=demo`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  });
  const json = await response.json();
  if (!response.ok) throw new Error(`${method} failed ${response.status}: ${JSON.stringify(json)}`);
  return json;
}

async function refresh(token) {
  const response = await fetch(`http://127.0.0.1:${ports.auth}/securetoken.googleapis.com/v1/token?key=demo`, {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: token }),
  });
  const json = await response.json();
  if (!response.ok) throw new Error(`refresh failed ${response.status}: ${JSON.stringify(json)}`);
  return json;
}

if (!fs.existsSync(binary)) throw new Error(`release binary not found: ${binary}`);
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'firebase emu persistence restart '));
const dataDir = path.join(root, 'data with spaces');
let processHandle;
let first;
try {
  processHandle = await start(['--data-dir', dataDir]);
  const duplicateOwner = spawn(binary, ['--data-dir', dataDir, '--no-functions'], {
    cwd: repository, env: environment, stdio: ['ignore', 'ignore', 'pipe'],
  });
  let duplicateError = '';
  duplicateOwner.stderr.on('data', chunk => { duplicateError += chunk; });
  const duplicateCode = await new Promise(resolve => duplicateOwner.once('exit', resolve));
  assert.notEqual(duplicateCode, 0);
  assert.match(duplicateError, /already owned by another firebase-emu process/);
  first = clients();
  const typed = {
    int64: 9_007_199_254_740_991,
    timestamp: Timestamp.fromMillis(1_700_000_000_123),
    bytes: Buffer.from([0, 1, 2, 253, 254, 255]),
    reference: first.db.doc('references/target'),
    point: new GeoPoint(13.7563, 100.5018),
    nested: { array: [1, 'two', { ok: true }] },
    nan: Number.NaN,
  };
  await first.db.doc('typed/main').set(typed);
  await first.db.doc('query/a').set({ active: true, rank: 2 });
  await first.db.doc('query/b').set({ active: true, rank: 1 });
  const batch = first.db.batch();
  batch.set(first.db.doc('atomic/one'), { batch: 'committed' });
  batch.set(first.db.doc('atomic/two'), { batch: 'committed' });
  await batch.commit();

  await first.db.doc('counter/value').set({ count: 0 });
  await Promise.all(Array.from({ length: 40 }, () =>
    first.db.doc('counter/value').update({ count: FieldValue.increment(1) })));
  assert.equal((await first.db.doc('counter/value').get()).get('count'), 40);

  const failed = first.db.batch();
  failed.set(first.db.doc('failed/visible'), { shouldNotExist: true });
  failed.update(first.db.doc('failed/missing'), { value: 1 });
  await assert.rejects(failed.commit());
  assert.equal((await first.db.doc('failed/visible').get()).exists, false);

  await first.auth.createUser({ uid: 'durable-user', email: 'durable@example.test', password: 'test-password' });
  await first.auth.setCustomUserClaims('durable-user', { role: 'admin', level: 7 });
  const signedIn = await identity('accounts:signInWithPassword', {
    email: 'durable@example.test', password: 'test-password', returnSecureToken: true,
  });
  const beforeRestartIdToken = signedIn.idToken;
  const beforeRestartRefreshToken = signedIn.refreshToken;

  const objectBytes = Buffer.concat([Buffer.from('binary\0payload:'), Buffer.from([0, 255, 17, 34])]);
  await first.bucket.file('../unsafe/name.bin').save(objectBytes, {
    resumable: false,
    metadata: { contentType: 'application/octet-stream', metadata: { fixture: 'restart' } },
  });
  await first.close(); first = null;
  await stop(processHandle, true); processHandle = null;

  processHandle = await start(['--data-dir', dataDir]);
  const second = clients();
  const snapshot = await second.db.doc('typed/main').get();
  assert.equal(snapshot.get('int64'), typed.int64);
  assert.equal(snapshot.get('timestamp').toMillis(), typed.timestamp.toMillis());
  assert.deepEqual(snapshot.get('bytes'), typed.bytes);
  assert.equal(snapshot.get('reference').path, 'references/target');
  assert.deepEqual(snapshot.get('point'), typed.point);
  assert.deepEqual(snapshot.get('nested'), typed.nested);
  assert.ok(Number.isNaN(snapshot.get('nan')));
  assert.deepEqual((await second.db.collection('query').where('active', '==', true).orderBy('rank').get()).docs.map(doc => doc.id), ['b', 'a']);
  assert.equal((await second.db.collection('atomic').get()).size, 2);
  assert.equal((await second.db.doc('counter/value').get()).get('count'), 40);

  const user = await second.auth.getUser('durable-user');
  assert.deepEqual(user.customClaims, { role: 'admin', level: 7 });
  const lookup = await identity('accounts:lookup', { idToken: beforeRestartIdToken });
  assert.equal(lookup.users[0].localId, 'durable-user');
  const refreshed = await refresh(beforeRestartRefreshToken);
  assert.equal(refreshed.user_id, 'durable-user');
  const [download] = await second.bucket.file('../unsafe/name.bin').download();
  assert.deepEqual(download, objectBytes);
  const [metadata] = await second.bucket.file('../unsafe/name.bin').getMetadata();
  assert.equal(metadata.metadata.fixture, 'restart');

  const other = clients(otherProject);
  await other.db.doc('isolation/kept').set({ project: otherProject });
  const clear = await fetch(`http://127.0.0.1:${ports.firestore}/emulator/v1/projects/${project}/databases/(default)/documents`, { method: 'DELETE' });
  assert.equal(clear.status, 200);
  assert.equal((await second.db.doc('typed/main').get()).exists, false);
  assert.equal((await other.db.doc('isolation/kept').get()).exists, true);
  await second.auth.deleteUser('durable-user');
  await second.bucket.file('../unsafe/name.bin').delete();
  await Promise.all([second.close(), other.close()]);
  await stop(processHandle, true); processHandle = null;

  processHandle = await start(['--data-dir', dataDir]);
  const third = clients();
  assert.equal((await third.db.doc('typed/main').get()).exists, false);
  await assert.rejects(third.auth.getUser('durable-user'), error => error.code === 'auth/user-not-found');
  assert.equal((await third.bucket.file('../unsafe/name.bin').exists())[0], false);
  const otherAfterRestart = clients(otherProject);
  assert.equal((await otherAfterRestart.db.doc('isolation/kept').get()).exists, true);
  await Promise.all([third.close(), otherAfterRestart.close()]);
  await stop(processHandle); processHandle = null;

  processHandle = await start(['--in-memory']);
  const ephemeral = clients();
  await ephemeral.db.doc('ephemeral/gone').set({ value: true });
  await ephemeral.close();
  await stop(processHandle, true); processHandle = null;
  processHandle = await start(['--in-memory']);
  const ephemeralRestart = clients();
  assert.equal((await ephemeralRestart.db.doc('ephemeral/gone').get()).exists, false);
  await ephemeralRestart.close();
  await stop(processHandle); processHandle = null;

  console.log(JSON.stringify({
    status: 'passed',
    assertions: 26,
    modes: ['sqlite-wal', 'in-memory'],
    sdks: ['@google-cloud/firestore 7.11.6', 'firebase-admin 11.11.1', '@google-cloud/storage 7.7.0', 'Identity Toolkit REST'],
  }));
} finally {
  await first?.close().catch(() => {});
  await stop(processHandle).catch(() => {});
  fs.rmSync(root, { recursive: true, force: true });
}
