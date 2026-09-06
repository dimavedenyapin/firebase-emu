import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { connect } from '../src/sdk.mjs';
import { validateCurrency } from '../src/app.mjs';

const target = process.env.TARGET_NAME ?? 'configured-emulator';
const strict = !process.argv.includes('--probe');
const runId = randomUUID().replaceAll('-', '').slice(0, 12);
const collection = `node-sdk-${runId}`;
const objectName = `node-sdk/${runId}.txt`;
const uid = `node-${runId}`;
const results = [];
let connection;

function bounded(promise, ms = 6000) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Operation timed out after ${ms} ms`)), ms); })
  ]).finally(() => clearTimeout(timer));
}

async function check(service, name, action, dependency) {
  const required = dependency && results.find(item => item.name === dependency);
  if (required && required.status !== 'passed') {
    results.push({ service, name, status: 'blocked', error: `Requires ${dependency}` });
    return;
  }
  const started = Date.now();
  try {
    await bounded(Promise.resolve().then(action));
    results.push({ service, name, status: 'passed', durationMs: Date.now() - started });
  } catch (error) {
    results.push({ service, name, status: 'failed', error: error.message, code: error.code ?? null, durationMs: Date.now() - started });
  }
}

try {
  connection = connect(collection);
  const { app } = connection;

  await check('fixture', 'fixture.currency.valid', () => assert.equal(validateCurrency('USD'), 'USD'));
  await check('fixture', 'fixture.currency.invalid', () => assert.throws(() => validateCurrency('US')));

  await check('firestore', 'firestore.create', () => app.create('first', { label: 'First', rank: 1, currency: 'USD' }));
  await check('firestore', 'firestore.read', async () => {
    assert.deepEqual(await app.read('first'), { label: 'First', rank: 1, currency: 'USD' });
  }, 'firestore.create');

  // These capability checks own their setup and cleanup. A failure in one must
  // not prevent the other deliberately unsupported Firestore calls from being measured.
  await check('firestore', 'firestore.partial-merge', async () => {
    const id = 'partial-merge';
    const transformId = 'field-transforms';
    try {
      await app.create(id, { label: 'Preserved', rank: 10, currency: 'USD' });
      await app.update(id, { rank: 11 });
      assert.deepEqual(await app.read(id), { label: 'Preserved', rank: 11, currency: 'USD' });

      await app.merge(transformId, {
        rank: 1, tags: ['seed'], obsolete: true, preserved: 'yes',
        nested: { left: 1, right: 2 }, label: 'Transforms', currency: 'USD',
      });
      await app.transformUpdate(transformId);
      const transformed = await app.read(transformId);
      assert.equal(transformed.rank, 5);
      assert.deepEqual(transformed.tags, ['seed', 'updated']);
      assert.equal('obsolete' in transformed, false);
      assert.equal(transformed.preserved, 'yes');
      assert.deepEqual(transformed.nested, { left: 9, right: 2 });
      assert.equal(typeof transformed.updatedAt?.toDate, 'function');
    } finally {
      await Promise.allSettled([id, transformId].map(value => bounded(app.remove(value), 2000)));
    }
  });
  await check('firestore', 'firestore.where-order-limit', async () => {
    const ids = ['query-low', 'query-mid', 'query-high'];
    try {
      await app.batchCreate([
        { id: ids[0], label: 'Low', rank: 31, currency: 'USD' },
        { id: ids[1], label: 'Mid', rank: 33, currency: 'USD' },
        { id: ids[2], label: 'High', rank: 35, currency: 'USD' }
      ]);
      assert.deepEqual((await app.search(30)).map(item => item.id), ['query-high', 'query-mid']);
    } finally {
      await Promise.allSettled(ids.map(id => bounded(app.remove(id), 2000)));
    }
  });
  await check('firestore', 'firestore.listener', async () => {
    const id = 'listener';
    let stop;
    try {
      await app.create(id, { label: 'Listener', rank: 20, currency: 'USD' });
      let nextValue;
      const initial = new Promise((resolve, reject) => {
        nextValue = resolve;
        stop = app.listen(id, value => nextValue(value), reject);
      });
      assert.deepEqual(await bounded(initial, 2000), { label: 'Listener', rank: 20, currency: 'USD' });
      const changed = new Promise(resolve => { nextValue = resolve; });
      await app.update(id, { rank: 21 });
      assert.deepEqual(await bounded(changed, 2000), { label: 'Listener', rank: 21, currency: 'USD' });
    } finally {
      stop?.();
      await bounded(app.remove(id), 2000).catch(() => {});
    }
  });
  await check('firestore', 'firestore.batch-commit', () => app.batchCreate([
    { id: 'second', label: 'Second', rank: 3, currency: 'THB' },
    { id: 'third', label: 'Third', rank: 4, currency: 'EUR' }
  ]));
  await check('firestore', 'firestore.list', async () => assert.equal((await app.list()).length, 3), 'firestore.batch-commit');
  await check('firestore', 'firestore.delete', async () => {
    await app.remove('first');
    assert.equal(await app.read('first'), null);
  }, 'firestore.create');

  await check('auth', 'auth.createUser', async () => assert.equal((await app.createUser({ uid, email: `${uid}@example.test`, displayName: 'Node SDK probe' })).uid, uid));
  await check('auth', 'auth.getUser', async () => assert.equal((await app.getUser(uid)).email, `${uid}@example.test`), 'auth.createUser');
  await check('auth', 'auth.updateUser', async () => assert.equal((await app.updateUser(uid, { displayName: 'Updated probe' })).displayName, 'Updated probe'), 'auth.createUser');
  await check('auth', 'auth.listUsers', async () => assert.ok((await app.listUsers()).users.some(user => user.uid === uid)), 'auth.createUser');
  await check('auth', 'auth.deleteUser', () => app.deleteUser(uid), 'auth.createUser');

  await check('storage', 'storage.upload', () => app.upload(objectName, 'node-sdk-body', false));
  await check('storage', 'storage.download', async () => assert.equal(await app.download(objectName), 'node-sdk-body'), 'storage.upload');
  await check('storage', 'storage.list-prefix', async () => assert.ok((await app.listFiles('node-sdk/')).includes(objectName)), 'storage.upload');
  await check('storage', 'storage.delete', () => app.deleteFile(objectName), 'storage.upload');
} catch (error) {
  results.push({ service: 'harness', name: 'connection', status: 'failed', error: error.stack ?? String(error) });
} finally {
  if (connection) await bounded(connection.close(), 5000).catch(() => {});
}

const report = {
  sdk: 'node',
  target,
  versions: { firebaseAdmin: '11.11.1', firestore: '7.11.6', storage: '7.7.0' },
  results
};
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (strict && results.some(item => item.status !== 'passed')) process.exitCode = 1;
