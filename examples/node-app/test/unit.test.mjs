import assert from 'node:assert/strict';
import test from 'node:test';
import { Firestore } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';
import { initializeApp } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';
import { createRecordApp } from '../src/app.mjs';
import { readConfig } from '../src/config.mjs';

function fakeClients() {
  const writes = [];
  const document = {
    create: async value => writes.push(['create', value]),
    get: async () => ({ exists: true, data: () => ({ label: 'saved', rank: 1, currency: 'USD' }) }),
    set: async (value, options) => writes.push(['set', value, options]),
    update: async value => writes.push(['update', value]),
    delete: async () => writes.push(['delete'])
  };
  const query = { docs: [] };
  const collection = {
    doc: () => document,
    limit: () => ({ get: async () => query }),
    where: () => ({ orderBy: () => ({ limit: () => ({ get: async () => query }) }) })
  };
  const batch = { set: (ref, value) => writes.push(['batch-set', ref, value]), commit: async () => writes.push(['commit']) };
  const db = { collection: () => collection, batch: () => batch };
  const auth = {
    createUser: async value => value,
    getUser: async uid => ({ uid }),
    updateUser: async (uid, value) => ({ uid, ...value }),
    listUsers: async () => ({ users: [] }),
    deleteUser: async () => undefined
  };
  const file = { save: async value => writes.push(['upload', value.toString()]), download: async () => [Buffer.from('body')], delete: async () => writes.push(['file-delete']) };
  const bucket = { file: () => file, getFiles: async () => [[{ name: 'b' }, { name: 'a' }]] };
  return { db, auth, bucket, writes };
}

test('current SDK entry points load with supported ESM import patterns', () => {
  assert.equal(typeof Firestore, 'function');
  assert.equal(typeof Storage, 'function');
  assert.equal(typeof initializeApp, 'function');
  assert.equal(typeof getAuth, 'function');
});

test('configuration accepts explicit loopback emulator endpoints', () => {
  assert.deepEqual(readConfig({
    GCLOUD_PROJECT: 'demo-node-unit',
    FIRESTORE_EMULATOR_HOST: '127.0.0.1:8080',
    FIREBASE_AUTH_EMULATOR_HOST: 'localhost:9099',
    FIREBASE_STORAGE_EMULATOR_HOST: '127.0.0.1:9199'
  }), {
    projectId: 'demo-node-unit',
    firestoreHost: '127.0.0.1:8080',
    authHost: 'localhost:9099',
    storageEndpoint: 'http://127.0.0.1:9199',
    bucket: 'demo-node-unit.appspot.com'
  });
});

test('configuration rejects production projects and remote endpoints', () => {
  assert.throws(() => readConfig({ GCLOUD_PROJECT: 'production' }), /demo-/);
  assert.throws(() => readConfig({
    GCLOUD_PROJECT: 'demo-node',
    FIRESTORE_EMULATOR_HOST: 'example.com:8080',
    FIREBASE_AUTH_EMULATOR_HOST: 'localhost:9099',
    STORAGE_EMULATOR_HOST: 'http://localhost:9199'
  }), /loopback/);
});

test('records are normalized and validated', async () => {
  const clients = fakeClients();
  const app = createRecordApp(clients);
  await app.create('one', { label: ' Example ', rank: 2, currency: 'USD' });
  assert.deepEqual(clients.writes[0], ['create', { label: 'Example', rank: 2, currency: 'USD' }]);
  await assert.rejects(app.create('two', { label: 'Bad', rank: 2, currency: 'US' }), /three-letter uppercase/);
  await assert.rejects(app.create('bad/id', { label: 'Bad', rank: 2, currency: 'USD' }), /without \/$/);
});

test('the app delegates Auth and Storage operations', async () => {
  const clients = fakeClients();
  const app = createRecordApp(clients);
  assert.deepEqual(await app.getUser('u1'), { uid: 'u1' });
  await app.upload('folder/file.txt', 'body');
  assert.equal(await app.download('folder/file.txt'), 'body');
  assert.deepEqual(await app.listFiles('folder/'), ['a', 'b']);
});
