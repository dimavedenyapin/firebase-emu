'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const admin = require('firebase-admin');

// The official emulator intentionally loads the fixture's .env.local.
const eventLog = '/tmp/firebase-emu-functions-events.jsonl';
try { fs.unlinkSync(eventLog); } catch (error) { if (error.code !== 'ENOENT') throw error; }
const app = admin.initializeApp({projectId: 'demo-functions'}, 'official-baseline');
const db = app.firestore();
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

(async () => {
  const callable = await fetch('http://127.0.0.1:15001/demo-functions/us-central1/echo', {
    method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({data: {baseline: true}}),
  });
  assert.equal(callable.status, 200);
  assert.deepEqual(await callable.json(), {result: {data: {baseline: true}, uid: null}});
  const http = await fetch('http://127.0.0.1:15001/demo-functions/us-central1/http/nested?q=baseline', {
    method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({safe: true}),
  });
  assert.equal(http.status, 201);
  assert.equal((await http.json()).path, '/nested');
  await db.doc('items/official').set({count: 1});
  await db.doc('items/official').update({count: 2});
  await db.doc('items/official').delete();
  for (let attempt = 0; attempt < 100; attempt++) {
    if (fs.existsSync(eventLog) && fs.readFileSync(eventLog, 'utf8').trim().split('\n').length >= 6) break;
    await sleep(50);
  }
  const records = fs.readFileSync(eventLog, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  const named = name => records.filter(record => record.name === name).map(record => record.value);
  assert.deepEqual(named('create'), [{data: {count: 1}, param: 'official', exists: true}]);
  assert.deepEqual(named('update'), [{before: {count: 1}, after: {count: 2}, param: 'official'}]);
  assert.deepEqual(named('remove'), [{data: {count: 2}, param: 'official', exists: true}]);
  assert.equal(named('write').length, 3);
  await app.delete();
  console.log('official Functions baseline: callable, HTTP, create/update/delete/write 8/8 passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
