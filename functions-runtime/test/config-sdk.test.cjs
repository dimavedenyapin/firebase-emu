'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {spawn} = require('node:child_process');
const path = require('node:path');

test('firebase-functions reads nested local runtime config and FIREBASE_CONFIG', async t => {
  const runtimeConfig = {fixture: {nested: {enabled: true, limit: 7}}};
  const firebaseConfig = {
    projectId: 'demo-functions-config',
    storageBucket: 'demo-functions-config.appspot.com',
    databaseURL: 'http://127.0.0.1:9000?ns=demo-functions-config',
  };
  const child = spawn(process.execPath, [
    path.resolve(__dirname, '../adapter.cjs'),
    '--source', path.resolve(__dirname, '../fixtures/config'),
    '--project', 'demo-functions-config',
  ], {
    env: {
      PATH: process.env.PATH,
      CLOUD_RUNTIME_CONFIG: JSON.stringify(runtimeConfig),
      FIREBASE_CONFIG: JSON.stringify(firebaseConfig),
      LOCAL_FIXTURE_VALUE: 'from-demo-dotenv',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(async () => {
    if (child.exitCode === null) {
      child.kill('SIGTERM');
      await new Promise(resolve => child.once('exit', resolve));
    }
  });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const ready = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error(stderr || 'adapter readiness timed out')), 15000);
    let stdout = '';
    child.once('exit', code => {
      clearTimeout(timer);
      reject(Error(`adapter exited ${code}: ${stderr}`));
    });
    child.stdout.on('data', chunk => {
      stdout += chunk;
      const line = stdout.split('\n').find(item => item.startsWith('FIREBASE_EMU_READY '));
      if (line) {
        clearTimeout(timer);
        resolve(JSON.parse(line.slice('FIREBASE_EMU_READY '.length)));
      }
    });
  });
  const response = await fetch(`http://127.0.0.1:${ready.port}/invoke/readConfig`, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({data: null}),
  });
  assert.equal(response.status, 200, stderr);
  assert.deepEqual(await response.json(), {result: {
    nested: {enabled: true, limit: 7},
    projectId: 'demo-functions-config',
    databaseURL: 'http://127.0.0.1:9000?ns=demo-functions-config',
    localValue: 'from-demo-dotenv',
  }});
});
