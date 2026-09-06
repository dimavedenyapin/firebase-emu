'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {spawn} = require('node:child_process');
const {createServer} = require('node:net');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const repository = path.resolve(__dirname, '../..');
const binary = path.join(repository, 'target/release/firebase-emu');
const projectRoot = path.join(repository, 'functions-runtime/fixtures');
const node22 = process.env.FIREBASE_FUNCTIONS_NODE_22 || process.execPath;

async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', error => error ? reject(error) : resolve()));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
async function waitFor(predicate, label, timeoutMs = 20000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw Error(`Timed out waiting for ${label}`);
}

test('release binary supervises Node22 and dispatches real SDK traffic', {timeout: 120000}, async t => {
  assert.ok(fs.existsSync(binary), `release binary missing: ${binary}`);
  assert.ok(fs.existsSync(node22), `Node22 missing: ${node22}`);
  assert.equal(process.versions.node.split('.')[0], '22', 'run this test suite with Node 22');
  const [functionsPort, firestorePort, authPort, storagePort, databasePort, pubsubPort, pagePort, debugPort] = await Promise.all(Array.from({length: 8}, freePort));
  const suffix = `${process.pid}-${Date.now()}`;
  const eventLog = path.join(os.tmpdir(), `firebase-functions-events-${suffix}.jsonl`);
  const pidFile = path.join(os.tmpdir(), `firebase-functions-child-${suffix}.pid`);
  const configPidFile = path.join(os.tmpdir(), `firebase-functions-config-child-${suffix}.pid`);
  let stderr = '';
  let browser;
  const browserProfile = fs.mkdtempSync(path.join(os.tmpdir(), 'firebase-emu-brave-'));
  const staticServer = require('node:http').createServer((request, response) => {
    const bundles = {
      '/firebase-app.js': 'firebase-app-compat.js',
      '/firebase-functions.js': 'firebase-functions-compat.js',
      '/firebase-firestore.js': 'firebase-firestore-compat.js',
      '/firebase-auth.js': 'firebase-auth-compat.js',
    };
    if (bundles[request.url]) {
      response.setHeader('content-type', 'text/javascript');
      return fs.createReadStream(path.join(repository, 'functions-runtime/node_modules/firebase', bundles[request.url])).pipe(response);
    }
    response.setHeader('content-type', 'text/html');
    response.end(`<!doctype html><body>running<script src="/firebase-app.js"></script><script src="/firebase-functions.js"></script><script src="/firebase-firestore.js"></script><script src="/firebase-auth.js"></script><script>
      (async()=>{try{
        firebase.initializeApp({projectId:'demo-functions-cli',apiKey:'fake'});
        firebase.auth().useEmulator('http://127.0.0.1:${authPort}');
        const credential=await firebase.auth().createUserWithEmailAndPassword('callable-${suffix}@example.test','secret123');
        firebase.functions().useEmulator('127.0.0.1',${functionsPort});
        const callable=(await firebase.functions().httpsCallable('echo')({from:'browser'})).data;
        const db=firebase.firestore(); db.useEmulator('127.0.0.1',${firestorePort});
        const ref=db.doc('items/browser'); await ref.set({source:'browser',count:3}); await ref.update({count:4}); await ref.delete();
        document.body.textContent=JSON.stringify({ok:true,callable,authUid:credential.user.uid});
      }catch(error){document.body.textContent=JSON.stringify({ok:false,error:String(error),stack:error.stack});}})();
    </script>`);
  });
  await new Promise(resolve => staticServer.listen(pagePort, '127.0.0.1', resolve));
  const child = spawn(binary, ['--config', projectRoot, '--project', 'demo-functions-cli', '--runtime-config', '{"fixture":{"nested":{"enabled":true,"limit":9}}}'], {
    cwd: repository,
    env: {
      PATH: process.env.PATH,
      FIREBASE_FUNCTIONS_NODE_22: node22,
      FIREBASE_FUNCTIONS_EMU_PORT: String(functionsPort),
      FIRESTORE_EMU_PORT: String(firestorePort),
      FIREBASE_AUTH_EMU_PORT: String(authPort),
      FIREBASE_STORAGE_EMU_PORT: String(storagePort),
      FIREBASE_DATABASE_EMU_PORT: String(databasePort),
      PUBSUB_EMULATOR_PORT: String(pubsubPort),
      FIXTURE_EVENT_LOG: eventLog,
      FIXTURE_PID_FILE: pidFile,
      FIXTURE_CONFIG_PID_FILE: configPidFile,
    },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  child.stderr.on('data', chunk => { stderr += chunk; });
  t.after(async () => {
    if (browser?.exitCode === null) {
      browser.kill('SIGTERM');
      await new Promise(resolve => browser.once('exit', resolve));
    }
    await new Promise(resolve => staticServer.close(resolve));
    if (child.exitCode === null) {
      child.kill('SIGTERM');
      await new Promise(resolve => child.once('exit', resolve));
    }
    for (const file of [eventLog, pidFile, configPidFile]) {
      try { fs.unlinkSync(file); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
    fs.rmSync(browserProfile, {recursive: true, force: true, maxRetries: 5, retryDelay: 50});
  });
  await waitFor(() => stderr.includes(`Functions emulator ready on 127.0.0.1:${functionsPort}`), 'binary readiness');
  assert.ok(fs.existsSync(pidFile), stderr);
  const nodePid = Number(fs.readFileSync(pidFile, 'utf8'));
  assert.ok(fs.existsSync(configPidFile), stderr);
  const configNodePid = Number(fs.readFileSync(configPidFile, 'utf8'));
  const base = `http://127.0.0.1:${functionsPort}`;
  const browserOrigin = 'http://127.0.0.1:35173';
  const preflight = await fetch(`${base}/__/functions/drain`, {
    method: 'OPTIONS',
    headers: {origin: browserOrigin, 'access-control-request-method': 'GET'},
  });
  assert.equal(preflight.status, 200, stderr);
  assert.equal(preflight.headers.get('access-control-allow-origin'), '*');
  assert.match(preflight.headers.get('access-control-allow-methods') || '', /(^|,\s*)GET(,|$)/);
  const browserDrain = await fetch(`${base}/__/functions/drain`, {headers: {origin: browserOrigin}});
  const browserDrainBody = await browserDrain.json();
  assert.equal(browserDrain.status, 200, `${JSON.stringify(browserDrainBody)}\n${stderr}`);
  assert.equal(browserDrain.headers.get('access-control-allow-origin'), '*');
  assert.deepEqual(browserDrainBody, {completed: 0, failures: [], pending: 0});
  const drain = async expected => {
    const response = await fetch(`${base}/__/functions/drain`);
    assert.equal(response.status, expected, `${await response.text()}\n${stderr}`);
  };
  const post = (url, body) => fetch(base + url, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(body)});

  const http = await post('/demo-functions-cli/us-central1/http/nested?q=one', {safe: true});
  assert.equal(http.status, 201, stderr);
  assert.deepEqual(await http.json(), {method: 'POST', path: '/nested', query: {q: 'one'}, body: {safe: true}, raw: '{"safe":true}'});

  assert.deepEqual((await (await post('/demo-functions-cli/us-central1/readConfig', {data: null})).json()).result, {
    nested: {enabled: true, limit: 9}, projectId: 'demo-functions-cli',
    databaseURL: `http://127.0.0.1:${databasePort}?ns=demo-functions-cli`, localValue: 'from-demo-dotenv',
  });

  process.env.FIRESTORE_EMULATOR_HOST = `127.0.0.1:${firestorePort}`;
  process.env.FIREBASE_STORAGE_EMULATOR_HOST = `127.0.0.1:${storagePort}`;
  process.env.GCLOUD_PROJECT = 'demo-functions-cli';
  const admin = require('firebase-admin');
  const adminApp = admin.initializeApp({projectId: 'demo-functions-cli', storageBucket: 'demo-functions-cli.appspot.com'}, `admin-${suffix}`);
  const db = adminApp.firestore();
  await db.doc('items/node').set({source: 'node', count: 1});
  await db.doc('items/node').update({count: 2});
  await db.doc('items/node').delete();
  await db.doc('async/waited').set({done: true});

  browser = spawn('/Applications/Brave Browser.app/Contents/MacOS/Brave Browser', [
    '--headless=new', '--no-first-run', '--disable-background-networking', '--disable-component-update',
    `--remote-debugging-port=${debugPort}`, `--user-data-dir=${browserProfile}`, `http://127.0.0.1:${pagePort}/`,
  ], {stdio: 'ignore'});
  let page;
  await waitFor(async () => {
    try {
      const pages = await (await fetch(`http://127.0.0.1:${debugPort}/json/list`)).json();
      page = pages.find(item => item.type === 'page'); return Boolean(page);
    } catch { return false; }
  }, 'Brave DevTools page');
  const socket = new WebSocket(page.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, {once: true}); socket.addEventListener('error', reject, {once: true}); });
  let commandId = 0;
  const pending = new Map();
  socket.addEventListener('message', event => { const message = JSON.parse(event.data); if (pending.has(message.id)) { pending.get(message.id)(message); pending.delete(message.id); } });
  const cdp = (method, params = {}) => new Promise(resolve => { const id = ++commandId; pending.set(id, resolve); socket.send(JSON.stringify({id, method, params})); });
  let browserResult;
  await waitFor(async () => {
    const message = await cdp('Runtime.evaluate', {expression: 'document.body.textContent', returnByValue: true});
    const text = message.result?.result?.value;
    if (!text?.startsWith('{')) return false;
    browserResult = JSON.parse(text); return true;
  }, 'browser Firebase SDK writes');
  socket.close();
  assert.equal(browserResult.ok, true, browserResult.error);
  assert.deepEqual(browserResult.callable.data, {from: 'browser'});
  assert.equal(browserResult.callable.uid, browserResult.authUid);
  await drain(200);

  const bucket = adminApp.storage().bucket();
  await bucket.file('incoming/safe.txt').save(Buffer.from('safe fixture'));
  await bucket.file('incoming/safe.txt').delete();
  assert.equal((await post('/__/functions/pubsub/fixture-topic', {data: {payment: 42}, attributes: {tenant: 'fixture'}})).status, 202);
  assert.equal((await post('/__/functions/schedule/schedule', {})).status, 202);
  await drain(200);

  const records = fs.readFileSync(eventLog, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  const named = name => records.filter(record => record.name === name).map(record => record.value);
  assert.deepEqual(named('create').map(value => value.param).sort(), ['browser', 'node']);
  assert.deepEqual(named('update').find(value => value.param === 'node'), {before: {source: 'node', count: 1}, after: {source: 'node', count: 2}, param: 'node'});
  assert.deepEqual(named('remove').map(value => value.param).sort(), ['browser', 'node']);
  assert.deepEqual(named('asyncCreate'), [{data: {done: true}, param: 'waited'}]);
  assert.equal(named('write').length, 6);
  assert.deepEqual(named('topic')[0], {json: {payment: 42}, attributes: {tenant: 'fixture'}});
  assert.equal(named('schedule').length, 1);
  assert.equal(named('finalize')[0].name, 'incoming/safe.txt');
  assert.equal(named('storageDelete')[0].name, 'incoming/safe.txt');

  await db.doc('bad/error').set({fail: true});
  const failed = await fetch(`${base}/__/functions/drain`);
  assert.equal(failed.status, 500);
  const failedStatus = await failed.json();
  assert.equal(failedStatus.pending, 0);
  assert.ok(failedStatus.failures.some(item => item.error.includes('event failure')));

  await adminApp.delete();
  child.kill('SIGTERM');
  await new Promise(resolve => child.once('exit', resolve));
  assert.equal(child.exitCode, 0, stderr);
  await waitFor(() => {
    try { process.kill(nodePid, 0); return false; } catch (error) { return error.code === 'ESRCH'; }
  }, 'Node child shutdown');
  await waitFor(() => {
    try { process.kill(configNodePid, 0); return false; } catch (error) { return error.code === 'ESRCH'; }
  }, 'second codebase Node child shutdown');
});
