import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repository = path.resolve(root, '../..');
const binary = path.resolve(process.env.FIREBASE_EMU_BINARY || path.join(repository, 'target/release/firebase-emu'));
const dataRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'firebase browser persistence '));
const dataDir = path.join(dataRoot, 'data with spaces');
const project = 'demo-browser-persistence';

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer().unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(error => error ? reject(error) : resolve(port));
    });
  });
}
const ports = { firestore: await freePort(), auth: await freePort(), storage: await freePort() };
const settings = {
  VITE_FIREBASE_projectId: project,
  VITE_FIREBASE_AUTH_EMULATOR_HOST: `127.0.0.1:${ports.auth}`,
  VITE_FIRESTORE_EMULATOR_HOST: `127.0.0.1:${ports.firestore}`,
  VITE_FIREBASE_STORAGE_EMULATOR_HOST: '127.0.0.1',
  VITE_FIREBASE_STORAGE_EMULATOR_PORT: String(ports.storage),
};
const environment = {
  ...process.env,
  GCLOUD_PROJECT: project,
  FIRESTORE_EMU_PORT: String(ports.firestore),
  FIREBASE_AUTH_EMU_PORT: String(ports.auth),
  FIREBASE_STORAGE_EMU_PORT: String(ports.storage),
};

async function start() {
  const child = spawn(binary, ['--data-dir', dataDir, '--no-functions'], { cwd: repository, env: environment, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const deadline = Date.now() + 20_000;
  for (const port of Object.values(ports)) {
    while (true) {
      if (child.exitCode !== null) throw new Error(`firebase-emu exited ${child.exitCode}: ${stderr}`);
      const connected = await new Promise(resolve => {
        const socket = net.connect({ host: '127.0.0.1', port });
        socket.once('connect', () => { socket.destroy(); resolve(true); });
        socket.once('error', () => { socket.destroy(); resolve(false); });
      });
      if (connected) break;
      if (Date.now() > deadline) throw new Error(`timed out waiting for ${port}: ${stderr}`);
      await delay(25);
    }
  }
  return child;
}

async function stop(child, crash = false) {
  if (!child || child.exitCode !== null) return;
  child.kill(crash && process.platform !== 'win32' ? 'SIGKILL' : 'SIGTERM');
  await new Promise(resolve => child.once('exit', resolve));
}

async function pageClient(browser, vite) {
  const page = await browser.newPage();
  page.setDefaultTimeout(10_000);
  await page.goto(vite.resolvedUrls.local[0]);
  await page.evaluate(async settings => {
    const { createClient } = await import('/src/firebase.js');
    window.persistenceClient = createClient(settings);
  }, settings);
  return page;
}

let emulator;
let vite;
let browser;
try {
  assert.ok(fs.existsSync(binary), `release binary not found: ${binary}`);
  vite = await createServer({ root, server: { host: '127.0.0.1', port: 0 }, logLevel: 'silent' });
  await vite.listen();
  browser = await chromium.launch({
    headless: true,
    ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE } : {}),
  });
  emulator = await start();
  let page = await pageClient(browser, vite);
  await page.evaluate(async () => {
    await window.persistenceClient.signUp('browser.restart@example.test', 'test-password');
    await window.persistenceClient.write('survives-restart', { title: 'Browser durable', active: true, count: 77 });
    await window.persistenceClient.upload('browser-restart.bin', new Uint8Array([0, 1, 2, 253, 254, 255]));
    await window.persistenceClient.signOut();
  });
  await page.close();
  await stop(emulator, true); emulator = null;

  emulator = await start();
  page = await pageClient(browser, vite);
  const result = await page.evaluate(async () => {
    const auth = await window.persistenceClient.signIn('browser.restart@example.test', 'test-password');
    const document = await window.persistenceClient.read('survives-restart');
    const query = await window.persistenceClient.query(70);
    const bytes = Array.from(await window.persistenceClient.downloadBytes('browser-restart.bin'));
    return { uid: auth.user.uid, document, query: query.map(item => item.id), bytes };
  });
  assert.ok(result.uid);
  assert.deepEqual(result.document, { title: 'Browser durable', active: true, count: 77 });
  assert.deepEqual(result.query, ['survives-restart']);
  assert.deepEqual(result.bytes, [0, 1, 2, 253, 254, 255]);
  await page.close();
  console.log(JSON.stringify({ status: 'passed', assertions: 4, sdk: 'Firebase browser 12.12.1' }));
} finally {
  await stop(emulator).catch(() => {});
  await browser?.close();
  await vite?.close();
  fs.rmSync(dataRoot, { recursive: true, force: true });
}
