import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright';

const root = resolve(import.meta.dirname, '../../..');
const binary = resolve(root, process.env.FIRERUST_BINARY || 'target/debug/firebase-emu');
async function freePort() {
  const server = createServer();
  await new Promise((yes) => server.listen(0, '127.0.0.1', yes));
  const port = server.address().port;
  await new Promise((yes) => server.close(yes));
  return port;
}
const [firestore, auth, storage, pubsub, ui] = [await freePort(), await freePort(), await freePort(), await freePort(), await freePort()];
const emulator = spawn(binary, ['--in-memory', '--no-functions', '--project', 'demo-fields-ui', '--ui-port', String(ui), '--pubsub-port', String(pubsub)], {
  env: { ...process.env, FIRESTORE_EMU_PORT: String(firestore), FIREBASE_AUTH_EMU_PORT: String(auth), FIREBASE_STORAGE_EMU_PORT: String(storage) },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let log = '';
for (const stream of [emulator.stdout, emulator.stderr]) stream.on('data', (data) => { log += data.toString(); });
let browser;
try {
  const url = `http://127.0.0.1:${ui}`;
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch(`${url}/api/config`)).ok) break; } catch {}
    if (i === 99) throw new Error(`Emulator did not start: ${log}`);
    await delay(100);
  }
  const name = 'projects/demo-fields-ui/databases/(default)/documents/items/one';
  const commit = await fetch(`http://127.0.0.1:${firestore}/v1/projects/demo-fields-ui/databases/(default)/documents:commit`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ writes: [{ update: { name, fields: { keep: { integerValue: '9223372036854775807' } } } }, { update: { name: name.replace('/one', '/empty'), fields: {} } }] }),
  });
  assert.equal(commit.status, 200, await commit.text());
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}) });
  const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
  await page.goto(url);
  await page.getByRole('button', { name: 'items', exact: true }).first().click();
  await page.getByRole('button', { name: 'empty', exact: true }).click();
  assert.equal(await page.getByText('Empty document. Add a field to start.').count(), 1);
  await page.getByRole('button', { name: 'Add field' }).click();
  await page.getByRole('textbox', { name: 'Field name' }).fill('literal.dot`name');
  await page.getByRole('combobox', { name: 'Type' }).selectOption('mapValue');
  await page.getByRole('textbox', { name: 'Value' }).fill('{bad json');
  await page.getByRole('button', { name: 'Add field' }).last().click();
  await page.getByRole('alert').getByText('map value must be valid JSON.').waitFor();
  await page.getByRole('textbox', { name: 'Value' }).fill('{"fields":{"nested":{"arrayValue":{"values":[{"integerValue":"7"}]}}}}');
  await page.getByRole('button', { name: 'Add field' }).last().click();
  await page.getByText('literal.dot`name added.').waitFor();
  await page.getByRole('button', { name: 'Rename literal.dot`name' }).click();
  await page.getByRole('textbox', { name: 'Field name' }).fill('renamed.field');
  await page.getByRole('textbox', { name: 'Field name' }).press('Enter');
  await page.getByText('literal.dot`name renamed to renamed.field.').waitFor();
  const afterRename = await (await fetch(`${url}/api/firestore/documents?project=demo-fields-ui&database=(default)`)).json();
  assert.deepEqual(afterRename.documents.find((doc) => doc.name.endsWith('/empty')).fields, {
    'renamed.field': { mapValue: { fields: { nested: { arrayValue: { values: [{ integerValue: '7' }] } } } } },
  });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.getByRole('button', { name: 'Delete renamed.field' }).count(), 1);
  page.once('dialog', (dialog) => dialog.dismiss());
  await page.getByRole('button', { name: 'Delete renamed.field' }).click();
  assert.equal(await page.getByRole('button', { name: 'Delete renamed.field' }).count(), 1);
  page.once('dialog', (dialog) => dialog.accept());
  await page.getByRole('button', { name: 'Delete renamed.field' }).click();
  await page.getByText('renamed.field deleted.').waitFor();
  await page.getByRole('button', { name: 'one', exact: true }).last().click();
  await page.getByRole('button', { name: 'Add field' }).click();
  await page.getByRole('textbox', { name: 'Field name' }).fill('keep');
  await page.getByRole('button', { name: 'Add field' }).last().click();
  await page.getByRole('alert').getByText('field already exists').waitFor();
  await page.getByRole('button', { name: 'Cancel' }).click();
  const documents = await (await fetch(`${url}/api/firestore/documents?project=demo-fields-ui&database=(default)`)).json();
  assert.deepEqual(documents.documents.find((doc) => doc.name === name).fields, { keep: { integerValue: '9223372036854775807' } });
  assert.deepEqual(documents.documents.find((doc) => doc.name.endsWith('/empty')).fields, {});
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'mobile layout has horizontal overflow');
  console.log('Console browser field add/rename/delete, cancel, duplicate feedback, empty document, mobile: PASS');
} finally {
  await browser?.close();
  if (emulator.exitCode === null) {
    emulator.kill('SIGTERM');
    await new Promise((yes) => emulator.once('exit', yes));
  }
}
