'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const {spawn, spawnSync} = require('node:child_process');
const {test} = require('node:test');
const pkg = require('../package.json');

const targets = {
  'darwin-x64': 'x86_64-apple-darwin',
  'darwin-arm64': 'aarch64-apple-darwin',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'linux-arm64': 'aarch64-unknown-linux-gnu',
};

test('launcher downloads, verifies, caches, and forwards spaced arguments', {
  skip: process.platform === 'win32' ? 'POSIX fixture executable' : false,
}, async t => {
  const target = targets[`${process.platform}-${process.arch}`];
  if (!target) return t.skip('No release target for this test host');
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'firebase launcher test '));
  const contents = path.join(root, 'contents');
  const served = path.join(root, 'served');
  const cache = path.join(root, 'cache with spaces');
  fs.mkdirSync(path.join(contents, 'functions-runtime'), {recursive: true});
  fs.mkdirSync(served);
  fs.writeFileSync(path.join(contents, 'firebase-emu'), '#!/bin/sh\nprintf "%s\\n" "$@"\n', {mode: 0o755});
  fs.writeFileSync(path.join(contents, 'functions-runtime', 'adapter.cjs'), "'use strict';\n");
  const asset = `firebase-emu-v${pkg.version}-${target}.tar.gz`;
  const archive = path.join(served, asset);
  assert.equal(spawnSync('tar', ['-czf', archive, '-C', contents, '.']).status, 0);
  const digest = crypto.createHash('sha256').update(fs.readFileSync(archive)).digest('hex');
  const manifest = `firebase-emu-v${pkg.version}-checksums.txt`;
  fs.writeFileSync(path.join(served, manifest), `${digest}  ${asset}\n`);
  const server = http.createServer((request, response) => {
    const file = path.join(served, path.basename(request.url));
    if (!fs.existsSync(file)) { response.statusCode = 404; return response.end(); }
    fs.createReadStream(file).pipe(response);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    await new Promise(resolve => server.close(resolve));
    fs.rmSync(root, {recursive: true, force: true});
  });
  const output = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.resolve(__dirname, '../bin/firebase-emu.cjs'), '--config', 'a path with spaces'], {
      env: {...process.env, FIREBASE_EMU_CACHE_DIR: cache, FIREBASE_EMU_RELEASE_BASE_URL: `http://127.0.0.1:${server.address().port}`},
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = ''; let stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('exit', code => code === 0 ? resolve(stdout) : reject(new Error(stderr)));
  });
  assert.equal(output, '--config\na path with spaces\n');
  assert.ok(fs.existsSync(path.join(cache, pkg.version, target, 'firebase-emu')));
  assert.ok(fs.existsSync(path.join(cache, pkg.version, target, 'functions-runtime', 'adapter.cjs')));
});
