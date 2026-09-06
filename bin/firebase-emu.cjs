#!/usr/bin/env node
'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const os = require('node:os');
const path = require('node:path');
const {spawn, spawnSync} = require('node:child_process');
const pkg = require('../package.json');

const repository = process.env.FIREBASE_EMU_RELEASE_REPOSITORY || 'dimavedenyapin/firebase-emu';
const version = process.env.FIREBASE_EMU_VERSION || pkg.version;
const targets = {
  'darwin-x64': 'x86_64-apple-darwin',
  'darwin-arm64': 'aarch64-apple-darwin',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'linux-arm64': 'aarch64-unknown-linux-gnu',
  'win32-x64': 'x86_64-pc-windows-msvc',
};
const target = targets[`${process.platform}-${process.arch}`];
if (!target) {
  console.error(`firebase-emu has no release binary for ${process.platform}/${process.arch}`);
  process.exit(1);
}

const extension = process.platform === 'win32' ? 'zip' : 'tar.gz';
const asset = `firebase-emu-v${version}-${target}.${extension}`;
const checksums = `firebase-emu-v${version}-checksums.txt`;
const releaseBase = process.env.FIREBASE_EMU_RELEASE_BASE_URL ||
  `https://github.com/${repository}/releases/download/v${version}`;
const cacheBase = process.env.FIREBASE_EMU_CACHE_DIR || (process.platform === 'win32'
  ? path.join(process.env.LOCALAPPDATA || os.tmpdir(), 'firebase-emu')
  : path.join(process.env.XDG_CACHE_HOME || path.join(os.homedir(), '.cache'), 'firebase-emu'));
const installDir = path.join(cacheBase, version, target);
const executable = path.join(installDir, process.platform === 'win32' ? 'firebase-emu.exe' : 'firebase-emu');

function download(url, destination, redirects = 5) {
  return new Promise((resolve, reject) => {
    const client = new URL(url).protocol === 'http:' ? http : https;
    const request = client.get(url, {headers: {'user-agent': 'firebase-emu-rs'}}, response => {
      if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location && redirects) {
        response.resume();
        return download(new URL(response.headers.location, url).toString(), destination, redirects - 1).then(resolve, reject);
      }
      if (response.statusCode !== 200) {
        response.resume();
        return reject(new Error(`download failed (${response.statusCode}) for ${url}`));
      }
      const output = fs.createWriteStream(destination, {flags: 'wx'});
      response.pipe(output);
      output.on('finish', () => output.close(resolve));
      output.on('error', reject);
    });
    request.on('error', reject);
  });
}

function sha256(file) {
  const hash = crypto.createHash('sha256');
  hash.update(fs.readFileSync(file));
  return hash.digest('hex');
}

async function install() {
  if (fs.existsSync(executable)) return;
  fs.mkdirSync(path.dirname(installDir), {recursive: true});
  const temporary = fs.mkdtempSync(path.join(path.dirname(installDir), '.install-'));
  try {
    const archive = path.join(temporary, asset);
    const manifest = path.join(temporary, checksums);
    await Promise.all([
      download(`${releaseBase}/${asset}`, archive),
      download(`${releaseBase}/${checksums}`, manifest),
    ]);
    const escaped = asset.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const match = fs.readFileSync(manifest, 'utf8').match(new RegExp(`^([a-f0-9]{64})\\s+\\*?${escaped}$`, 'mi'));
    if (!match) throw new Error(`checksum manifest has no entry for ${asset}`);
    const actual = sha256(archive);
    if (actual !== match[1].toLowerCase()) throw new Error(`SHA-256 mismatch for ${asset}`);
    const unpack = process.platform === 'win32'
      ? spawnSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', 'Expand-Archive -LiteralPath $args[0] -DestinationPath $args[1] -Force', archive, temporary], {stdio: 'inherit'})
      : spawnSync('tar', ['-xzf', archive, '-C', temporary], {stdio: 'inherit'});
    if (unpack.status !== 0) throw new Error(`failed to extract ${asset}`);
    fs.rmSync(archive, {force: true});
    fs.rmSync(manifest, {force: true});
    if (process.platform !== 'win32') fs.chmodSync(path.join(temporary, 'firebase-emu'), 0o755);
    try {
      fs.renameSync(temporary, installDir);
    } catch (error) {
      if (!fs.existsSync(executable)) throw error;
    }
  } finally {
    if (fs.existsSync(temporary)) fs.rmSync(temporary, {recursive: true, force: true});
  }
  if (!fs.existsSync(executable)) throw new Error(`release archive did not contain ${path.basename(executable)}`);
}

install().then(() => {
  const child = spawn(executable, process.argv.slice(2), {stdio: 'inherit', env: process.env});
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => child.kill(signal));
  child.on('error', error => { console.error(error.message); process.exitCode = 1; });
  child.on('exit', (code, signal) => {
    if (signal) process.kill(process.pid, signal);
    else process.exitCode = code ?? 1;
  });
}).catch(error => {
  console.error(`firebase-emu install failed: ${error.message}`);
  process.exitCode = 1;
});
