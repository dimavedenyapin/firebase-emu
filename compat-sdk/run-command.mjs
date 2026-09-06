import { spawn } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const [id, target, expected, resultsDir, cwd, ...command] = process.argv.slice(2);
if (!id || !target || !expected || !resultsDir || !cwd || command.length === 0) {
  console.error('usage: run-command.mjs <id> <target> <pass|unsupported|unavailable> <results-dir> <cwd> <command...>');
  process.exit(64);
}

const startedAt = new Date();
const started = Date.now();
let stdout = '';
let stderr = '';
let exitCode = null;
let signal = null;
let spawnError = null;

try {
  const child = spawn(command[0], command.slice(1), {
    cwd,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe']
  });
  child.stdout.on('data', chunk => { stdout += chunk; process.stdout.write(chunk); });
  child.stderr.on('data', chunk => { stderr += chunk; process.stderr.write(chunk); });
  ({ code: exitCode, signal } = await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, closeSignal) => resolve({ code, signal: closeSignal }));
  }));
} catch (error) {
  spawnError = String(error?.message ?? error);
}

function parsedOutput(text) {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try { return JSON.parse(trimmed); } catch {}
  const lines = trimmed.split(/\r?\n/);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    try { return JSON.parse(lines.slice(index).join('\n')); } catch {}
  }
  return null;
}

let status;
if (expected === 'unavailable') status = 'unavailable';
else if (expected === 'unsupported') status = 'unsupported';
else status = exitCode === 0 && !spawnError ? 'passed' : 'failed';

const report = {
  id,
  target,
  expected,
  status,
  startedAt: startedAt.toISOString(),
  durationMs: Date.now() - started,
  command,
  cwd,
  exitCode,
  signal,
  spawnError,
  output: parsedOutput(stdout),
  stdout,
  stderr
};
await mkdir(resultsDir, { recursive: true });
const filename = `${id.replace(/[^a-zA-Z0-9_.-]/g, '_')}.json`;
await writeFile(path.join(resultsDir, filename), `${JSON.stringify(report, null, 2)}\n`);
console.error(`[sdk-compat] ${id}: ${status}${exitCode === null ? '' : ` (exit ${exitCode})`}`);
process.exit(status === 'failed' ? 1 : 0);
