import { fileURLToPath } from 'node:url';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
const required = JSON.parse(await readFile(new URL('./required-checks.json', import.meta.url), 'utf8'));
test('matrix fails for absent, blocked, or duplicate checks', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'sdk-summary-'));
  const run = () => spawnSync(process.execPath, [fileURLToPath(new URL('./summarize.mjs', import.meta.url)), dir], { encoding: 'utf8' }).status;
  try {
    assert.equal(run(), 1);
    for (const target of ['google', 'rust']) for (const sdk of ['node', 'react']) {
      const id = `${sdk}.${target}.integration`;
      await writeFile(path.join(dir, `${id}.json`), JSON.stringify({ id, target, status: 'passed', output: { results: required[sdk].map(name => ({ name, status: 'passed' })) } }));
    }
    assert.equal(run(), 0);
    const file = path.join(dir, 'react.rust.integration.json');
    const value = JSON.parse(await readFile(file, 'utf8'));
    value.output.results[0].status = 'blocked';
    await writeFile(file, JSON.stringify(value));
    assert.equal(run(), 1);
    value.output.results[0].status = 'passed';
    value.output.results.push(value.output.results[0]);
    await writeFile(file, JSON.stringify(value));
    assert.equal(run(), 1);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
