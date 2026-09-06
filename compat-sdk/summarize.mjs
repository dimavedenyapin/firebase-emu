import { readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const [resultsDir, mode, targetOnly] = process.argv.slice(2);
const required = JSON.parse(await readFile(new URL('./required-checks.json', import.meta.url), 'utf8'));
const targets = mode === '--target' ? [targetOnly] : ['google', 'rust'];
const files = (await readdir(resultsDir)).filter(name => name.endsWith('.json') && name !== 'summary.json').sort();
const cases = [];
for (const file of files) {
  try {
    const value = JSON.parse(await readFile(path.join(resultsDir, file), 'utf8'));
    if (value?.id && value?.status) cases.push(value);
  } catch (error) {
    cases.push({ id: file, target: 'harness', status: 'failed', reason: `Invalid result JSON: ${error.message}` });
  }
}
for (const target of targets) {
  for (const sdk of ['node', 'react']) {
    const id = `${sdk}.${target}.integration`;
    const item = cases.find(item => item.id === id);
    const names = item?.output?.results?.map(result => result.name);
    if (!names || JSON.stringify([...names].sort()) !== JSON.stringify([...required[sdk]].sort())) {
      cases.push({ id: `${id}.missing-results`, target, status: 'failed' });
    }
  }
}
const counts = Object.fromEntries(['passed', 'failed', 'unsupported', 'unavailable', 'skipped'].map(status => [status, cases.filter(item => item.status === status).length]));
const capabilities = cases
  .filter(item => item.id.endsWith('.integration') && Array.isArray(item.output?.results))
  .flatMap(item => item.output.results.map(result => ({ caseId: item.id, target: item.target, ...result })));
const capabilityCounts = Object.fromEntries(
  ['passed', 'failed', 'blocked'].map(status => [status, capabilities.filter(item => item.status === status).length])
);
const matrixCounts = Object.fromEntries(
  [...new Set(capabilities.map(item => `${item.target}.${item.caseId.startsWith('react.') ? 'browser' : 'node'}`))].map(key => {
    const [target, sdk] = key.split('.');
    const selected = capabilities.filter(item => item.target === target && item.caseId.startsWith(`${sdk === 'browser' ? 'react' : 'node'}.`));
    return [key, Object.fromEntries(['passed', 'failed', 'blocked'].map(status => [status, selected.filter(item => item.status === status).length]))];
  })
);
const summary = {
  generatedAt: new Date().toISOString(),
  counts,
  capabilityCounts,
  matrixCounts,
  capabilities,
  cases
};
await writeFile(path.join(resultsDir, 'summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ resultsDir, counts }, null, 2));
if (cases.some(item => item.status !== 'passed') || capabilities.some(item => item.status !== 'passed')) process.exitCode = 1;
