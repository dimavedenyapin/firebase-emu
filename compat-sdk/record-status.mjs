import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const [id, target, status, resultsDir, reason] = process.argv.slice(2);
if (!id || !target || !status || !resultsDir || !reason) process.exit(64);
await mkdir(resultsDir, { recursive: true });
const report = {
  id,
  target,
  expected: status,
  status,
  executed: false,
  reason
};
await writeFile(path.join(resultsDir, `${id.replace(/[^a-zA-Z0-9_.-]/g, '_')}.json`), `${JSON.stringify(report, null, 2)}\n`);
console.error(`[sdk-compat] ${id}: ${status} (${reason})`);
