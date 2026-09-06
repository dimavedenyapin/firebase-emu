import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const [appDir, sdk] = process.argv.slice(2);
const expected = sdk === 'node'
  ? { 'firebase-admin': '11.11.1', '@google-cloud/firestore': '7.11.6', '@google-cloud/storage': '7.7.0', 'peakflo-schema': '5.11.62' }
  : { firebase: '12.12.1', react: '16.14.0', 'react-dom': '16.14.0', '@peakflo/peakflo-schema': '4.11.58' };
const lock = JSON.parse(await readFile(`${appDir}/package-lock.json`, 'utf8'));
for (const [name, version] of Object.entries(expected)) {
  const installed = JSON.parse(await readFile(`${appDir}/node_modules/${name}/package.json`, 'utf8'));
  assert.equal(installed.version, version, `${name}: installed version`);
  assert.equal(lock.packages[`node_modules/${name}`].version, version, `${name}: locked version`);
}
if (sdk === 'node') assert.match(lock.packages['node_modules/peakflo-schema'].resolved, /03a381c70f841b731d95adf01fb63b3c3b13cd24$/);
console.log(JSON.stringify({ sdk, versions: expected }));
