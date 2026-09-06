// Representative emulator load: Firestore CRUD/query/listeners, Auth ops,
// Storage upload/download, callable + Firestore-trigger + pubsub/schedule.
// Uses firebase-admin from compat/node_modules (Admin 13) when available,
// else functions-runtime/node_modules (Admin 9). Loopback + demo- only.
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '../..');
const require = createRequire(import.meta.url);
let admin;
for (const p of [`${repo}/compat/node_modules/firebase-admin`, `${repo}/functions-runtime/node_modules/firebase-admin`]) {
  try { admin = require(p); break; } catch {}
}
if (!admin) throw new Error('firebase-admin not found in compat or functions-runtime node_modules');

const projectId = process.env.BENCH_PROJECT || process.env.GCLOUD_PROJECT || 'demo-bench';
if (!projectId.startsWith('demo-')) throw new Error('demo- project required');
for (const v of ['FIRESTORE_EMULATOR_HOST', 'FIREBASE_AUTH_EMULATOR_HOST', 'FIREBASE_STORAGE_EMULATOR_HOST']) {
  if (!/^(127\.0\.0\.1|localhost|\[::1\]):\d+$/.test(process.env[v] || '')) throw new Error(`${v} must be loopback`);
}
const functionsHost = process.env.FUNCTIONS_EMU_HOST || '127.0.0.1:18401';
const t0 = Date.now();
const counts = { firestoreWrites: 0, firestoreReads: 0, queries: 0, listenerEvents: 0, authOps: 0, storageOps: 0, callables: 0, triggers: 0, failures: 0 };
const lat = [];
const timed = async (fn) => { const s = Date.now(); try { return await fn(); } finally { lat.push(Date.now() - s); } };
const fail = (e, label) => { counts.failures++; console.error(`FAIL ${label}: ${e?.message || e}`); };

const app = admin.initializeApp({ projectId, storageBucket: `${projectId}.appspot.com` }, `bench-${Date.now()}`);
const db = app.firestore();
const auth = app.auth();
const bucket = app.storage().bucket();

// --- Firestore: 200 docs CRUD + queries + listeners ---
const N = Number(process.env.BENCH_DOCS || 200);
await timed(async () => {
  const batch = db.batch();
  for (let i = 0; i < N; i++) batch.set(db.doc(`bench/items-${i}`), { rank: i, label: `item-${i}`, even: i % 2 === 0 });
  await batch.commit();
  counts.firestoreWrites += N;
});
for (let i = 0; i < 50; i++) {
  try {
    await timed(async () => {
      const snap = await db.collection('bench').where('even', '==', true).orderBy('rank').limit(10).get();
      counts.queries++; counts.firestoreReads += snap.size;
    });
  } catch (e) { fail(e, 'query'); }
}
// Live listeners on one doc + one query; drive 10 updates through them.
let docEvents = 0, queryEvents = 0;
const docDone = new Promise((resolve) => {
  const unsub = db.doc('bench/live-doc').onSnapshot((s) => { docEvents++; counts.listenerEvents++; if (docEvents >= 4) { unsub(); resolve(); } }, (e) => { fail(e, 'docListener'); resolve(); });
  setTimeout(resolve, 15000);
});
const qDone = new Promise((resolve) => {
  const unsub = db.collection('bench').where('even', '==', true).limit(5).onSnapshot((s) => { queryEvents++; counts.listenerEvents += s.size; if (queryEvents >= 3) { unsub(); resolve(); } }, (e) => { fail(e, 'queryListener'); resolve(); });
  setTimeout(resolve, 15000);
});
await db.doc('bench/live-doc').set({ n: 0 });
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
await pause(300);
for (let i = 1; i <= 3; i++) { await timed(() => db.doc('bench/live-doc').update({ n: i })); counts.firestoreWrites++; await pause(300); }
for (let i = 0; i < 10; i++) { await timed(() => db.doc(`bench/live-${i}`).set({ even: i % 2 === 0, rank: 1000 + i })); counts.firestoreWrites++; await pause(150); }
await Promise.all([docDone, qDone]);
for (let i = 0; i < 20; i++) { try { await timed(() => db.doc(`bench/items-${i}`).get()); counts.firestoreReads++; } catch (e) { fail(e, 'read'); } }
await timed(async () => {
  const b = db.batch();
  for (let i = 0; i < 20; i++) b.delete(db.doc(`bench/items-${i}`));
  await b.commit();
  counts.firestoreWrites += 20;
});

// --- Auth: 10 users full cycle + list ---
for (let i = 0; i < 10; i++) {
  const uid = `bench-user-${Date.now()}-${i}`;
  try {
    await timed(() => auth.createUser({ uid, email: `${uid}@example.test`, displayName: 'Before' })); counts.authOps++;
    await timed(() => auth.getUser(uid)); counts.authOps++;
    await timed(() => auth.updateUser(uid, { displayName: 'After' })); counts.authOps++;
    await timed(() => auth.deleteUser(uid)); counts.authOps++;
  } catch (e) { fail(e, 'auth'); }
}
try { await timed(() => auth.listUsers(100)); counts.authOps++; } catch (e) { fail(e, 'authList'); }

// --- Storage: 5 objects (4KB/64KB/256KB), download, list, delete ---
const sizes = [4 * 1024, 4 * 1024, 64 * 1024, 64 * 1024, 256 * 1024];
let n = 0;
for (const size of sizes) {
  const name = `bench/blob-${Date.now()}-${n++}-${size}.bin`;
  const buf = Buffer.alloc(size, 'x');
  try {
    await timed(() => bucket.file(name).save(buf, { contentType: 'application/octet-stream', resumable: false })); counts.storageOps++;
    const [dl] = await timed(() => bucket.file(name).download()); counts.storageOps++;
    if (dl.length !== size) { counts.failures++; console.error(`FAIL storage size ${name}: ${dl.length} != ${size}`); }
    await timed(() => bucket.file(name).delete()); counts.storageOps++;
  } catch (e) { fail(e, 'storage'); }
}
try { await timed(() => bucket.getFiles({ prefix: 'bench/' })); counts.storageOps++; } catch (e) { fail(e, 'storageList'); }

// --- Functions: callable echo x20 + HTTP x10 (basic fixture, first codebase) ---
const base = `http://${functionsHost}`;
const post = (url, body) => fetch(base + url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
for (let i = 0; i < 20; i++) {
  try { const r = await timed(() => post(`/${projectId}/us-central1/echo`, { data: { i } })); counts.callables++; if (r.status !== 200) { counts.failures++; console.error(`FAIL callable status ${r.status}`); } await r.text(); }
  catch (e) { fail(e, 'callable'); }
}
for (let i = 0; i < 10; i++) {
  try { const r = await timed(() => post(`/${projectId}/us-central1/http`, { safe: true, i })); if (r.status !== 201) { counts.failures++; console.error(`FAIL http status ${r.status}`); } await r.text(); counts.triggers++; }
  catch (e) { fail(e, 'http'); }
}
// Firestore-trigger writes x30 through the emulator, then drain + pubsub/schedule.
for (let i = 0; i < 30; i++) {
  try { await timed(() => db.doc(`items/trig-${Date.now()}-${i}`).set({ n: i })); counts.firestoreWrites++; }
  catch (e) { fail(e, 'triggerWrite'); }
}
try {
  const d = await timed(() => fetch(`${base}/__/functions/drain`));
  if (d.status !== 200) { counts.failures++; console.error(`FAIL drain ${d.status}: ${await d.text()}`); }
  else counts.triggers++;
} catch (e) { fail(e, 'drain'); }
try {
  const p = await timed(() => post('/__/functions/pubsub/fixture-topic', { data: { bench: 1 }, attributes: {} }));
  if (p.status === 202) counts.triggers++;
  const s = await timed(() => post('/__/functions/schedule/schedule', {}));
  if (s.status === 202) counts.triggers++;
  const d2 = await fetch(`${base}/__/functions/drain`);
  if (d2.status === 200) counts.triggers++;
} catch (e) { fail(e, 'pubsub'); }

lat.sort((a, b) => a - b);
const pct = (p) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(lat.length * p))] : 0;
console.log(JSON.stringify({
  projectId, elapsedMs: Date.now() - t0, counts,
  payload: { firestoreDocs: N, queries: 50, listenerDocEvents: docEvents, listenerQuerySnapshots: queryEvents, authUsers: 10, storageObjects: sizes.length, storageBytes: sizes.reduce((a, b) => a + b, 0), callables: 20, httpCalls: 10, triggerWrites: 30 },
  latencyMs: { samples: lat.length, p50: pct(0.5), p95: pct(0.95), max: lat[lat.length - 1] || 0 },
  failures: counts.failures,
}));
await app.delete();
process.exit(counts.failures ? 1 : 0);
