import { bounded } from './config';
import { ActionType, validateActionType } from './schema';
export async function runProbe(client) {
  const results = []; const id = `probe-${Date.now()}`; const email = `${id}@example.test`; const name = `${id}.txt`;
  async function check(name, action, dependency) {
    if (dependency && !results.find(item => item.name === dependency && item.status === 'passed')) { results.push({ name, status: 'blocked', error: `Requires ${dependency}` }); return; }
    const start = Date.now();
    try { await bounded(Promise.resolve().then(action), 7000); results.push({ name, status: 'passed', durationMs: Date.now() - start }); }
    catch (error) { results.push({ name, status: 'failed', error: error.message, code: error.code || null, durationMs: Date.now() - start }); }
  }
  function assert(value, message) { if (!value) throw new Error(message); }
  await check('fixture.actionType.valid', () => assert(validateActionType(ActionType.UPDATE) === 'UPDATE', 'Synthetic action type was rejected'));
  await check('fixture.actionType.invalid', () => {
    try { validateActionType('UNKNOWN'); } catch { return; }
    throw new Error('Unknown synthetic action type was accepted');
  });
  await check('auth.signUp', () => client.signUp(email, 'test-password'));
  await check('auth.signOut', () => client.signOut());
  await check('auth.signIn', () => client.signIn(email, 'test-password'), 'auth.signUp');
  await check('firestore.create', () => client.create(id));
  await check('firestore.read', async () => assert((await client.read(id))?.title === 'Browser note', 'Document value did not match'), 'firestore.create');
  await check('firestore.delete', async () => { await client.remove(id); assert(await client.read(id) === null, 'Deleted document still exists'); }, 'firestore.create');
  await check('firestore.partial-merge', async () => {
    const partialId = `${id}-partial`;
    try {
      await client.write(partialId, { title: 'Preserved', active: true, count: 1 });
      await client.update(partialId);
      const value = await client.read(partialId);
      assert(value?.count === 2 && value?.title === 'Preserved' && value?.active === true, 'Update did not preserve omitted fields');
    } finally { await bounded(client.remove(partialId), 2000).catch(() => {}); }
  });
  await check('firestore.where-order-limit', async () => {
    const queryIds = [`${id}-low`, `${id}-mid`, `${id}-high`];
    try {
      await Promise.all([
        client.write(queryIds[0], { title: 'Low', active: true, count: 31 }),
        client.write(queryIds[1], { title: 'Mid', active: true, count: 33 }),
        client.write(queryIds[2], { title: 'High', active: true, count: 35 })
      ]);
      assert(JSON.stringify((await client.query(30)).map(item => item.id)) === JSON.stringify([queryIds[2], queryIds[1]]), 'where/order/limit result did not match');
    } finally { await Promise.allSettled(queryIds.map(queryId => bounded(client.remove(queryId), 2000))); }
  });
  await check('firestore.listener', async () => {
    const listenerId = `${id}-listener`; let stop;
    try {
      await client.write(listenerId, { title: 'Listener', active: true, count: 7 });
      let nextValue;
      const initial = new Promise((resolve, reject) => {
        nextValue = resolve;
        stop = client.listen(listenerId, value => nextValue(value), reject);
      });
      assert((await bounded(initial, 2000))?.count === 7, 'Initial listener value did not match');
      const changed = new Promise(resolve => { nextValue = resolve; });
      await client.updateFromOtherClient(listenerId);
      assert((await bounded(changed, 2000))?.count === 2, 'Live listener update did not match');
    } finally { stop?.(); await bounded(client.remove(listenerId), 2000).catch(() => {}); }
  });
  await check('storage.upload', () => client.upload(name, new TextEncoder().encode('browser storage test')));
  await check('storage.download', async () => assert(await client.download(name) === 'browser storage test', 'File bytes did not match'), 'storage.upload');
  await check('storage.list', async () => assert((await client.listFiles()).includes(name), 'Uploaded file is absent from list'), 'storage.upload');
  await check('storage.delete', () => client.deleteFile(name), 'storage.upload');
  await client.signOut();
  return { sdk: 'firebase/browser', version: '12.12.1', projectId: client.settings.projectId, results };
}
