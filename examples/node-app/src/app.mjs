import peakfloSchema from 'peakflo-schema';

const { currencyCodeSchema } = peakfloSchema;

// Dependency injection keeps unit checks separate from SDK compatibility checks.
export function createRecordApp({ db, auth, bucket, FieldValue }, collection = 'node-example') {
  const records = db.collection(collection);
  function ref(id) {
    if (typeof id !== 'string' || !id || id.includes('/')) throw new Error('Use a nonempty record ID without /');
    return records.doc(id);
  }
  function record(data) {
    if (!data || typeof data.label !== 'string' || !data.label.trim() || !Number.isFinite(data.rank)) throw new Error('A record needs a label and a finite rank');
    const { error, value: currency } = currencyCodeSchema.required().validate(data.currency);
    if (error) throw new Error(`Invalid Peakflo currency: ${error.message}`);
    return { label: data.label.trim(), rank: data.rank, currency };
  }
  return {
    async create(id, data) { await ref(id).create(record(data)); },
    async read(id) { const snapshot = await ref(id).get(); return snapshot.exists ? snapshot.data() : null; },
    async replace(id, data) { await ref(id).set(record(data)); },
    async merge(id, fields) { await ref(id).set(fields, { merge: true }); },
    async update(id, fields) { await ref(id).update(fields); },
    async transformUpdate(id) {
      await ref(id).update({
        rank: FieldValue.increment(4),
        tags: FieldValue.arrayUnion('updated'),
        obsolete: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
        'nested.left': 9,
      });
    },
    async remove(id) { await ref(id).delete(); },
    async list() { return (await records.limit(50).get()).docs.map(doc => ({ id: doc.id, ...doc.data() })); },
    async search(minRank) { return (await records.where('rank', '>=', minRank).orderBy('rank', 'desc').limit(2).get()).docs.map(doc => ({ id: doc.id, ...doc.data() })); },
    listen(id, next, error) { return ref(id).onSnapshot(snapshot => next(snapshot.exists ? snapshot.data() : null), error); },
    async batchCreate(items) {
      const validated = items.map(({ id, ...data }) => [ref(id), record(data)]);
      const batch = db.batch();
      for (const [target, data] of validated) batch.set(target, data);
      await batch.commit();
    },
    createUser: data => auth.createUser(data),
    getUser: uid => auth.getUser(uid),
    updateUser: (uid, fields) => auth.updateUser(uid, fields),
    listUsers: () => auth.listUsers(100),
    deleteUser: uid => auth.deleteUser(uid),
    async upload(name, contents, resumable = false) { await bucket.file(name).save(Buffer.from(contents), { contentType: 'text/plain', resumable }); },
    async download(name) { const [contents] = await bucket.file(name).download(); return contents.toString(); },
    async listFiles(prefix) { const [files] = await bucket.getFiles({ prefix }); return files.map(file => file.name).sort(); },
    async deleteFile(name) { await bucket.file(name).delete(); }
  };
}
