import { initializeApp, getApps, getApp, deleteApp } from 'firebase/app';
import { getAuth, connectAuthEmulator, createUserWithEmailAndPassword, signInWithEmailAndPassword, signOut } from 'firebase/auth';
import { getFirestore, connectFirestoreEmulator, doc, setDoc, updateDoc, getDocFromServer, deleteDoc, collection, query, where, orderBy, limit, getDocsFromServer, onSnapshot, terminate } from 'firebase/firestore';
import { getStorage, connectStorageEmulator, ref, uploadBytes, getDownloadURL, deleteObject, listAll } from 'firebase/storage';
import { config, validateCredentials, validateId, bounded } from './config';
export function createClient(env = import.meta.env) {
  const settings = config(env);
  const app = getApps().length ? getApp() : initializeApp({ projectId: settings.projectId, apiKey: 'demo-api-key', appId: 'demo-web-app', storageBucket: `${settings.projectId}.appspot.com` });
  const auth = getAuth(app); const db = getFirestore(app); const storage = getStorage(app);
  connectAuthEmulator(auth, settings.auth.url, { disableWarnings: true });
  connectFirestoreEmulator(db, settings.firestore.host, settings.firestore.port);
  connectStorageEmulator(storage, settings.storage.host, settings.storage.port);
  storage.maxUploadRetryTime = 4000; storage.maxOperationRetryTime = 4000;
  const document = id => doc(db, 'browser-notes', validateId(id));
  return {
    settings,
    signUp(email, password) { validateCredentials(email, password); return bounded(createUserWithEmailAndPassword(auth, email, password)); },
    signIn(email, password) { validateCredentials(email, password); return bounded(signInWithEmailAndPassword(auth, email, password)); },
    signOut: () => bounded(signOut(auth)),
    create: id => bounded(setDoc(document(id), { title: 'Browser note', active: true, count: 1 })),
    write: (id, data) => bounded(setDoc(document(id), data)),
    read: async id => { const result = await bounded(getDocFromServer(document(id))); return result.exists() ? result.data() : null; },
    updateFromOtherClient: async id => {
      const writerApp = initializeApp(app.options, `writer-${Date.now()}`);
      const writer = getFirestore(writerApp);
      connectFirestoreEmulator(writer, settings.firestore.host, settings.firestore.port);
      try { await bounded(updateDoc(doc(writer, 'browser-notes', validateId(id)), { count: 2 })); }
      finally { await terminate(writer); await deleteApp(writerApp); }
    },
    update: id => bounded(updateDoc(document(id), { count: 2 })),
    remove: id => bounded(deleteDoc(document(id))),
    query: async minCount => {
      const target = query(collection(db, 'browser-notes'), where('active', '==', true), where('count', '>=', minCount), orderBy('count', 'desc'), limit(2));
      const result = await bounded(getDocsFromServer(target));
      return result.docs.map(item => ({ id: item.id, ...item.data() }));
    },
    listen(id, next, error) { return onSnapshot(document(id), { includeMetadataChanges: true }, snap => { if (!snap.metadata.fromCache) next(snap.exists() ? snap.data() : null); }, error); },
    upload: (name, bytes) => bounded(uploadBytes(ref(storage, name), bytes, { contentType: 'text/plain' })),
    download: async name => { const url = await bounded(getDownloadURL(ref(storage, name))); const response = await bounded(fetch(url, { signal: AbortSignal.timeout(6000) })); if (!response.ok) throw new Error(`Download HTTP ${response.status}`); return response.text(); },
    downloadBytes: async name => { const url = await bounded(getDownloadURL(ref(storage, name))); const response = await bounded(fetch(url, { signal: AbortSignal.timeout(6000) })); if (!response.ok) throw new Error(`Download HTTP ${response.status}`); return new Uint8Array(await response.arrayBuffer()); },
    listFiles: async () => (await bounded(listAll(ref(storage)))).items.map(item => item.name),
    deleteFile: name => bounded(deleteObject(ref(storage, name)))
  };
}
