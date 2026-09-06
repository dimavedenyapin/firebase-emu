import { FieldValue, Firestore } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';
import { initializeApp, deleteApp } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';
import { readConfig } from './config.mjs';
import { createRecordApp } from './app.mjs';

export function connect(collection) {
  const config = readConfig();
  process.env.STORAGE_EMULATOR_HOST = config.storageEndpoint;
  const db = new Firestore({ projectId: config.projectId, host: config.firestoreHost, ssl: false, ignoreUndefinedProperties: true });
  const storage = new Storage({ projectId: config.projectId, apiEndpoint: config.storageEndpoint, useAuthWithCustomEndpoint: false, retryOptions: { autoRetry: false } });
  const admin = initializeApp({ projectId: config.projectId }, `node-example-${Date.now()}`);
  return {
    config,
    app: createRecordApp({ db, auth: getAuth(admin), bucket: storage.bucket(config.bucket), FieldValue }, collection),
    async close() { await Promise.all([db.terminate(), deleteApp(admin)]); }
  };
}
