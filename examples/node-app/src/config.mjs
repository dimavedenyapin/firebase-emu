export function readConfig(env = process.env) {
  const projectId = env.GCLOUD_PROJECT;
  if (!/^demo-[a-z0-9-]+$/.test(projectId ?? '')) throw new Error('GCLOUD_PROJECT must be a demo- project ID');
  function host(key, value, url = false) {
    const pattern = url ? /^http:\/\/(127\.0\.0\.1|localhost|\[::1\]):(\d+)$/ : /^(127\.0\.0\.1|localhost|\[::1\]):(\d+)$/;
    const match = pattern.exec(value ?? '');
    if (!match || +match[2] < 1 || +match[2] > 65535) throw new Error(`${key} must use loopback and an explicit valid port`);
    return value;
  }
  const firestoreHost = host('FIRESTORE_EMULATOR_HOST', env.FIRESTORE_EMULATOR_HOST);
  const authHost = host('FIREBASE_AUTH_EMULATOR_HOST', env.FIREBASE_AUTH_EMULATOR_HOST);
  const storageEndpoint = host('STORAGE_EMULATOR_HOST', env.STORAGE_EMULATOR_HOST ?? (env.FIREBASE_STORAGE_EMULATOR_HOST && `http://${env.FIREBASE_STORAGE_EMULATOR_HOST}`), true);
  return { projectId, firestoreHost, authHost, storageEndpoint, bucket: `${projectId}.appspot.com` };
}
