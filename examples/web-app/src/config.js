export function endpoint(value, fallback) {
  const url = new URL(value?.includes('://') ? value : `http://${value || fallback}`);
  if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) || !url.port || url.username || url.password || url.pathname !== '/' || url.search || url.hash) throw new Error('Use a loopback HTTP emulator endpoint with a port.');
  return { host: url.hostname, port: Number(url.port), url: url.origin };
}
export function config(env = {}) {
  const projectId = env.VITE_FIREBASE_projectId || 'demo-rust-emu';
  if (!/^demo-[a-z0-9-]+$/.test(projectId)) throw new Error('Use a demo- project ID.');
  return { projectId, auth: endpoint(env.VITE_FIREBASE_AUTH_EMULATOR_HOST, '127.0.0.1:9099'), firestore: endpoint(env.VITE_FIRESTORE_EMULATOR_HOST, '127.0.0.1:8080'), storage: endpoint(`${env.VITE_FIREBASE_STORAGE_EMULATOR_HOST || '127.0.0.1'}:${env.VITE_FIREBASE_STORAGE_EMULATOR_PORT || '9199'}`) };
}
export function validateCredentials(email, password) {
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new Error('Enter a valid email address.');
  if (password.length < 6) throw new Error('Use at least six password characters.');
}
export function validateId(id) {
  if (!id.trim() || id.includes('/')) throw new Error('Enter a document ID without a slash.');
  return id;
}
export async function bounded(promise, ms = 6000) {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Operation timed out after ${ms} ms`)), ms); })]); }
  finally { clearTimeout(timer); }
}
