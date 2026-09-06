import { deleteApp, initializeApp }

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, canonicalize(item)])
    );
  }
  return value;
} from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";
import { getStorage } from "firebase-admin/storage";

const requiredHosts = [
  "FIRESTORE_EMULATOR_HOST",
  "FIREBASE_AUTH_EMULATOR_HOST",
  "FIREBASE_STORAGE_EMULATOR_HOST"
];
for (const variable of requiredHosts) {
  const value = process.env[variable];
  if (!value || !/^(127\.0\.0\.1|localhost|\[::1\]):\d+$/.test(value)) {
    throw new Error(`${variable} must be set to an explicit loopback host and port`);
  }
}

const projectId = process.env.GCLOUD_PROJECT;
if (!projectId?.startsWith("demo-")) {
  throw new Error("GCLOUD_PROJECT must be set to a demo- project ID");
}

const bucketName = `${projectId}.appspot.com`;
const app = initializeApp({ projectId, storageBucket: bucketName });
const result = { auth: {}, firestore: {}, storage: {} };
const operationTimeoutMs = Number(process.env.COMPAT_OPERATION_TIMEOUT_MS || 10000);

function withTimeout(operation, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${operationTimeoutMs}ms`)), operationTimeoutMs);
  });
  return Promise.race([operation(), timeout]).finally(() => clearTimeout(timer));
}

async function capture(section, name, operation) {
  try {
    result[section][name] = { ok: true, value: await withTimeout(operation, `${section}.${name}`) };
  } catch (error) {
    result[section][name] = {
      ok: false,
      code: error?.code ?? null,
      message: String(error?.message ?? error).replaceAll(projectId, "<project>")
    };
  }
}

await capture("firestore", "crud", async () => {
  const db = getFirestore(app);
  const ref = db.doc("compat/items-one");
  await ref.set({ rank: 1, label: "first" });
  const created = (await ref.get()).data();
  await ref.update({ rank: 2 });
  const queried = await db.collection("compat").limit(10).get();
  const updated = (await ref.get()).data();
  await ref.delete();
  return { created, updated, queryCount: queried.size, deleted: !(await ref.get()).exists };
});

await capture("auth", "crud", async () => {
  const auth = getAuth(app);
  const created = await auth.createUser({ uid: "compat-user", email: "compat@example.test", displayName: "Before" });
  const fetched = await auth.getUser(created.uid);
  const updated = await auth.updateUser(created.uid, { displayName: "After" });
  const listed = await auth.listUsers(100);
  await auth.deleteUser(created.uid);
  return {
    created: { uid: created.uid, email: created.email },
    fetched: fetched.uid,
    updated: updated.displayName,
    listContainsUser: listed.users.some((user) => user.uid === created.uid)
  };
});

await capture("storage", "roundTrip", async () => {
  const bucket = getStorage(app).bucket();
  const file = bucket.file("compat/hello.txt");
  await file.save(Buffer.from("hello emulator"), { contentType: "text/plain", resumable: false });
  const [contents] = await file.download();
  const [files] = await bucket.getFiles({ prefix: "compat/" });
  await file.delete();
  return { contents: contents.toString(), listContainsObject: files.some((item) => item.name === file.name) };
});

console.log(JSON.stringify(canonicalize(result), null, 2));
await Promise.race([
  deleteApp(app),
  new Promise((resolve) => setTimeout(resolve, operationTimeoutMs))
]);
process.exit(0);

