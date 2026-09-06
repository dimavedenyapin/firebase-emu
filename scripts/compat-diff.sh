#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results_dir="${COMPAT_RESULTS_DIR:-$repo_dir/target/compat}"
project_id="${GCLOUD_PROJECT:-demo-compat}"
node_bin="${NODE_BIN:-$(command -v node)}"
mkdir -p "$results_dir"

export GCLOUD_PROJECT="$project_id"
export FIREBASE_CONFIG="{\"projectId\":\"$project_id\",\"storageBucket\":\"$project_id.appspot.com\"}"
export FIRESTORE_EMULATOR_HOST="127.0.0.1:8080"
export FIREBASE_AUTH_EMULATOR_HOST="127.0.0.1:9099"
export FIREBASE_STORAGE_EMULATOR_HOST="127.0.0.1:9199"

cd "$repo_dir"
if [[ -n "${COMPAT_JAVA_HOME:-}" ]]; then
  export JAVA_HOME="$COMPAT_JAVA_HOME"
  export PATH="$JAVA_HOME/bin:$PATH"
elif command -v brew >/dev/null 2>&1 && [[ -x "$(brew --prefix openjdk@21 2>/dev/null)/bin/java" ]]; then
  export JAVA_HOME="$(brew --prefix openjdk@21)"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if [[ ! -d compat/node_modules/firebase-admin || ! -x compat/node_modules/.bin/firebase ]]; then
  npm install --prefix compat --ignore-scripts --no-audit --no-fund
fi
firebase_bin="${FIREBASE_BIN:-$repo_dir/compat/node_modules/.bin/firebase}"

"$firebase_bin" emulators:exec \
  --project "$project_id" \
  --config compat/firebase.json \
  --only auth,firestore,storage \
  "'$node_bin' compat/admin-smoke.mjs > '$results_dir/real.json'"

"$repo_dir/target/release/firebase-emu" >"$results_dir/rust.log" 2>&1 &
rust_pid=$!
trap 'kill "$rust_pid" 2>/dev/null || true' EXIT

for port in 8080 9099 9199; do
  for _ in {1..100}; do
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then break; fi
    sleep 0.05
  done
  nc -z 127.0.0.1 "$port"
done

"$node_bin" compat/admin-smoke.mjs > "$results_dir/rust.json"
diff -u "$results_dir/real.json" "$results_dir/rust.json" | tee "$results_dir/diff.txt" || true

echo "Compatibility outputs: $results_dir"

