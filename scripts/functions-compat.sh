#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
node_bin=${FIREBASE_FUNCTIONS_NODE_22:-node}

if ! command -v "$node_bin" >/dev/null 2>&1; then
  echo "Set FIREBASE_FUNCTIONS_NODE_22 to a Node 22 executable" >&2
  exit 1
fi

cargo build --release --manifest-path "$repo_dir/Cargo.toml"
(cd "$repo_dir/functions-runtime" && "$node_bin" --test test/config-sdk.test.cjs test/manifest.test.cjs test/protocol.test.cjs)
FIREBASE_FUNCTIONS_NODE_22="$node_bin" "$node_bin" --test "$repo_dir/functions-runtime/test/full-binary.test.cjs"

if command -v npx >/dev/null 2>&1; then
  original_home=$HOME
  official_home=${FIREBASE_FUNCTIONS_OFFICIAL_HOME:-/private/tmp/firebase-functions-official-home-$$}
  emulator_cache=${FIREBASE_EMULATORS_PATH:-$original_home/.cache/firebase/emulators}
  npm_cache=${npm_config_cache:-$original_home/.npm}
  mkdir -p "$official_home"
  (cd "$repo_dir/functions-runtime/fixtures" && HOME="$official_home" FIREBASE_EMULATORS_PATH="$emulator_cache" npm_config_cache="$npm_cache" PATH="$(dirname "$node_bin"):$PATH" npx -y firebase-tools@14.17.0 emulators:exec --project demo-functions --config firebase.official.json --only firestore,functions "$node_bin ../test/official-baseline.cjs")
else
  echo "npx is required for the pinned official Firebase baseline" >&2
  exit 1
fi
