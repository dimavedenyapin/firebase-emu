#!/usr/bin/env bash
set -uo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results_dir="${SDK_COMPAT_RESULTS_DIR:-$repo_dir/target/sdk-compat}"
project_id="${GCLOUD_PROJECT:-demo-sdk-compat}"
mode="${1:-all}"
mkdir -p "$results_dir"
if [[ -n "${SDK_COMPAT_WEB_APP_DIR:-}" ]]; then
  web_app_dir="$SDK_COMPAT_WEB_APP_DIR"
elif [[ -f "$repo_dir/examples/web-app/package.json" ]]; then
  web_app_dir="$repo_dir/examples/web-app"
else
  web_app_dir="$repo_dir/examples/react-app"
fi

export GCLOUD_PROJECT="$project_id"
export FIREBASE_PROJECT_ID="$project_id"
export FIREBASE_CONFIG="{\"projectId\":\"$project_id\",\"storageBucket\":\"$project_id.appspot.com\"}"
: "${FIRESTORE_EMULATOR_HOST:=127.0.0.1:8080}"
: "${FIREBASE_AUTH_EMULATOR_HOST:=127.0.0.1:9099}"
: "${FIREBASE_STORAGE_EMULATOR_HOST:=127.0.0.1:9199}"
: "${STORAGE_EMULATOR_HOST:=http://$FIREBASE_STORAGE_EMULATOR_HOST}"
export FIRESTORE_EMULATOR_HOST FIREBASE_AUTH_EMULATOR_HOST FIREBASE_STORAGE_EMULATOR_HOST STORAGE_EMULATOR_HOST
export FIRESTORE_EMU_PORT="${FIRESTORE_EMU_PORT:-${FIRESTORE_EMULATOR_HOST##*:}}"
export FIREBASE_AUTH_EMU_PORT="${FIREBASE_AUTH_EMU_PORT:-${FIREBASE_AUTH_EMULATOR_HOST##*:}}"
export FIREBASE_STORAGE_EMU_PORT="${FIREBASE_STORAGE_EMU_PORT:-${FIREBASE_STORAGE_EMULATOR_HOST##*:}}"
export VITE_FIREBASE_projectId="$project_id"
export VITE_FIRESTORE_EMULATOR_HOST="$FIRESTORE_EMULATOR_HOST"
export VITE_FIREBASE_AUTH_EMULATOR_HOST="http://$FIREBASE_AUTH_EMULATOR_HOST"
export VITE_FIREBASE_STORAGE_EMULATOR_HOST="127.0.0.1"
export VITE_FIREBASE_STORAGE_EMULATOR_PORT="$FIREBASE_STORAGE_EMU_PORT"
if [[ -z "${PLAYWRIGHT_CHROMIUM_EXECUTABLE:-}" ]]; then
  for candidate in "$HOME"/Library/Caches/ms-playwright/chromium_headless_shell-*/chrome-mac/headless_shell "$HOME"/Library/Caches/ms-playwright/chromium_headless_shell-*/chrome-headless-shell-mac-arm64/chrome-headless-shell; do
    if [[ -x "$candidate" ]]; then export PLAYWRIGHT_CHROMIUM_EXECUTABLE="$candidate"; fi
  done
fi

record() {
  node "$repo_dir/compat-sdk/record-status.mjs" "$1" "$2" "$3" "$results_dir" "$4"
}

run_case() {
  local id="$1" target="$2" expectation="$3" cwd="$4"
  shift 4
  node "$repo_dir/compat-sdk/run-command.mjs" "$id" "$target" "$expectation" "$results_dir" "$cwd" "$@" || true
}

prepare_app() {
  local id="$1" app_dir="$2"
  if [[ ! -f "$app_dir/package.json" ]]; then
    record "$id.setup" local unavailable "Application package is absent: $app_dir"
    return 1
  fi

  if [[ ! -d "$app_dir/node_modules" ]]; then
    if [[ "${SDK_COMPAT_INSTALL:-0}" == 1 ]]; then
      run_case "$id.install" local pass "$app_dir" npm ci --no-audit --no-fund
    else
      record "$id.setup" local unavailable "Dependencies are absent; rerun with SDK_COMPAT_INSTALL=1"
      return 1
    fi
  fi
  return 0
}

run_target() {
  local target="$1"
  export TARGET_NAME="$target"
  if prepare_app node "$repo_dir/examples/node-app"; then
    run_case "node.$target.integration" "$target" pass "$repo_dir/examples/node-app" npm --silent run test:integration
  fi
  if prepare_app react "$web_app_dir"; then
    run_case "react.$target.integration" "$target" pass "$web_app_dir" npm --silent run test:integration
  fi
}

if [[ "$mode" == --target ]]; then
  target="${2:-}"
  if [[ "$target" != google && "$target" != rust ]]; then
    echo "--target requires google or rust" >&2
    exit 64
  fi
  export TARGET_NAME="$target"
  run_target "$target"
  node "$repo_dir/compat-sdk/summarize.mjs" "$results_dir" --target "$target"
  exit $?
fi

if [[ "$mode" != all ]]; then
  echo "usage: scripts/sdk-compat.sh [all|--target google|--target rust]" >&2
  exit 64
fi

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  echo "Node.js and npm are required" >&2
  exit 2
fi

# Result JSON is owned by this runner. Remove prior case/summary files so a
# previous invocation cannot make an unavailable check look present today.
find "$results_dir" -maxdepth 1 -type f -name '*.json' -delete

if prepare_app node "$repo_dir/examples/node-app"; then
  run_case node.versions local pass "$repo_dir/examples/node-app" node "$repo_dir/compat-sdk/check-versions.mjs" "$repo_dir/examples/node-app" node
  run_case node.unit local pass "$repo_dir/examples/node-app" npm --silent test
fi
if prepare_app react "$web_app_dir"; then
  run_case react.versions local pass "$web_app_dir" node "$repo_dir/compat-sdk/check-versions.mjs" "$web_app_dir" browser
  run_case react.unit local pass "$web_app_dir" npm --silent test
  run_case react.build local pass "$web_app_dir" npm --silent run build
fi

run_case matrix.unit local pass "$repo_dir" node --test compat-sdk/summarize.test.mjs
run_case rust.unit rust pass "$repo_dir" cargo test --all-targets
run_case rust.build rust pass "$repo_dir" cargo build --release

if [[ ! -d "$repo_dir/compat/node_modules" && "${SDK_COMPAT_INSTALL:-0}" == 1 ]]; then
  run_case compat.install local pass "$repo_dir/compat" npm ci --ignore-scripts --no-audit --no-fund
fi
firebase_bin="${FIREBASE_BIN:-$repo_dir/compat/node_modules/.bin/firebase}"
if [[ -n "${SDK_COMPAT_JAVA_HOME:-}" ]]; then
  export JAVA_HOME="$SDK_COMPAT_JAVA_HOME"
  export PATH="$JAVA_HOME/bin:$PATH"
elif command -v brew >/dev/null 2>&1 && [[ -x "$(brew --prefix openjdk@21 2>/dev/null)/bin/java" ]]; then
  export JAVA_HOME="$(brew --prefix openjdk@21)"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if [[ ! -x "$firebase_bin" ]]; then
  record google.harness google unavailable "Firebase CLI is absent (set FIREBASE_BIN or install compat dependencies)"
elif ! command -v java >/dev/null 2>&1; then
  record google.harness google unavailable "Java is required by the Google Firestore emulator"
else
  printf -v nested_command '%q --target google' "$repo_dir/scripts/sdk-compat.sh"
  run_case google.harness google pass "$repo_dir" "$firebase_bin" emulators:exec --project "$project_id" --config compat-sdk/firebase.json --only auth,firestore,storage "$nested_command"
fi

rust_binary="${FIREBASE_EMU_BIN:-$repo_dir/target/release/firebase-emu}"
if [[ ! -x "$rust_binary" ]]; then
  if command -v cargo >/dev/null 2>&1; then
    run_case rust.build rust pass "$repo_dir" cargo build --release
  fi
fi
if [[ ! -x "$rust_binary" ]]; then
  record rust.harness rust unavailable "Rust emulator binary is absent and could not be built (set FIREBASE_EMU_BIN or run cargo build --release)"
else
  "$rust_binary" >"$results_dir/rust-emulator.log" 2>&1 &
  rust_pid=$!
  trap 'kill "$rust_pid" 2>/dev/null || true' EXIT
  ready=0
  for _ in {1..100}; do
    if command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 "$FIRESTORE_EMU_PORT" 2>/dev/null && nc -z 127.0.0.1 "$FIREBASE_AUTH_EMU_PORT" 2>/dev/null && nc -z 127.0.0.1 "$FIREBASE_STORAGE_EMU_PORT" 2>/dev/null; then
      ready=1
      break
    fi
    sleep 0.05
  done
  if [[ "$ready" == 1 ]]; then
    run_target rust
  else
    record rust.harness rust unavailable "Rust emulator did not open ports 8080, 9099, and 9199"
  fi
  kill "$rust_pid" 2>/dev/null || true
  wait "$rust_pid" 2>/dev/null || true
fi

node "$repo_dir/compat-sdk/summarize.mjs" "$results_dir"
