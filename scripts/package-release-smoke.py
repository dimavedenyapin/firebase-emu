#!/usr/bin/env python3
"""Run native smoke tests against an extracted release archive."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_tcp(port: int, process: subprocess.Popen[bytes], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"firebase-emu exited {process.returncode}")
        with socket.socket() as client:
            client.settimeout(0.2)
            if client.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for 127.0.0.1:{port}")


def http_status(url: str, data: bytes | None = None) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def http_request(
    url: str,
    method: str,
    data: bytes | None = None,
    content_type: str = "application/json",
) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        method=method,
        data=data,
        headers={"content-type": content_type},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def wait_http(
    url: str,
    process: subprocess.Popen[bytes],
    expected_status: int,
    data: bytes | None = None,
    timeout: float = 30,
) -> bytes:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"firebase-emu exited {process.returncode}")
        try:
            status, body = http_status(url, data)
            if status == expected_status:
                return body
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for HTTP {expected_status} from {url}: {last_error}")


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGINT)
        process.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        process.kill()
        process.wait(timeout=10)


def extract(archive: Path, destination: Path) -> None:
    if archive.name.endswith(".tar.gz"):
        with tarfile.open(archive, "r:gz") as source:
            source.extractall(destination, filter="data")
    else:
        with zipfile.ZipFile(archive) as source:
            source.extractall(destination)


def run_binary(binary: Path, arguments: list[str], environment: dict[str, str]) -> subprocess.Popen[bytes]:
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen(
        [str(binary), *arguments],
        cwd=binary.parent,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=flags,
    )


def smoke(args: argparse.Namespace) -> None:
    archive = args.archive.resolve()
    with tempfile.TemporaryDirectory(prefix="firebase-emu-native-smoke-") as temporary:
        root = Path(temporary)
        extract(archive, root)
        binary = root / ("firebase-emu.exe" if os.name == "nt" else "firebase-emu")
        if os.name != "nt":
            if not os.access(binary, os.X_OK):
                raise RuntimeError("extracted firebase-emu is not executable")
        help_result = subprocess.run([str(binary), "--help"], capture_output=True, text=True, timeout=15)
        if help_result.returncode or "firebase-emu" not in help_result.stdout:
            raise RuntimeError(f"--help failed: {help_result.stdout}\n{help_result.stderr}")

        environment = os.environ.copy()
        environment.pop("FIREBASE_FUNCTIONS_ADAPTER", None)
        firestore, auth, storage, pubsub = (free_port() for _ in range(4))
        environment.update({
            "FIRESTORE_EMU_PORT": str(firestore),
            "FIREBASE_AUTH_EMU_PORT": str(auth),
            "FIREBASE_STORAGE_EMU_PORT": str(storage),
            "PUBSUB_EMULATOR_PORT": str(pubsub),
        })
        process = run_binary(binary, ["--no-functions"], environment)
        try:
            for port in (firestore, auth, storage, pubsub):
                wait_tcp(port, process)
            status, _ = http_status(f"http://127.0.0.1:{auth}/")
            if status == 0:
                raise RuntimeError("Auth HTTP listener returned no response")
        finally:
            stop(process)

        data_dir = root / "persistent data with spaces"
        persistence_args = ["--data-dir", str(data_dir), "--no-functions"]
        process = run_binary(binary, persistence_args, environment)
        try:
            for port in (firestore, auth, storage, pubsub):
                wait_tcp(port, process)
            duplicate = run_binary(binary, persistence_args, environment)
            try:
                duplicate.wait(timeout=10)
                duplicate_error = b"" if duplicate.stderr is None else duplicate.stderr.read(16384)
                if duplicate.returncode == 0 or b"already owned" not in duplicate_error:
                    raise RuntimeError(
                        f"duplicate data-directory owner was not rejected: {duplicate.returncode} {duplicate_error!r}"
                    )
            finally:
                stop(duplicate)

            firestore_document = "projects/demo-release/databases/(default)/documents/native/restart"
            commit = json.dumps({"writes": [{"update": {
                "name": firestore_document,
                "fields": {"value": {"integerValue": "9223372036854775807"}},
            }}]}).encode()
            status, body = http_request(
                f"http://127.0.0.1:{firestore}/v1/projects/demo-release/databases/(default)/documents:commit",
                "POST",
                commit,
            )
            if status != 200:
                raise RuntimeError(f"persistent Firestore seed failed: {status} {body!r}")
            status, body = http_request(
                f"http://127.0.0.1:{auth}/identitytoolkit.googleapis.com/v1/projects/demo-release/accounts",
                "POST",
                json.dumps({"localId": "native-user", "email": "native@example.test"}).encode(),
            )
            if status != 200:
                raise RuntimeError(f"persistent Auth seed failed: {status} {body!r}")
            storage_bytes = b"native\x00restart\xff"
            status, body = http_request(
                f"http://127.0.0.1:{storage}/demo-release.appspot.com/folder%2Fobject.bin",
                "PUT",
                storage_bytes,
                "application/octet-stream",
            )
            if status != 200:
                raise RuntimeError(f"persistent Storage seed failed: {status} {body!r}")
        finally:
            stop(process)

        process = run_binary(binary, persistence_args, environment)
        try:
            for port in (firestore, auth, storage, pubsub):
                wait_tcp(port, process)
            status, body = http_request(
                f"http://127.0.0.1:{firestore}/v1/projects/demo-release/databases/(default)/documents:batchGet",
                "POST",
                json.dumps({"documents": [firestore_document]}).encode(),
            )
            if status != 200 or json.loads(body)[0].get("found", {}).get("fields", {}).get("value", {}).get("integerValue") != "9223372036854775807":
                raise RuntimeError(f"persistent Firestore restart failed: {status} {body!r}")
            status, body = http_request(
                f"http://127.0.0.1:{auth}/identitytoolkit.googleapis.com/v1/projects/demo-release/accounts:lookup",
                "POST",
                json.dumps({"localId": ["native-user"]}).encode(),
            )
            if status != 200 or json.loads(body).get("users", [{}])[0].get("localId") != "native-user":
                raise RuntimeError(f"persistent Auth restart failed: {status} {body!r}")
            status, body = http_request(
                f"http://127.0.0.1:{storage}/demo-release.appspot.com/folder%2Fobject.bin",
                "GET",
            )
            if status != 200 or body != storage_bytes:
                raise RuntimeError(f"persistent Storage restart failed: {status} {body!r}")
        finally:
            stop(process)

        node = str(args.node.resolve()) if args.node else shutil.which("node")
        if not node:
            raise RuntimeError("Node is required for the packaged Functions smoke test")
        node_major = subprocess.check_output([node, "-p", "process.versions.node.split('.')[0]"], text=True).strip()
        if node_major != "22":
            raise RuntimeError(f"packaged Functions smoke requires Node 22, got {node_major}")

        source = root / "smoke-functions"
        source.mkdir()
        (source / "index.cjs").write_text(
            "'use strict';\nexports.smoke = async (req, res) => res.status(202).json({ok:true});\n",
            encoding="utf-8",
        )
        (source / "package.json").write_text(
            json.dumps({"name": "firebase-emu-release-smoke", "private": True, "main": "index.cjs"}),
            encoding="utf-8",
        )
        (source / ".firebase-emu-functions.json").write_text(json.dumps({"functions": [{
            "name": "smokeHttp",
            "handler": "smoke",
            "region": "us-central1",
            "trigger": {"type": "http"},
        }]}), encoding="utf-8")
        functions, firestore, auth, storage, database, pubsub = (free_port() for _ in range(6))
        (root / "firebase.json").write_text(json.dumps({
            "functions": {"source": "smoke-functions", "runtime": "nodejs22"},
            "emulators": {
                "functions": {"host": "127.0.0.1", "port": functions},
                "firestore": {"host": "127.0.0.1", "port": firestore},
                "auth": {"host": "127.0.0.1", "port": auth},
                "storage": {"host": "127.0.0.1", "port": storage},
                "database": {"host": "127.0.0.1", "port": database},
                "pubsub": {"host": "127.0.0.1", "port": pubsub},
            },
        }), encoding="utf-8")

        source_adapter = args.repository.resolve() / "functions-runtime/adapter.cjs"
        hidden_adapter = source_adapter.with_name("adapter.cjs.release-smoke-hidden")
        if hidden_adapter.exists():
            raise RuntimeError(f"refusing to overwrite {hidden_adapter}")
        source_adapter.rename(hidden_adapter)
        process = None
        try:
            environment.update({"FIREBASE_FUNCTIONS_NODE_22": node})
            process = run_binary(binary, ["--config", str(root), "--project", "demo-release"], environment)
            wait_tcp(functions, process)
            body = wait_http(
                f"http://127.0.0.1:{functions}/demo-release/us-central1/smokeHttp",
                process,
                202,
                b"{}",
            )
            if json.loads(body) != {"ok": True}:
                raise RuntimeError(f"Functions smoke returned unexpected body: {body!r}")
        except Exception as error:
            if process is not None:
                stop(process)
            stderr = b"" if process is None or process.stderr is None else process.stderr.read(16384)
            raise RuntimeError(f"packaged Functions smoke failed: {error}\n{stderr.decode(errors='replace')}") from error
        finally:
            if process is not None:
                stop(process)
            hidden_adapter.rename(source_adapter)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--node", type=Path)
    smoke(parser.parse_args())


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
