#!/usr/bin/env python3
"""Run native smoke tests against an extracted release archive."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile


MAX_ARCHIVE_MEMBERS = 25_000
MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024


@dataclass
class ManagedProcess:
    process: subprocess.Popen[bytes]
    log_path: Path


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def process_log(process: ManagedProcess) -> str:
    try:
        return process.log_path.read_text(encoding="utf-8", errors="replace")[-32_768:]
    except OSError:
        return "<log unavailable>"


def wait_tcp(port: int, process: ManagedProcess, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise RuntimeError(
                f"firebase-emu exited {process.process.returncode}\n{process_log(process)}"
            )
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
    process: ManagedProcess,
    expected_status: int,
    data: bytes | None = None,
    timeout: float = 30,
) -> bytes:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise RuntimeError(
                f"firebase-emu exited {process.process.returncode}\n{process_log(process)}"
            )
        try:
            status, body = http_status(url, data)
            if status == expected_status:
                return body
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for HTTP {expected_status} from {url}: {last_error}")


def stop(process: ManagedProcess) -> None:
    child = process.process
    if child.poll() is not None:
        return
    try:
        if os.name == "nt":
            child.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(child.pid, signal.SIGINT)
        child.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(child.pid), "/T", "/F"],
                check=False,
                capture_output=True,
            )
        else:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        child.wait(timeout=10)


def safe_member_name(name: str) -> PurePosixPath:
    if not name or "\\" in name or ":" in name:
        raise RuntimeError(f"unsafe archive member name: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise RuntimeError(f"unsafe archive member path: {name!r}")
    return path


def safe_link_target(member: PurePosixPath, target: str, hard_link: bool) -> None:
    if not target or "\\" in target or PurePosixPath(target).is_absolute():
        raise RuntimeError(f"unsafe archive link target: {member} -> {target!r}")
    base = PurePosixPath() if hard_link else member.parent
    normalized = posixpath.normpath(str(base / target))
    if normalized == ".." or normalized.startswith("../"):
        raise RuntimeError(f"archive link escapes extraction root: {member} -> {target!r}")


def extract(archive: Path, destination: Path) -> None:
    if archive.name.endswith(".tar.gz"):
        with tarfile.open(archive, "r:gz") as source:
            members = source.getmembers()
            if len(members) > MAX_ARCHIVE_MEMBERS:
                raise RuntimeError("archive contains too many members")
            total_size = 0
            names: set[PurePosixPath] = set()
            for member in members:
                name = safe_member_name(member.name.rstrip("/"))
                if name in names:
                    raise RuntimeError(f"duplicate archive member: {name}")
                names.add(name)
                if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                    raise RuntimeError(f"unsupported archive member type: {name}")
                if member.isfile():
                    total_size += member.size
                if member.issym() or member.islnk():
                    safe_link_target(name, member.linkname, member.islnk())
            if total_size > MAX_UNCOMPRESSED_BYTES:
                raise RuntimeError("archive uncompressed size exceeds limit")
            source.extractall(destination, filter="data")
    elif archive.name.endswith(".zip"):
        with zipfile.ZipFile(archive) as source:
            members = source.infolist()
            if len(members) > MAX_ARCHIVE_MEMBERS:
                raise RuntimeError("archive contains too many members")
            total_size = 0
            names: set[PurePosixPath] = set()
            for member in members:
                name = safe_member_name(member.filename.rstrip("/"))
                if name in names:
                    raise RuntimeError(f"duplicate archive member: {name}")
                names.add(name)
                if member.flag_bits & 0x1:
                    raise RuntimeError(f"encrypted archive member is not allowed: {name}")
                if stat.S_ISLNK(member.external_attr >> 16):
                    raise RuntimeError(f"ZIP symbolic link is not allowed: {name}")
                total_size += member.file_size
            if total_size > MAX_UNCOMPRESSED_BYTES:
                raise RuntimeError("archive uncompressed size exceeds limit")
            source.extractall(destination)
    else:
        raise RuntimeError(f"unsupported archive format: {archive.name}")


def run_binary(
    binary: Path,
    arguments: list[str],
    environment: dict[str, str],
    log_path: Path,
) -> ManagedProcess:
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    with log_path.open("wb") as log:
        child = subprocess.Popen(
            [str(binary), *arguments],
            cwd=binary.parent,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=flags,
            start_new_session=os.name != "nt",
        )
    return ManagedProcess(child, log_path)


def clean_environment(root: Path) -> dict[str, str]:
    allowed = (
        "PATH",
        "PATHEXT",
        "SystemRoot",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "LANG",
        "LC_ALL",
        "TZ",
    )
    environment = {name: os.environ[name] for name in allowed if name in os.environ}
    environment.update(
        {
            "HOME": str(root),
            "USERPROFILE": str(root),
            "TMPDIR": str(root),
            "TEMP": str(root),
            "TMP": str(root),
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        }
    )
    return environment


def smoke(args: argparse.Namespace) -> None:
    archive = args.archive.resolve()
    with tempfile.TemporaryDirectory(prefix="firebase-emu-native-smoke-") as temporary:
        root = Path(temporary)
        extract(archive, root)
        binary = root / ("firebase-emu.exe" if os.name == "nt" else "firebase-emu")
        if not binary.is_file() or binary.is_symlink() or binary.resolve().parent != root.resolve():
            raise RuntimeError("archive does not contain a safe top-level firebase-emu binary")
        adapter = root / "functions-runtime" / "adapter.cjs"
        if not adapter.is_file() or adapter.is_symlink():
            raise RuntimeError("archive does not contain a safe Functions adapter")
        if os.name != "nt":
            if not os.access(binary, os.X_OK):
                raise RuntimeError("extracted firebase-emu is not executable")
        help_result = subprocess.run([str(binary), "--help"], capture_output=True, text=True, timeout=15)
        if help_result.returncode or "firebase-emu" not in help_result.stdout:
            raise RuntimeError(f"--help failed: {help_result.stdout}\n{help_result.stderr}")

        environment = clean_environment(root)
        firestore, auth, storage, pubsub, ui = (free_port() for _ in range(5))
        environment.update({
            "FIRESTORE_EMU_PORT": str(firestore),
            "FIREBASE_AUTH_EMU_PORT": str(auth),
            "FIREBASE_STORAGE_EMU_PORT": str(storage),
            "PUBSUB_EMULATOR_PORT": str(pubsub),
            "FIREBASE_UI_EMU_PORT": str(ui),
        })
        process = run_binary(binary, ["--no-functions"], environment, root / "listeners.log")
        try:
            for port in (firestore, auth, storage, pubsub):
                wait_tcp(port, process)
            wait_tcp(ui, process)
            status, console = http_status(f"http://127.0.0.1:{ui}/")
            if status != 200 or b"Firestore" not in console:
                raise RuntimeError("packaged console did not load")
            if args.require_firerust_brand:
                if (
                    b"<title>FireRust Console</title>" not in console
                    or b'img src="/firerust-logo.png"' not in console
                ):
                    raise RuntimeError("packaged FireRust console did not load")
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{ui}/firerust-logo.png", timeout=5
                ) as logo_response:
                    logo = logo_response.read()
                    content_type = logo_response.headers.get_content_type()
                    policy = logo_response.headers.get("content-security-policy", "")
                if content_type != "image/png" or not logo.startswith(b"\x89PNG\r\n\x1a\n"):
                    raise RuntimeError("packaged FireRust logo is not a PNG response")
                if "img-src 'self'" not in policy:
                    raise RuntimeError("packaged FireRust logo response has no image CSP")
            status, config = http_status(f"http://127.0.0.1:{ui}/api/config")
            if status != 200 or "defaultProject" not in json.loads(config):
                raise RuntimeError("packaged console config failed")
        finally:
            stop(process)

        data_dir = root / "persistent data with spaces"
        persistence_args = ["--data-dir", str(data_dir), "--no-functions"]
        process = run_binary(binary, persistence_args, environment, root / "persistence-seed.log")
        try:
            for port in (firestore, auth, storage, pubsub):
                wait_tcp(port, process)
            duplicate_environment = environment.copy()
            duplicate_ports = (free_port() for _ in range(5))
            duplicate_environment.update(dict(zip(
                (
                    "FIRESTORE_EMU_PORT",
                    "FIREBASE_AUTH_EMU_PORT",
                    "FIREBASE_STORAGE_EMU_PORT",
                    "PUBSUB_EMULATOR_PORT",
                    "FIREBASE_UI_EMU_PORT",
                ),
                map(str, duplicate_ports),
            )))
            duplicate = run_binary(
                binary,
                persistence_args,
                duplicate_environment,
                root / "duplicate-owner.log",
            )
            try:
                duplicate.process.wait(timeout=10)
                duplicate_error = process_log(duplicate)
                if duplicate.process.returncode == 0 or "already owned" not in duplicate_error:
                    raise RuntimeError(
                        "duplicate data-directory owner was not rejected: "
                        f"{duplicate.process.returncode} {duplicate_error!r}"
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

        process = run_binary(binary, persistence_args, environment, root / "persistence-restart.log")
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

        process = None
        try:
            environment.update({"FIREBASE_FUNCTIONS_NODE_22": node})
            process = run_binary(
                binary,
                ["--config", str(root), "--project", "demo-release"],
                environment,
                root / "functions.log",
            )
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
            log = "" if process is None else process_log(process)
            raise RuntimeError(f"packaged Functions smoke failed: {error}\n{log}") from error
        finally:
            if process is not None:
                stop(process)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--repository", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--node", type=Path)
    parser.add_argument("--require-firerust-brand", action="store_true")
    smoke(parser.parse_args())


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
