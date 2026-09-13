#!/usr/bin/env python3
"""Download, verify, and test one published native archive."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import re
import subprocess
import sys
import tempfile
from pathlib import Path
import urllib.request


REPOSITORY = "dimavedenyapin/firebase-emu"
TAG_RE = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")
TARGETS = {
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
}
CHECKSUM_RE = re.compile(r"([0-9a-f]{64})  ([A-Za-z0-9_.+-]+)")
MAX_ARCHIVE_DOWNLOAD_BYTES = 600 * 1024 * 1024
MAX_MANIFEST_DOWNLOAD_BYTES = 2 * 1024 * 1024


def download(url: str, destination: Path, maximum_bytes: int) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "firebase-emu-release-acceptance"})
    with urllib.request.urlopen(request, timeout=60) as response, destination.open("wb") as output:
        if response.status != 200:
            raise RuntimeError(f"download returned HTTP {response.status}: {url}")
        content_length = response.headers.get("content-length")
        if content_length is not None and int(content_length) > maximum_bytes:
            raise RuntimeError(f"download exceeds size limit: {url}")
        received = 0
        while chunk := response.read(1024 * 1024):
            received += len(chunk)
            if received > maximum_bytes:
                raise RuntimeError(f"download exceeds size limit: {url}")
            output.write(chunk)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_checksums(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        match = CHECKSUM_RE.fullmatch(line)
        if match is None:
            raise RuntimeError(f"invalid checksum manifest line {line_number}")
        digest, name = match.groups()
        if name in entries:
            raise RuntimeError(f"duplicate checksum manifest entry: {name}")
        entries[name] = digest
    return entries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="v0.1.4")
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument("--node", required=True, type=Path)
    args = parser.parse_args()
    if TAG_RE.fullmatch(args.tag) is None:
        parser.error("tag must be vX.Y.Z")
    if not args.node.is_file():
        parser.error("--node must identify a file")

    root = Path(__file__).resolve().parent.parent
    suffix = ".zip" if args.target.endswith("windows-msvc") else ".tar.gz"
    archive_name = f"firebase-emu-{args.tag}-{args.target}{suffix}"
    manifest_name = f"firebase-emu-{args.tag}-checksums.txt"
    base_url = f"https://github.com/{REPOSITORY}/releases/download/{args.tag}"

    with tempfile.TemporaryDirectory(prefix="firebase-release-acceptance-") as temporary:
        temp = Path(temporary)
        archive = temp / archive_name
        manifest = temp / manifest_name
        download(f"{base_url}/{archive_name}", archive, MAX_ARCHIVE_DOWNLOAD_BYTES)
        download(f"{base_url}/{manifest_name}", manifest, MAX_MANIFEST_DOWNLOAD_BYTES)

        entries = read_checksums(manifest)
        expected = entries.get(archive_name)
        actual = sha256_file(archive)
        if expected is None:
            raise RuntimeError(f"checksum manifest does not list {archive_name}")
        if not hmac.compare_digest(expected, actual):
            raise RuntimeError("archive checksum mismatch")
        print(f"Verified {archive_name}: {actual}", flush=True)

        subprocess.run(
            [
                sys.executable,
                str(root / "scripts/package-release-smoke.py"),
                "--archive",
                str(archive),
                "--node",
                str(args.node.resolve()),
            ],
            check=True,
        )
        print(
            f"PASS {args.tag} {args.target}: published archive, console, "
            "Auth, Firestore, Storage, persistence, lock, restart, and Functions",
            flush=True,
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
