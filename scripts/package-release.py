#!/usr/bin/env python3
"""Build and validate firebase-emu release archives."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile
import tempfile
import tomllib
import zipfile


TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
)
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
REPOSITORY = Path(__file__).resolve().parent.parent


def npm_version(path: Path, expected: str) -> None:
    package = json.loads(path.read_text(encoding="utf-8"))
    if package.get("version") != expected:
        raise SystemExit(f"{path.relative_to(REPOSITORY)} version must be {expected}")
    lock_path = path.with_name("package-lock.json")
    if not lock_path.is_file():
        raise SystemExit(f"missing {lock_path.relative_to(REPOSITORY)}")
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    lock_version = lock.get("packages", {}).get("", {}).get("version", lock.get("version"))
    if lock_version != expected:
        raise SystemExit(f"{lock_path.relative_to(REPOSITORY)} version must be {expected}")


def source_version(requested: str | None, tag: str | None) -> str:
    cargo = tomllib.loads((REPOSITORY / "Cargo.toml").read_text(encoding="utf-8"))
    package = cargo.get("package", {})
    version = package.get("version")
    if package.get("name") != "firebase-emu" or not isinstance(version, str):
        raise SystemExit("Cargo.toml must describe the firebase-emu package")
    if not VERSION_RE.fullmatch(version):
        raise SystemExit(f"unsupported Cargo package version: {version!r}")
    if requested is not None and requested != version:
        raise SystemExit(f"requested version {requested} does not match Cargo version {version}")

    npm_version(REPOSITORY / "functions-runtime/package.json", version)
    root_package = REPOSITORY / "package.json"
    if root_package.exists():
        npm_version(root_package, version)

    if tag and tag != f"v{version}":
        raise SystemExit(f"release tag {tag!r} must be exactly v{version}")
    for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
        if not (REPOSITORY / name).is_file():
            raise SystemExit(f"missing required release file: {name}")
    return version


def archive_name(version: str, target: str) -> str:
    suffix = ".zip" if target.endswith("windows-msvc") else ".tar.gz"
    return f"firebase-emu-v{version}-{target}{suffix}"


def normalized_tar_info(info: tarfile.TarInfo, epoch: int) -> tarfile.TarInfo:
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    info.mtime = epoch
    return info


def package(args: argparse.Namespace) -> None:
    version = source_version(args.version, args.tag)
    target = args.target
    if target not in TARGETS:
        raise SystemExit(f"unsupported release target: {target}")
    binary_name = "firebase-emu.exe" if target.endswith("windows-msvc") else "firebase-emu"
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"release binary does not exist: {binary}")
    runtime = REPOSITORY / "functions-runtime"
    for name in ("adapter.cjs", "package.json", "package-lock.json", "node_modules"):
        if not (runtime / name).exists():
            raise SystemExit(f"missing packaged Functions runtime path: functions-runtime/{name}")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    destination = output / archive_name(version, target)
    if destination.exists():
        destination.unlink()

    with tempfile.TemporaryDirectory(prefix="firebase-emu-release-") as temporary:
        stage = Path(temporary)
        shutil.copyfile(binary, stage / binary_name)
        if not target.endswith("windows-msvc"):
            (stage / binary_name).chmod(0o755)
        packaged_runtime = stage / "functions-runtime"
        packaged_runtime.mkdir()
        for name in ("adapter.cjs", "package.json", "package-lock.json"):
            shutil.copy2(runtime / name, packaged_runtime / name)
        shutil.copytree(runtime / "node_modules", packaged_runtime / "node_modules", symlinks=True)
        for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
            shutil.copy2(REPOSITORY / name, stage / name)

        if target.endswith("windows-msvc"):
            with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
                for path in sorted(stage.rglob("*")):
                    if path.is_file():
                        archive.write(path, path.relative_to(stage).as_posix())
        else:
            epoch = int(os.environ.get("SOURCE_DATE_EPOCH", "0"))
            with destination.open("wb") as raw:
                with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=epoch) as compressed:
                    with tarfile.open(fileobj=compressed, mode="w", dereference=False) as archive:
                        for name in (binary_name, "functions-runtime", "LICENSE", "THIRD_PARTY_NOTICES.md"):
                            archive.add(
                                stage / name,
                                arcname=name,
                                recursive=True,
                                filter=lambda info: normalized_tar_info(info, epoch),
                            )
    verify_archive(destination, version, target)
    print(destination)


def member_names(path: Path) -> tuple[list[str], int | None]:
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getmembers()
            binary = next((m for m in members if m.name == "firebase-emu"), None)
            return [m.name.rstrip("/") for m in members if m.name.rstrip("/")], binary.mode if binary else None
    with zipfile.ZipFile(path) as archive:
        return [name.rstrip("/") for name in archive.namelist() if name.rstrip("/")], None


def verify_archive(path: Path, version: str, target: str) -> None:
    expected_name = archive_name(version, target)
    if path.name != expected_name:
        raise SystemExit(f"archive must be named {expected_name}, got {path.name}")
    names, binary_mode = member_names(path)
    binary_name = "firebase-emu.exe" if target.endswith("windows-msvc") else "firebase-emu"
    required = {
        binary_name,
        "functions-runtime/adapter.cjs",
        "functions-runtime/package.json",
        "functions-runtime/package-lock.json",
        "LICENSE",
        "THIRD_PARTY_NOTICES.md",
    }
    missing = sorted(required.difference(names))
    if missing:
        raise SystemExit(f"archive is missing required paths: {', '.join(missing)}")
    if not any(name.startswith("functions-runtime/node_modules/") for name in names):
        raise SystemExit("archive is missing production functions-runtime/node_modules contents")
    allowed_roots = {binary_name, "functions-runtime", "LICENSE", "THIRD_PARTY_NOTICES.md"}
    unexpected = sorted({PurePosixPath(name).parts[0] for name in names}.difference(allowed_roots))
    if unexpected:
        raise SystemExit(f"archive has unexpected root paths: {', '.join(unexpected)}")
    if binary_mode is not None and not binary_mode & stat.S_IXUSR:
        raise SystemExit("POSIX archive binary is not executable")


def verify(args: argparse.Namespace) -> None:
    version = source_version(args.version, args.tag)
    verify_archive(args.archive.resolve(), version, args.target)
    print(args.archive.resolve())


def checksums(args: argparse.Namespace) -> None:
    version = source_version(args.version, args.tag)
    directory = args.directory.resolve()
    expected = [archive_name(version, target) for target in TARGETS]
    actual = sorted(path.name for path in directory.iterdir() if path.name.endswith((".tar.gz", ".zip")))
    if actual != sorted(expected):
        raise SystemExit(f"release archives do not match contract; expected {sorted(expected)}, got {actual}")
    lines = []
    for name in expected:
        digest = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        lines.append(f"{digest}  {name}")
    manifest = directory / f"firebase-emu-v{version}-checksums.txt"
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(manifest)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    commands = result.add_subparsers(dest="command", required=True)
    version = commands.add_parser("version")
    version.add_argument("--version")
    version.add_argument("--tag", default=os.environ.get("RELEASE_TAG"))

    create = commands.add_parser("package")
    create.add_argument("--version", required=True)
    create.add_argument("--tag", default=os.environ.get("RELEASE_TAG"))
    create.add_argument("--target", required=True, choices=TARGETS)
    create.add_argument("--binary", required=True, type=Path)
    create.add_argument("--output", required=True, type=Path)

    inspect = commands.add_parser("verify")
    inspect.add_argument("--version", required=True)
    inspect.add_argument("--tag", default=os.environ.get("RELEASE_TAG"))
    inspect.add_argument("--target", required=True, choices=TARGETS)
    inspect.add_argument("--archive", required=True, type=Path)

    sums = commands.add_parser("checksums")
    sums.add_argument("--version", required=True)
    sums.add_argument("--tag", default=os.environ.get("RELEASE_TAG"))
    sums.add_argument("--directory", required=True, type=Path)
    return result


def main() -> None:
    args = parser().parse_args()
    if args.command == "version":
        print(source_version(args.version, args.tag))
    elif args.command == "package":
        package(args)
    elif args.command == "verify":
        verify(args)
    else:
        checksums(args)


if __name__ == "__main__":
    main()
