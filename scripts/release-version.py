#!/usr/bin/env python3
"""Plan and record deterministic versions for automatic default-branch releases."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess


REPOSITORY = Path(__file__).resolve().parent.parent
STABLE_VERSION = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
TAG_VERSION = re.compile(r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def run(*arguments: str) -> str:
    return subprocess.check_output(arguments, cwd=REPOSITORY, text=True).strip()


def cargo_version() -> str:
    text = (REPOSITORY / "Cargo.toml").read_text(encoding="utf-8")
    package = text.split("[package]", 1)[1].split("\n[", 1)[0]
    match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', package, re.MULTILINE)
    if not match or not STABLE_VERSION.fullmatch(match.group(1)):
        raise SystemExit("automatic releases require a stable X.Y.Z Cargo package version")
    return match.group(1)


def version_tuple(version: str) -> tuple[int, int, int]:
    match = STABLE_VERSION.fullmatch(version)
    if not match:
        raise ValueError(f"not a stable semantic version: {version}")
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def next_version(current: str, tags: list[str]) -> tuple[str, str]:
    versions = [tuple(int(part) for part in match.groups()) for tag in tags if (match := TAG_VERSION.fullmatch(tag))]
    if not versions:
        return current, "first-release"
    latest = max(versions)
    if version_tuple(current) > latest:
        return current, "intentional-source-bump"
    return ".".join(str(part) for part in (latest[0], latest[1], latest[2] + 1)), "automatic-patch"


def replace_exact(path: Path, old: str, new: str, count: int) -> None:
    text = path.read_text(encoding="utf-8")
    updated, replacements = re.subn(old, new, text, count=count)
    if replacements != count:
        raise SystemExit(f"expected {count} version field(s) in {path.relative_to(REPOSITORY)}, found {replacements}")
    path.write_text(updated, encoding="utf-8")


def synchronize_version(old: str, new: str) -> None:
    escaped = re.escape(old)
    replace_exact(REPOSITORY / "Cargo.toml", rf'(?m)^(version\s*=\s*)"{escaped}"', rf'\1"{new}"', 1)
    cargo_lock = REPOSITORY / "Cargo.lock"
    text = cargo_lock.read_text(encoding="utf-8")
    pattern = rf'(?ms)(\[\[package\]\]\nname = "firebase-emu"\nversion = )"{escaped}"'
    updated, replacements = re.subn(pattern, rf'\1"{new}"', text, count=1)
    if replacements != 1:
        raise SystemExit("Cargo.lock must contain exactly one firebase-emu package version")
    cargo_lock.write_text(updated, encoding="utf-8")
    for directory in (REPOSITORY, REPOSITORY / "functions-runtime"):
        replace_exact(directory / "package.json", rf'("version"\s*:\s*)"{escaped}"', rf'\1"{new}"', 1)
        replace_exact(directory / "package-lock.json", rf'("version"\s*:\s*)"{escaped}"', rf'\1"{new}"', 2)


def output(name: str, value: str) -> None:
    destination = os.environ.get("GITHUB_OUTPUT")
    if destination:
        with open(destination, "a", encoding="utf-8") as stream:
            stream.write(f"{name}={value}\n")
    print(f"{name}={value}")


def plan() -> None:
    source = os.environ["SOURCE_SHA"]
    branch = os.environ["DEFAULT_BRANCH"]
    default_ref = f"origin/{branch}"
    run("git", "fetch", "origin", branch, "--tags", "--force")

    marker = f"Firebase-Emu-Release-Source: {source}"
    recovered = run("git", "log", default_ref, "--fixed-strings", f"--grep={marker}", "--format=%H")
    recovered_commits = [line for line in recovered.splitlines() if line]
    if len(recovered_commits) > 1:
        raise SystemExit(f"multiple release commits record source {source}; refusing an ambiguous retry")
    if recovered_commits:
        run("git", "checkout", "--detach", recovered_commits[0])
        version = cargo_version()
        output("version", version)
        output("changed", "false")
        output("policy", "retry-recorded-commit")
        return

    tags_at_source = [tag for tag in run("git", "tag", "--points-at", source).splitlines() if TAG_VERSION.fullmatch(tag)]
    if len(tags_at_source) > 1:
        raise SystemExit(f"multiple stable release tags point at {source}; refusing an ambiguous retry")
    if tags_at_source:
        version = tags_at_source[0][1:]
        if cargo_version() != version:
            raise SystemExit(f"existing tag {tags_at_source[0]} disagrees with source manifests")
        output("version", version)
        output("changed", "false")
        output("policy", "retry-existing-tag")
        return

    remote_head = run("git", "rev-parse", default_ref)
    if remote_head != source:
        raise SystemExit(
            f"default branch advanced from event commit {source} to {remote_head}; "
            "refusing a stale version push (rerun the newest automatic-release workflow)"
        )
    current = cargo_version()
    tags = run("git", "tag", "--list", "v*").splitlines()
    version, policy = next_version(current, tags)
    changed = version != current
    if changed:
        synchronize_version(current, version)
    output("version", version)
    output("changed", str(changed).lower())
    output("policy", policy)


if __name__ == "__main__":
    import sys

    if sys.argv[1:] != ["plan"]:
        raise SystemExit("usage: release-version.py plan")
    plan()
