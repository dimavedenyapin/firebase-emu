from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SOURCE_SCRIPT = Path(__file__).resolve().parents[1] / "scripts/release-version.py"
WORKFLOWS = Path(__file__).resolve().parents[1] / ".github/workflows"


class ReleasePlannerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="firebase-emu-release-test-")
        root = Path(self.temporary.name)
        self.remote = root / "origin.git"
        self.repo = root / "repo"
        subprocess.run(["git", "init", "--bare", "--initial-branch=main", self.remote], check=True, capture_output=True)
        subprocess.run(["git", "init", "--initial-branch=main", self.repo], check=True, capture_output=True)
        self.git("config", "user.name", "Release Test")
        self.git("config", "user.email", "release@example.test")
        self.git("remote", "add", "origin", str(self.remote))
        (self.repo / "scripts").mkdir()
        shutil.copy2(SOURCE_SCRIPT, self.repo / "scripts/release-version.py")
        (self.repo / "functions-runtime").mkdir()
        self.write_versions("0.1.0")
        self.git("add", ".")
        self.git("commit", "-m", "initial")
        self.git("push", "-u", "origin", "main")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def git(self, *args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=self.repo, text=True).strip()

    def write_versions(self, version: str) -> None:
        (self.repo / "Cargo.toml").write_text(
            f'[package]\nname = "firebase-emu"\nversion = "{version}"\n\n[dependencies]\n', encoding="utf-8"
        )
        (self.repo / "Cargo.lock").write_text(
            f'version = 4\n\n[[package]]\nname = "firebase-emu"\nversion = "{version}"\n', encoding="utf-8"
        )
        for directory, name in ((self.repo, "firebase-emu-rs"), (self.repo / "functions-runtime", "runtime")):
            (directory / "package.json").write_text(
                json.dumps({"name": name, "version": version}, indent=2) + "\n", encoding="utf-8"
            )
            (directory / "package-lock.json").write_text(
                json.dumps({"name": name, "version": version, "packages": {"": {"name": name, "version": version}, "node_modules/x": {"version": "9.9.9"}}}, indent=2) + "\n",
                encoding="utf-8",
            )

    def plan(self, source: str) -> dict[str, str]:
        output = self.repo / "output"
        output.unlink(missing_ok=True)
        env = os.environ | {"SOURCE_SHA": source, "DEFAULT_BRANCH": "main", "GITHUB_OUTPUT": str(output)}
        subprocess.run(["python3", "scripts/release-version.py", "plan"], cwd=self.repo, env=env, check=True, capture_output=True, text=True)
        return dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())

    def test_first_release_uses_current_version_and_existing_tag_is_retry(self) -> None:
        source = self.git("rev-parse", "HEAD")
        self.assertEqual(self.plan(source), {"version": "0.1.0", "changed": "false", "policy": "first-release"})
        self.git("tag", "v0.1.0", source)
        self.git("push", "origin", "v0.1.0")
        self.assertEqual(self.plan(source)["policy"], "retry-existing-tag")

    def test_subsequent_release_bumps_patch_and_synchronizes_only_root_packages(self) -> None:
        first = self.git("rev-parse", "HEAD")
        self.git("tag", "v0.1.0", first)
        self.git("push", "origin", "v0.1.0")
        (self.repo / "change.txt").write_text("next\n", encoding="utf-8")
        self.git("add", "change.txt")
        self.git("commit", "-m", "next feature")
        source = self.git("rev-parse", "HEAD")
        self.git("push", "origin", "main")
        result = self.plan(source)
        self.assertEqual(result, {"version": "0.1.1", "changed": "true", "policy": "automatic-patch"})
        for path in ("Cargo.toml", "Cargo.lock", "package.json", "package-lock.json", "functions-runtime/package.json", "functions-runtime/package-lock.json"):
            self.assertIn("0.1.1", (self.repo / path).read_text(encoding="utf-8"))
        nested = json.loads((self.repo / "functions-runtime/package-lock.json").read_text(encoding="utf-8"))
        self.assertEqual(nested["packages"]["node_modules/x"]["version"], "9.9.9")

    def test_intentional_minor_or_major_source_bump_is_honored(self) -> None:
        first = self.git("rev-parse", "HEAD")
        self.git("tag", "v0.1.0", first)
        self.git("push", "origin", "v0.1.0")
        self.write_versions("1.0.0")
        self.git("add", ".")
        self.git("commit", "-m", "intentional major")
        source = self.git("rev-parse", "HEAD")
        self.git("push", "origin", "main")
        result = self.plan(source)
        self.assertEqual(result, {"version": "1.0.0", "changed": "false", "policy": "intentional-source-bump"})

    def test_recorded_version_commit_makes_retry_idempotent(self) -> None:
        first = self.git("rev-parse", "HEAD")
        self.git("tag", "v0.1.0", first)
        self.git("push", "origin", "v0.1.0")
        (self.repo / "change.txt").write_text("next\n", encoding="utf-8")
        self.git("add", "change.txt")
        self.git("commit", "-m", "next")
        source = self.git("rev-parse", "HEAD")
        self.git("push", "origin", "main")
        self.plan(source)
        self.git("add", ".")
        self.git("commit", "-m", "Release v0.1.1", "-m", f"Firebase-Emu-Release-Source: {source}")
        release_commit = self.git("rev-parse", "HEAD")
        self.git("push", "origin", "main")
        result = self.plan(source)
        self.assertEqual(result["policy"], "retry-recorded-commit")
        self.assertEqual(self.git("rev-parse", "HEAD"), release_commit)

    def test_stale_event_refuses_to_overwrite_newer_default_branch(self) -> None:
        stale = self.git("rev-parse", "HEAD")
        (self.repo / "change.txt").write_text("newer\n", encoding="utf-8")
        self.git("add", "change.txt")
        self.git("commit", "-m", "newer")
        self.git("push", "origin", "main")
        env = os.environ | {"SOURCE_SHA": stale, "DEFAULT_BRANCH": "main"}
        result = subprocess.run(["python3", "scripts/release-version.py", "plan"], cwd=self.repo, env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing a stale version push", result.stderr)


class WorkflowSafetyTest(unittest.TestCase):
    def test_default_release_is_serialized_and_pr_jobs_are_read_only(self) -> None:
        automatic = (WORKFLOWS / "automatic-release.yml").read_text(encoding="utf-8")
        release = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
        publisher = (WORKFLOWS.parents[1] / "scripts/publish-release.sh").read_text(encoding="utf-8")
        self.assertIn("branches:\n      - main", automatic)
        self.assertIn("cancel-in-progress: false", automatic)
        self.assertIn("uses: ./.github/workflows/ci.yml", automatic)
        self.assertIn("uses: ./.github/workflows/release.yml", automatic)
        self.assertIn("contents: read", release)
        self.assertIn("if: inputs.publish", release)
        self.assertNotIn("push:\n    tags:", release)
        self.assertLess(publisher.index("cmp \"$asset\""), publisher.index("-F draft=false"))
        self.assertIn("refusing to move it", publisher)
        self.assertIn("refusing to mutate it", publisher)


if __name__ == "__main__":
    unittest.main()
