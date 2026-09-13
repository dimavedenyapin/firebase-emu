# Release process

A push to `main` starts an automatic release.

Steps in the automatic release:
1. The workflow picks the correct version number.
2. The workflow commits the version files. The commit author is `github-actions[bot]`.
3. The workflow pushes the version commit to `main`. This push is always a fast-forward push. The workflow does not force-push.
4. The workflow runs the required Rust and Node CI checks on the exact version commit.
5. If the checks pass, the workflow builds and smoke-tests five native targets.
6. The workflow verifies the complete release bundle.
7. The workflow creates the tag and the GitHub Release.

You do not need to create a tag by hand. You do not need to open a separate release pull request.

## Version numbers

The first merge publishes version `0.1.0`, if tag `v0.1.0` does not exist yet.

After the first release, the workflow raises the version by one patch number. Example: `0.1.0` becomes `0.1.1`.

To release a minor or major version, change all six version files in the same feature pull request. Set each file to a stable version. This version must be higher than every existing release. Change these files:
- `Cargo.toml`
- the `firebase-emu` entry in `Cargo.lock`
- the root `package.json`
- the root package entry in `package-lock.json`
- `functions-runtime/package.json`
- `functions-runtime/package-lock.json`

The workflow uses this higher version. If the version is not higher than the latest release, the workflow ignores it and uses the automatic patch number instead.

## Safety rules for the automatic version commit

- The version commit carries a `Firebase-Emu-Release-Source` trailer. Retries reuse the same commit.
- Release runs on `main` run one at a time (serialized). A stale run cannot overwrite newer work.
- An existing tag must already point at the exact release commit.
- An existing published release must have byte-identical assets.
- The workflow uses the built-in `GITHUB_TOKEN` for the commit, the tag, and the release. It does not use a personal access token.
- A tag pushed with `GITHUB_TOKEN` does not start a new workflow run. This is why the build job and the publish job stay in one workflow graph.

## Branch protection on `main`

As of this hardening pass, `main` has one active repository ruleset: `main-beta-hardening-core`. This ruleset blocks two actions for every actor, with no exceptions:
- Force-push to `main`.
- Deletion of `main`.

The automatic release commit is a normal fast-forward push. This ruleset does not block it.

This ruleset does **not** require a pull request before a push to `main`. This ruleset does **not** require a passing CI check before a push to `main`. Here is why.

GitHub lets a GitHub App bypass ruleset rules only when the repository belongs to an organization. This repository belongs to a personal account, not an organization. A request to add the `github-actions` App (App ID `15368`) as a bypass actor on this repository fails with this exact error:

```
Actor GitHub Actions integration must be part of the ruleset source or owner organization
```

Without that bypass, a "require pull request" rule or a "require status checks" rule would also block the automatic version commit. That commit pushes straight to `main` before its own CI run starts, so it can never carry a passing check at push time.

Do not add a "require pull request before merging" rule or a "require status checks to pass" rule for `main` until one of these is true:
1. The repository moves to a GitHub organization. Then add the `github-actions` App as a bypass actor, with bypass mode "always", scoped only to those two rules.
2. The automatic release commit changes so it goes through a pull request with auto-merge, instead of a direct push.

Until then, the required CI check (`Rust and Functions checks`, from `ci.yml`) stays informational on a direct push to `main`. It stays a real, enforced gate for the automatic release itself: the `quality` job in `automatic-release.yml` must pass before the `release` job runs and publishes anything.

## Launch checks

Run the manual "Published release acceptance" workflow for the selected tag. Review all five native jobs. PR CI checks the source code. It does not check a published release. Do not treat a passing PR CI run as proof that a release is good.

New published archives include GitHub build attestations. Verify an archive with:

```
gh attestation verify ARCHIVE --repo dimavedenyapin/firebase-emu
```

The historical `v0.1.4` archives have checksums, but they do not have attestations. The v0.1.4 build ran before this attestation step existed.
