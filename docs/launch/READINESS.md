# Public beta readiness

Status: **Not ready to publish.**

This page separates historical release evidence from the results for the
public-beta review branch. The announcement files are drafts. Do not post them.

## Review scope

The review branch starts at `0a3e40f`, the source commit for published
`v0.1.4`. It integrates these reviewed worker commits:

- Documentation: `7deff787c238ea7559edd893d7cdd9f2ae7d80bd`
- Security: `979e687f1b521c636e224f3f9c136f9239c2b68d`
- Release workflow: `86c7616ebdced5be2b5cb9fb0c913571249a9048`
- Published-release acceptance: `e292828c23531027583ae843a0ca9bdcfc28e077`
  and `2876259080f23ade086e3af42080ea613acaa5ac`
- Contributor support: `7cb49399461a6c516039a5278ece0a64b696b57f`
- Draft launch materials: `3cb0a85ac9876cd27fbdadc6973a26ceb9fc69f7`

Integration corrections add README links, remove the invalid startup
comparison, make the demo show the real empty Pub/Sub state, correct screenshot
provenance, and move automatic patch versions through a reviewed pull request.

## Evidence for published v0.1.4

The published v0.1.4 archives passed the five-platform acceptance workflow:
https://github.com/dimavedenyapin/firebase-emu/actions/runs/34738183312

This result checks the old published archives. It does not check the security,
documentation, or workflow changes in this review branch. The v0.1.4 archives
have SHA-256 checksums. They do not have the new build attestations.

The screenshots in `docs/images/` also come from a local build of base commit
`0a3e40f`. They are not published-archive evidence and are not patched-branch
screenshots.

## Evidence for the review branch

The review pull request is:
https://github.com/dimavedenyapin/firebase-emu/pull/10

The pull request checks page records exact-head source CI and published v0.1.4
acceptance for the final review head:
https://github.com/dimavedenyapin/firebase-emu/pull/10/checks

A manual `Release binaries` run builds and smoke-tests the five native targets
from the review branch. Its publish job must remain skipped. The pull request
and the task report record the final run URL.

Local integration checks passed 87/87 Rust tests and a locked release build.
A real browser on nondefault loopback ports passed Auth, Firestore, and the real
empty Pub/Sub state. A Firestore integer edit and document clone remained after
a process restart. The clone no-overwrite check returned the expected HTTP 409.
Stored script markup in a field name and value did not execute. The normal UI
path had zero browser errors and warnings; the deliberate duplicate-clone check
added only the expected failed-resource entry for HTTP 409.

Exact same-origin UI requests passed on IPv4 and IPv6. A different loopback
Origin and an external Host both returned HTTP 403. Accepted and rejected UI
responses included CSP, `nosniff`, and `no-store` headers. The old screenshots
still match the material UI behavior, so they were not replaced. Their old-base
provenance remains explicit.

The security worker recorded 87 passing Rust tests, Node SDK 19/19, browser SDK
15/15, Functions adapter 10/10, full Functions 1/1, production Functions npm
audit 0, and Rust audit 0. These results apply to the security worker commit,
not to a published release. Development and legacy SDK fixtures still report
5 moderate, 10 high, and 1 critical npm finding. They are not in the packaged
Functions production dependencies.

## Repository controls

The release workflow now creates a version pull request when an automatic patch
change is necessary. It cannot push that change directly to `main`. A person
must approve the exact version commit after `Rust and Functions checks` passes.
After merge, the workflow runs exact-head CI again before it can build, attest,
tag, or publish.

The active `main-beta-hardening-core` ruleset requires the pull request, one
approval after the last push, resolved review threads, a current branch, and
the GitHub Actions `Rust and Functions checks` result. It will continue to block
force-push and deletion. It has no bypass actor. The repository keeps default
workflow permissions read-only and permits GitHub Actions to create pull
requests. The release workflow has only its explicit job permissions and has no
approval or merge step.

## Remaining launch actions

1. Review and approve the public-beta preparation pull request.
2. Merge it only after the required exact-head check passes.
3. Review and merge the generated version pull request for the selected patched
   release. The expected automatic patch is v0.1.5 unless a higher version is
   selected in the review pull request.
4. Confirm that automatic release CI, five native builds, package smoke tests,
   checksums, attestations, and publication all pass for that release.
5. Run published five-platform acceptance for that new tag.
6. Update the draft announcement to use the selected patched tag. Then perform
   a final editorial review before any post.

The public beta is ready only after all six actions are complete.
