# FireRust troubleshooting

This page does not need a private Peakflo application. It covers only the public
FireRust beta and the examples in this repository: the
[minimal example](../examples/quickstart/README.md), the
[Node example](../examples/node-app/README.md), and the
[web example](../examples/web-app/README.md).

| Symptom | Check |
| --- | --- |
| Port occupied | Stop the process you own or select service ports with the [documented environment variables](../README.md#install-and-run). `--ui-port 0` selects a free console port only. |
| Binary will not start | Check the release platform and OS/libc baseline. Older Linux glibc is not supported by the published baseline. |
| Functions absent | Install your application's locked dependencies, select Node 22, and pass `--config` or the Functions source. `--no-functions` disables workers. |
| Data disappears | Use `--data-dir PATH`; the default is memory storage. Resolve the startup log path before changing anything. |
| Data directory already owned | Stop its existing owner. Do not delete the lock while that process runs. |
| Trigger runs twice | Delivery is at least once. Make the handler idempotent using stable event IDs. |
| Drain does not finish | Inspect handler failures and self-triggering writes. A continuing loop remains pending. |
| Production differs | Check the compatibility table. Security Rules, indexes and isolation are not reproduced. |
| Download checksum fails | Do not run the file. Retry the release download; report the release tag, platform and error. |

For a bug report include the release tag, OS/architecture, Node and SDK versions,
startup arguments without secrets, and a minimal demo-project reproduction.
[Open a bug report](https://github.com/dimavedenyapin/firebase-emu/issues/new/choose)
with the bug issue form.

## Support policy

This is a public beta project with one maintainer. Support is best effort.

- The maintainer reviews issues and pull requests when time allows. There is
  no fixed response time. There is no service-level agreement.
- There is no paid support channel. There is no dedicated support channel.
- Use [GitHub Issues](https://github.com/dimavedenyapin/firebase-emu/issues)
  for bugs and questions.
- Use [private vulnerability reporting](https://github.com/dimavedenyapin/firebase-emu/security/advisories/new)
  for security problems. See [SECURITY.md](../SECURITY.md).
- The maintainer can close a report that falls outside the beta scope in
  [Compatibility](COMPATIBILITY.md). The maintainer can decline a change
  outside that scope. See [Contributing](../CONTRIBUTING.md).
