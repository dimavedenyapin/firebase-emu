# Security policy

## Report a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/dimavedenyapin/firebase-emu/security/advisories/new).
Do not put credentials, real user data, or an exploit for another system in a public issue.
Include the version, the platform, the affected route, and a small synthetic test case.
The maintainer will review reports when capacity is available. There is no response-time guarantee.

## Supported versions and threat model

Security fixes apply to the latest release. Older versions can remain unsupported.

This is a local test tool. It does not implement Security Rules, production identity
verification, IAM, or Storage signature verification. All listeners accept only a
loopback IP address. HTTP services also reject a non-loopback `Host` or HTTP/2
authority. Browser requests must also have a loopback `Origin`. These controls reduce
cross-site access and DNS-rebinding risk. They do not protect the emulator from other
processes in the same user account.

Do not expose an emulator port through a proxy, a tunnel, a public container port,
or a shared remote host. Do not use this emulator as a production service.

Use `demo-` projects and synthetic data. Functions run application code with the
permissions of the current user. Review the code and its environment before you start
it. Do not supply production credentials.

Persistent data can contain local passwords, session tokens, document values, event
data, and Storage objects. Keep the data directory private. Keep it out of version
control. Browser SDK routes permit loopback development origins for SDK compatibility.
Serve the development application from loopback. Use a dedicated browser profile,
and do not open untrusted sites while the emulator runs.

## Releases

The launcher downloads from GitHub over HTTPS by default. It verifies the published
SHA-256 checksum before extraction. A checksum confirms file integrity. It is not an
independent signature because the release publisher supplies the archive and the
checksum. Do not set a custom release URL unless you trust its operator and transport.

New attested releases also provide build provenance. See the
[release instructions](docs/RELEASING.md).
