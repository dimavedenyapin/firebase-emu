# Minimal local example

This example needs no Firebase SDK. It needs no Node project. It needs no cloud
credential. It needs no private Peakflo application. It needs only Python 3
and the emulator binary.

1. Start the emulator on its default ports with an empty `demo-local` project.
   Use the [main README install command](../../README.md#install-and-run). For
   example:

   ```sh
   npx --yes github:dimavedenyapin/firebase-emu#v0.1.5 --project demo-local --ui-port 0 --no-functions
   ```

2. From the repository root, run the seed script:

   ```sh
   python3 examples/quickstart/seed.py
   ```

3. Open the printed emulator console URL. Look at the created user and
   document.

The script sends plain HTTP requests to literal loopback URLs. It uses only
synthetic data. It creates one Auth user (`developer@example.test`) and one
Firestore document (`products/starter`). Each write uses a must-not-exist
precondition. The script cannot overwrite or delete existing data.

You can run the script more than once against the same data directory. The
first run creates the demo data. Each later run reports that the data
already exists. It leaves that data unchanged. If the emulator is not
reachable, the script prints the command to start it. It then stops with a
non-zero exit status. It does not print a raw program error.

See [Troubleshooting](../../docs/TROUBLESHOOTING.md) if a step does not work
as described here.

For examples that use a real Firebase SDK, see the
[Node example](../node-app/README.md) and the
[web example](../web-app/README.md) in the adjacent directories.
