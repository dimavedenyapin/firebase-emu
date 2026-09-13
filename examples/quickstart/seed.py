#!/usr/bin/env python3
"""Create synthetic local demo data. Run after the README quickstart.

This script uses only plain HTTP to literal loopback URLs. It needs no
Firebase SDK, no cloud project and no credential. It is safe to run more
than once: it never changes or removes an existing user or document.
"""
import json
import sys
import urllib.error
import urllib.request

PROJECT = "demo-local"
AUTH_PORT = 9099
FIRESTORE_PORT = 8080

START_COMMAND = (
    f"npx --yes github:dimavedenyapin/firebase-emu#v0.1.4 "
    f"--project {PROJECT} --ui-port 0 --no-functions"
)

# Error codes the emulator returns when the demo data already exists from an
# earlier run. Treat these as a safe, expected no-op instead of a failure.
ALREADY_EXISTS_CODES = (
    "DUPLICATE_LOCAL_ID",
    "EMAIL_EXISTS",
    "FAILED_PRECONDITION",
    "ALREADY_EXISTS",
    "precondition failed",
)


class SeedError(Exception):
    """A plain-language failure, safe to print directly to the user."""


def post(port, path, body):
    """Send one POST request with a JSON body to a loopback emulator port.

    Returns the parsed JSON response on success. Raises SeedError with a
    clear message on any connection problem or emulator error response.
    """
    url = f"http://127.0.0.1:{port}/{path}"
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise SeedError(f"{path} -> HTTP {error.code}: {_error_message(error)}") from error
    except urllib.error.URLError as error:
        raise SeedError(
            f"Could not reach the emulator on port {port} ({error.reason}).\n"
            f"Start it first, then run this script again:\n  {START_COMMAND}"
        ) from error


def _error_message(error):
    """Read the emulator's JSON error body, or fall back to the raw text.

    Includes the machine-readable status/code (for example FAILED_PRECONDITION
    or DUPLICATE_LOCAL_ID) alongside the human message, so callers can match
    on the stable code instead of the wording.
    """
    body = error.read().decode("utf-8", errors="replace")
    try:
        detail = json.loads(body)["error"]
        code = detail.get("status") or ""
        message = detail.get("message") or ""
        return f"{code} {message}".strip() if code else message
    except (ValueError, KeyError, TypeError):
        return body or error.reason


def _already_exists(error):
    return any(code in str(error) for code in ALREADY_EXISTS_CODES)


def create_demo_user():
    """Create one synthetic Auth user. Leaves an existing user unchanged."""
    try:
        post(
            AUTH_PORT,
            f"identitytoolkit.googleapis.com/v1/projects/{PROJECT}/accounts",
            {"localId": "demo-user", "email": "developer@example.test"},
        )
        print("Created Auth user demo-user (developer@example.test).")
    except SeedError as error:
        if _already_exists(error):
            print("Auth user demo-user already exists. Left it unchanged.")
        else:
            raise


def create_demo_document():
    """Create one synthetic Firestore document. Never overwrites it.

    The write uses a must-not-exist precondition, so a re-run cannot
    change a document that a previous run, or the user, already created.
    """
    try:
        post(
            FIRESTORE_PORT,
            f"v1/projects/{PROJECT}/databases/(default)/documents:commit",
            {
                "writes": [
                    {
                        "update": {
                            "name": (
                                f"projects/{PROJECT}/databases/(default)"
                                "/documents/products/starter"
                            ),
                            "fields": {
                                "name": {"stringValue": "Starter kit"},
                                "stock": {"integerValue": "12"},
                                "details": {
                                    "mapValue": {
                                        "fields": {
                                            "color": {"stringValue": "blue"}
                                        }
                                    }
                                },
                            },
                        },
                        "currentDocument": {"exists": False},
                    }
                ]
            },
        )
        print("Created Firestore document products/starter.")
    except SeedError as error:
        if _already_exists(error):
            print("Firestore document products/starter already exists. Left it unchanged.")
        else:
            raise


def main():
    try:
        create_demo_user()
        create_demo_document()
    except SeedError as error:
        print(f"Seed failed: {error}", file=sys.stderr)
        return 1
    print(f"Demo data ready in project {PROJECT}. Open the emulator console to view it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
