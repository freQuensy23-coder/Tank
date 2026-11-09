"""Publish the S29 X worker (`apify/x-reader`) to its Apify actor during a Tank deploy.

`deploy.sh` runs this on the production host with the worker source of the deployed commit:

    apify_worker.py publish <worker dir> <release sha>   build and activate the source if it changed
    apify_worker.py restore <build id>                   point the `direct` tag back at a build

`publish` compares the worker files with the source of the build tagged `direct` (README.md is
documentation and is ignored). Unchanged source does nothing. Changed source replaces the files of
actor version 0.3, builds it under the tag `tank-<sha12>` and, only after the build succeeded, moves
`direct` to the new build. It prints the build `direct` pointed at before the deploy and the one it
points at now, so a Tank rollback can restore the former. If retagging fails, it points `direct` back
at the previous build before failing. The API token is read from the host `.env` and never printed.
"""

import base64
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ACTOR_ID = "rbf5CQKQgKNVl4SfO"
VERSION = "0.3"
TAG = "direct"
IGNORED = {"README.md", "tank-release.json"}
BUILD_ID = re.compile(r"[A-Za-z0-9]{17}")
BUILD_DEADLINE_S = 600
API = "https://api.apify.com/v2"


class PublishError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise PublishError(message)


def host_token(env_file):
    values = []
    for line in Path(env_file).read_text().splitlines():
        match = re.fullmatch(r"\s*(?:export\s+)?APIFY_API_TOKEN\s*=\s*(.*?)\s*", line)
        if match:
            value = match[1]
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values.append(value)
    require(
        len(values) == 1 and re.fullmatch(r"[A-Za-z0-9_-]+", values[0]), "APIFY_API_TOKEN missing in .env"
    )
    return values[0]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Api:
    def __init__(self, token, base=API):
        self.token = token
        self.base = base
        self.opener = urllib.request.build_opener(NoRedirect, urllib.request.ProxyHandler({}))

    def request(self, method, path, body=None):
        request = urllib.request.Request(
            self.base + path,
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
        )
        try:
            with self.opener.open(request, timeout=75) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            raise PublishError(f"Apify {method} {path.split('?')[0]} failed with HTTP {error.code}") from None
        except (OSError, ValueError):
            raise PublishError(f"Apify {method} {path.split('?')[0]} transport or JSON failure") from None
        require(isinstance(value, dict) and isinstance(value.get("data"), dict), "invalid Apify response")
        return value["data"]


def local_files(root):
    root = Path(root)
    files = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "worker source contains a symlink")
        if path.is_file():
            files[path.relative_to(root).as_posix()] = path.read_bytes()
    require({"main.py", "Dockerfile", ".actor/actor.json"} <= files.keys(), "worker source incomplete")
    return files


def build_files(build):
    files = {}
    for entry in build.get("actVersion", {}).get("sourceFiles", []):
        if entry.get("folder"):
            continue
        content = entry.get("content", "")
        files[entry["name"]] = (
            base64.b64decode(content) if entry.get("format") == "BASE64" else content.encode()
        )
    return files


def worker_source(files):
    return {name: body for name, body in files.items() if name not in IGNORED}


class Worker:
    def __init__(self, api, clock=time.monotonic):
        self.api = api
        self.clock = clock
        self.actor = "/actors/" + ACTOR_ID

    def current(self):
        actor = self.api.request("GET", self.actor)
        build_id = (actor.get("taggedBuilds") or {}).get(TAG, {}).get("buildId")
        require(isinstance(build_id, str) and BUILD_ID.fullmatch(build_id), f"actor has no {TAG} build")
        return build_id

    def tag(self, build_id):
        require(isinstance(build_id, str) and BUILD_ID.fullmatch(build_id), "invalid build ID")
        self.api.request("PUT", self.actor, {"taggedBuilds": {TAG: {"buildId": build_id}}})
        require(self.current() == build_id, f"{TAG} tag did not move to {build_id}")

    def publish(self, root, sha):
        """Return (build `direct` pointed at before, build it points at now)."""
        require(re.fullmatch(r"[0-9a-f]{40}", sha), "invalid release SHA")
        files = local_files(root)
        previous = self.current()
        active = self.api.request("GET", "/actor-builds/" + previous)
        if worker_source(build_files(active)) == worker_source(files):
            return previous, previous
        self.api.request(
            "PUT",
            self.actor + "/versions/" + VERSION,
            {
                "versionNumber": VERSION,
                "sourceType": "SOURCE_FILES",
                "buildTag": None,
                "envVars": [],
                "applyEnvVarsToBuild": False,
                "sourceFiles": [
                    {"name": name, "format": "BASE64", "content": base64.b64encode(body).decode()}
                    for name, body in sorted(files.items())
                ],
            },
        )
        build = self.api.request("POST", self.actor + f"/builds?version={VERSION}&tag=tank-{sha[:12]}")
        build_id = build.get("id")
        require(isinstance(build_id, str) and BUILD_ID.fullmatch(build_id), "build ID missing")
        deadline = self.clock() + BUILD_DEADLINE_S
        while build.get("status") in {"READY", "RUNNING"} and self.clock() < deadline:
            build = self.api.request("GET", "/actor-builds/" + build_id + "?waitForFinish=60")
        require(build.get("status") == "SUCCEEDED", f"worker build {build_id} ended {build.get('status')}")
        built = worker_source(build_files(build))
        require(built == worker_source(files), "built source differs from the release")
        require(self.current() == previous, f"{TAG} tag changed while building")
        try:
            self.tag(build_id)
        except PublishError:
            try:
                self.tag(previous)
            except PublishError as error:
                message = f"{TAG} may point at {build_id}; restoring {previous} failed: {error}"
                raise PublishError(message) from None
            raise
        return previous, build_id


def main(argv=None, env_file=".env"):
    argv = sys.argv[1:] if argv is None else argv
    try:
        worker = Worker(Api(host_token(env_file)))
        if len(argv) == 3 and argv[0] == "publish":
            previous, current = worker.publish(argv[1], argv[2])
            state = "unchanged" if previous == current else "published"
            print(f"X worker {state}: {TAG} {previous} -> {current}", file=sys.stderr)
            print(previous, current)
        elif len(argv) == 2 and argv[0] == "restore":
            worker.tag(argv[1])
            print(f"X worker restored: {TAG} -> {argv[1]}", file=sys.stderr)
        else:
            raise PublishError("usage: apify_worker.py publish <dir> <sha> | restore <build id>")
    except PublishError as error:
        print(f"X worker: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
