import argparse
import base64
import hashlib
import json
import os
import re
import stat
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath

ACTOR_ID = "rbf5CQKQgKNVl4SfO"
BASELINE_BUILD = "c99YRMlKXube5V0gV"
CONTRACT = "tank-x-reader-v1"
MARKER = "tank-release.json"
VERSION = "0.3"


class DeployError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise DeployError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(files):
    return hashlib.sha256(
        canonical({name: hashlib.sha256(body).hexdigest() for name, body in files.items()})
    ).hexdigest()


def source_files(root):
    files = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "worker source contains symlink")
        if path.is_file():
            files[path.relative_to(root).as_posix()] = path.read_bytes()
    require(
        {
            "main.py",
            "direct_fetch.py",
            "public_profile.py",
            "requirements.txt",
            "Dockerfile",
            ".actor/actor.json",
        }
        <= files.keys(),
        "worker source incomplete",
    )
    require(MARKER not in files, "reserved release marker in worker source")
    require(
        "buildTag" not in json.loads(files[".actor/actor.json"]),
        "worker definition must not activate a build tag",
    )
    return files


def package(sha, destination):
    require(re.fullmatch(r"[0-9a-f]{40}", sha), "invalid release SHA")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    require(head == sha, "selected release is not checked out")
    names = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", sha, "apify/x-reader"], text=True
    ).splitlines()
    require(names, "worker source not tracked")
    files = {}
    for name in names:
        mode = subprocess.check_output(["git", "ls-tree", sha, name], text=True).split()[0]
        require(mode == "100644", "worker source must contain regular non-executable files")
        files[name.removeprefix("apify/x-reader/")] = subprocess.check_output(
            ["git", "show", f"{sha}:{name}"]
        )
    destination.mkdir(parents=True, exist_ok=False)
    worker = destination / "worker"
    for name, body in files.items():
        path = worker / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    files = source_files(worker)
    (destination / "manifest.json").write_bytes(
        canonical(
            {"release_sha": sha, "source_digest": digest(files), "contract": CONTRACT, "actor_id": ACTOR_ID}
        )
    )
    publisher = subprocess.check_output(["git", "show", f"{sha}:scripts/deploy/apify_deploy.py"])
    (destination / "apify_deploy.py").write_bytes(publisher)


def private_path(path, directory=False):
    info = path.lstat()
    require(info.st_uid == os.geteuid(), "deployment path has wrong owner")
    require(
        stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode),
        "deployment path has wrong type",
    )
    require(
        stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600),
        "deployment path has unsafe permissions",
    )


def host_token(path):
    private_path(path)
    values = []
    for line in path.read_text().splitlines():
        match = re.fullmatch(r"\s*(?:export\s+)?APIFY_API_TOKEN\s*=\s*(.*?)\s*", line)
        if match:
            value = match[1]
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            require(re.fullmatch(r"[A-Za-z0-9_-]+", value), "unsupported APIFY_API_TOKEN value")
            values.append(value)
    require(len(values) == 1, "host .env must define APIFY_API_TOKEN exactly once")
    return values[0]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Api:
    def __init__(self, token, base="https://api.apify.com/v2"):
        self.token = token
        self.base = base
        self.opener = urllib.request.build_opener(NoRedirect, urllib.request.ProxyHandler({}))

    def request(self, method, path, body=None):
        request = urllib.request.Request(
            self.base + path,
            data=canonical(body) if body is not None else None,
            method=method,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
        )
        try:
            with self.opener.open(request, timeout=75) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            raise DeployError(f"Apify {method} failed with HTTP {error.code}") from None
        except (OSError, ValueError):
            raise DeployError(f"Apify {method} transport or JSON failure") from None
        require(
            isinstance(value, dict) and isinstance(value.get("data"), dict), "invalid Apify response envelope"
        )
        return value["data"]


def decoded_files(entries):
    files = {}
    for entry in entries:
        if entry.get("folder"):
            continue
        name = entry["name"]
        require(
            name not in files
            and not PurePosixPath(name).is_absolute()
            and ".." not in PurePosixPath(name).parts,
            "invalid build source path",
        )
        require(entry.get("format") in {"TEXT", "BASE64"}, "unknown build source format")
        files[name] = (
            base64.b64decode(entry["content"], validate=True)
            if entry["format"] == "BASE64"
            else entry["content"].encode()
        )
    return files


class Publisher:
    def __init__(self, api, release, state, sha):
        self.api = api
        self.release = release
        self.state = state
        self.sha = sha
        self.clock = time.monotonic
        self.actor = "/actors/" + ACTOR_ID
        state.mkdir(mode=0o700, exist_ok=True)
        private_path(state, directory=True)

    def read(self, name):
        path = self.state / name
        if not path.exists():
            return None
        private_path(path)
        return json.loads(path.read_bytes())

    def write(self, name, value):
        path = self.state / name
        temporary = path.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(canonical(value))
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def current(self):
        actor = self.api.request("GET", self.actor)
        require(actor.get("id") == ACTOR_ID, "actor identity mismatch")
        build_id = actor.get("taggedBuilds", {}).get("direct", {}).get("buildId")
        require(
            isinstance(build_id, str) and re.fullmatch(r"[A-Za-z0-9]{17}", build_id), "missing direct build"
        )
        return build_id

    def build(self, build_id):
        require(isinstance(build_id, str) and re.fullmatch(r"[A-Za-z0-9]{17}", build_id), "invalid build ID")
        build = self.api.request("GET", "/actor-builds/" + build_id)
        require(
            build.get("id") == build_id
            and build.get("actId") == ACTOR_ID
            and build.get("status") == "SUCCEEDED",
            "build identity or status mismatch",
        )
        return build

    def provenance(self, build_id, expected_digest=None):
        build = self.build(build_id)
        version = build.get("actVersion", {})
        require(version.get("sourceType") == "SOURCE_FILES", "build source snapshot unavailable")
        files = decoded_files(version.get("sourceFiles", []))
        require(MARKER in files, "build release provenance missing")
        marker = json.loads(files.pop(MARKER))
        require(
            marker.get("actor_id") == ACTOR_ID and marker.get("contract") == CONTRACT,
            "incompatible worker contract",
        )
        require(re.fullmatch(r"[0-9a-f]{40}", marker.get("release_sha", "")), "build release SHA missing")
        require(digest(files) == marker.get("source_digest"), "build source digest mismatch")
        require(
            expected_digest is None or marker["source_digest"] == expected_digest,
            "build does not match release source",
        )
        return {**marker, "build_id": build_id}

    def prepare(self):
        require(self.read("pending.json") is None, "unfinished worker deployment requires recovery")
        deployed = self.state.parent / "deployed"
        previous_release = deployed.read_text().strip() if deployed.exists() else ""
        require(
            not previous_release or re.fullmatch(r"[0-9a-f]{40}", previous_release),
            "invalid previous Tank release",
        )
        manifest = json.loads((self.release / "manifest.json").read_bytes())
        files = source_files(self.release / "worker")
        require(
            manifest
            == {
                "release_sha": self.sha,
                "source_digest": digest(files),
                "contract": CONTRACT,
                "actor_id": ACTOR_ID,
            },
            "release artifact provenance mismatch",
        )
        previous = self.current()
        active = self.read("active.json")
        if active is None:
            require(previous == BASELINE_BUILD, "unknown legacy direct build")
            self.build(previous)
        else:
            require(active["build_id"] == previous, "active worker changed outside deployment")
            require(active["release_sha"] == previous_release, "Tank and worker release markers disagree")
            require(self.provenance(previous) == active["build"], "active worker provenance mismatch")
        cached = self.read(manifest["source_digest"] + ".json")
        if cached is not None:
            candidate = self.provenance(cached["build_id"], manifest["source_digest"])
            require(candidate == cached, "cached build provenance mismatch")
        else:
            if active is not None and active["build"]["source_digest"] == manifest["source_digest"]:
                candidate = self.provenance(previous, manifest["source_digest"])
            else:
                uploaded = dict(files)
                uploaded[MARKER] = canonical(manifest)
                version = {
                    "versionNumber": VERSION,
                    "sourceType": "SOURCE_FILES",
                    "buildTag": None,
                    "envVars": [],
                    "applyEnvVarsToBuild": False,
                    "sourceFiles": [
                        {"name": name, "format": "BASE64", "content": base64.b64encode(body).decode()}
                        for name, body in sorted(uploaded.items())
                    ],
                }
                actor = self.api.request("GET", self.actor)
                exists = any(item["versionNumber"] == VERSION for item in actor.get("versions", []))
                self.api.request(
                    "PUT" if exists else "POST",
                    self.actor + ("/versions/" + VERSION if exists else "/versions"),
                    version,
                )
                actual = self.api.request("GET", self.actor + "/versions/" + VERSION)
                require(
                    actual.get("sourceType") == "SOURCE_FILES"
                    and actual.get("buildTag") is None
                    and actual.get("envVars") == []
                    and actual.get("applyEnvVarsToBuild") is False
                    and decoded_files(actual.get("sourceFiles", [])) == uploaded,
                    "uploaded version verification failed",
                )
                build = self.api.request(
                    "POST", self.actor + f"/builds?version={VERSION}&tag=tank-{self.sha}"
                )
                build_id = build.get("id")
                require(
                    isinstance(build_id, str) and re.fullmatch(r"[A-Za-z0-9]{17}", build_id),
                    "build ID missing",
                )
                deadline = self.clock() + 600
                while build.get("status") in {"READY", "RUNNING"} and self.clock() < deadline:
                    build = self.api.request("GET", "/actor-builds/" + build_id + "?waitForFinish=60")
                require(build.get("status") == "SUCCEEDED", "worker build failed or exceeded deadline")
                candidate = self.provenance(build_id, manifest["source_digest"])
                require(candidate["release_sha"] == self.sha, "new build release SHA mismatch")
            self.write(manifest["source_digest"] + ".json", candidate)
        require(self.current() == previous, "direct tag changed while publishing")
        self.write(
            "pending.json",
            {
                "release_sha": self.sha,
                "previous": previous,
                "previous_active": active,
                "previous_release_sha": previous_release,
                "candidate": candidate,
                "activation_attempted": False,
            },
        )
        print(
            f"worker prepared release={self.sha} digest={manifest['source_digest']} build={candidate['build_id']}"
        )

    def pending(self):
        transaction = self.read("pending.json")
        if transaction is None or transaction["release_sha"] != self.sha:
            raise DeployError("worker transaction missing or release mismatch")
        return transaction

    def tag(self, build_id):
        self.api.request("PUT", self.actor, {"taggedBuilds": {"direct": {"buildId": build_id}}})
        require(self.current() == build_id, "direct tag activation verification failed")

    def activate(self):
        transaction = self.pending()
        candidate = transaction["candidate"]
        require(self.current() == transaction["previous"], "direct tag changed before activation")
        require(self.provenance(candidate["build_id"]) == candidate, "candidate provenance changed")
        transaction["activation_attempted"] = True
        self.write("pending.json", transaction)
        if candidate["build_id"] != transaction["previous"]:
            self.tag(candidate["build_id"])

    def commit(self):
        transaction = self.pending()
        candidate = transaction["candidate"]
        require(
            transaction["activation_attempted"] and self.current() == candidate["build_id"],
            "worker activation not verified",
        )
        require(
            self.provenance(candidate["build_id"]) == candidate, "worker provenance changed before commit"
        )
        self.write(
            "active.json", {"release_sha": self.sha, "build_id": candidate["build_id"], "build": candidate}
        )
        print(f"worker committed release={self.sha} build={candidate['build_id']}")

    def finish(self):
        transaction = self.pending()
        require(
            (self.state.parent / "deployed").read_text().strip() == self.sha,
            "Tank release marker not committed",
        )
        require(
            self.read("active.json")
            == {
                "release_sha": self.sha,
                "build_id": transaction["candidate"]["build_id"],
                "build": transaction["candidate"],
            },
            "worker release marker not committed",
        )
        require(
            self.current() == transaction["candidate"]["build_id"], "worker tag changed before completion"
        )
        (self.state / "pending.json").unlink()

    def rollback(self):
        if self.read("pending.json") is None:
            return
        transaction = self.pending()
        previous = transaction["previous"]
        if transaction["previous_active"] is None:
            require(previous == BASELINE_BUILD, "invalid bootstrap rollback build")
            self.build(previous)
        else:
            require(
                self.provenance(previous) == transaction["previous_active"]["build"],
                "rollback provenance mismatch",
            )
        if transaction["activation_attempted"]:
            current = self.current()
            require(
                current in {previous, transaction["candidate"]["build_id"]},
                "refusing to overwrite unrelated direct tag",
            )
            if current != previous:
                self.tag(previous)
            require(self.current() == previous, "worker rollback verification failed")
        if transaction["previous_active"] is None:
            (self.state / "active.json").unlink(missing_ok=True)
        else:
            self.write("active.json", transaction["previous_active"])
        transaction["rolled_back"] = True
        self.write("pending.json", transaction)
        print(f"worker restored build={previous}")

    def previous_release(self):
        print(self.pending()["previous_release_sha"])

    def rollback_finish(self):
        if self.read("pending.json") is None:
            return
        transaction = self.pending()
        require(
            transaction.get("rolled_back") and self.current() == transaction["previous"],
            "worker rollback incomplete",
        )
        require(self.read("active.json") == transaction["previous_active"], "worker rollback marker mismatch")
        deployed = self.state.parent / "deployed"
        previous_release = deployed.read_text().strip() if deployed.exists() else ""
        require(previous_release == transaction["previous_release_sha"], "Tank rollback marker mismatch")
        (self.state / "pending.json").unlink()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=[
            "package",
            "prepare",
            "activate",
            "commit",
            "finish",
            "rollback",
            "rollback_finish",
            "previous_release",
        ],
    )
    parser.add_argument("sha")
    parser.add_argument("directory", type=Path)
    arguments = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", arguments.sha), "invalid release SHA")
    if arguments.action == "package":
        package(arguments.sha, arguments.directory)
        return
    require(os.geteuid() == 0, "production publisher must run as root")
    publisher = Publisher(
        Api(host_token(Path(".env"))), arguments.directory, Path("apify-state"), arguments.sha
    )
    getattr(publisher, arguments.action)()


if __name__ == "__main__":
    try:
        main()
    except DeployError as error:
        raise SystemExit(str(error)) from None
    except (KeyError, TypeError, ValueError, OSError):
        raise SystemExit("Apify deployment failed; no unverified worker release accepted") from None
