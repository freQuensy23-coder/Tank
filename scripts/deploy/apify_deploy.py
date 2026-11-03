import base64
import json
import os
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

ACTOR = "/acts/rbf5CQKQgKNVl4SfO"
PENDING = Path("worker.pending")


def request(method, path, body=None):
    payload = None if body is None else json.dumps(body).encode()
    req = Request(
        "https://api.apify.com/v2" + path,
        data=payload,
        headers={
            "Authorization": "Bearer " + os.environ["APIFY_API_TOKEN"],
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urlopen(req, timeout=70) as response:
        return json.load(response)["data"]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def current():
    actor = request("GET", ACTOR)
    require(actor["id"] == ACTOR.split("/")[-1], "actor identity mismatch")
    return actor["taggedBuilds"]["direct"]["buildId"]


def snapshot(build_id):
    build = request("GET", "/actor-builds/" + build_id)
    require(
        build["id"] == build_id
        and build["actId"] == ACTOR.split("/")[-1]
        and build["status"] == "SUCCEEDED",
        "build identity or status mismatch",
    )
    version = build["actVersion"]
    require(version["sourceType"] == "SOURCE_FILES", "immutable source snapshot unavailable")
    files = {}
    for item in version["sourceFiles"]:
        require(item["name"] not in files, "duplicate build source")
        require(item["format"] in {"TEXT", "BASE64"}, "unknown source encoding")
        files[item["name"]] = (
            base64.b64decode(item["content"], validate=True)
            if item["format"] == "BASE64"
            else item["content"].encode()
        )
    return files


def prepare():
    require(not PENDING.exists(), "unfinished worker deployment requires recovery")
    root = Path("worker")
    require(root.is_dir(), "selected revision has no versioned worker")
    paths = sorted(path for path in root.rglob("*") if path.is_file())
    require(paths and not any(path.is_symlink() for path in root.rglob("*")), "invalid worker source")
    files = {path.relative_to(root).as_posix(): path.read_bytes() for path in paths}
    previous = current()
    candidate = previous
    if snapshot(previous) != files:
        actor = request("GET", ACTOR)
        version = {
            "versionNumber": "0.3",
            "sourceType": "SOURCE_FILES",
            "buildTag": None,
            "envVars": [],
            "applyEnvVarsToBuild": False,
            "sourceFiles": [
                {"name": name, "format": "BASE64", "content": base64.b64encode(body).decode()}
                for name, body in files.items()
            ],
        }
        exists = any(item["versionNumber"] == "0.3" for item in actor["versions"])
        request("PUT" if exists else "POST", ACTOR + ("/versions/0.3" if exists else "/versions"), version)
        build = request("POST", ACTOR + "/builds?version=0.3&tag=tank-" + sys.argv[2])
        candidate = build["id"]
        deadline = time.monotonic() + 600
        while build["status"] in {"READY", "RUNNING"} and time.monotonic() < deadline:
            build = request("GET", "/actor-builds/" + candidate + "?waitForFinish=60")
        require(build["status"] == "SUCCEEDED", "worker build failed or timed out")
        require(snapshot(candidate) == files, "immutable build source mismatch")
    require(current() == previous, "worker tag changed during preparation")
    temporary = PENDING.with_suffix(".next")
    temporary.write_text(json.dumps({"previous": previous, "candidate": candidate}))
    temporary.replace(PENDING)


def tag(build_id):
    if current() != build_id:
        request("PUT", ACTOR, {"taggedBuilds": {"direct": {"buildId": build_id}}})
    require(current() == build_id, "worker tag verification failed")


def main():
    action = sys.argv[1]
    if action == "prepare":
        prepare()
        return
    if action == "rollback" and not PENDING.exists():
        return
    pending = json.loads(PENDING.read_text())
    if action == "activate":
        require(current() == pending["previous"], "worker changed before activation")
        tag(pending["candidate"])
    elif action == "rollback":
        tag(pending["previous"])
    elif action == "finish":
        require(current() == pending["candidate"], "worker changed before commit")
        PENDING.unlink()
    elif action == "clear":
        PENDING.unlink(missing_ok=True)
    else:
        raise RuntimeError("unknown worker deployment action")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Apify deployment failed; transaction retained for recovery", file=sys.stderr)
        sys.exit(1)
