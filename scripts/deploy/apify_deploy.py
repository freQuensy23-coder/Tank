import base64
import json
import os
import re
import shlex
import signal
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

PENDING = Path("worker.pending")
if PENDING.exists() and sys.argv[1] != "prepare":
    retained = json.loads(PENDING.read_text())
    ACTOR, TAG = retained["actor"], retained["tag"]
else:
    TARGETS = re.findall(
        r"https://api\.apify\.com/v2/actors/([A-Za-z0-9]+)/run-sync\?build=([^&\s]+)",
        Path("worker-source.yaml").read_text(),
    )
    if len(TARGETS) != 1:
        raise RuntimeError("selected source must identify exactly one worker")
    ACTOR = "/acts/" + TARGETS[0][0]
    TAG = TARGETS[0][1]
DEADLINE = min(
    time.monotonic() + (300 if sys.argv[1] == "prepare" else 20),
    float(os.environ.get("APIFY_PUBLICATION_DEADLINE", "inf"))
    if sys.argv[1] in {"prepare", "activate"}
    else float("inf"),
)


def timed_out(signum, frame):
    raise TimeoutError("publication wall deadline exceeded")


signal.signal(signal.SIGALRM, timed_out)
signal.setitimer(signal.ITIMER_REAL, max(0.001, DEADLINE - time.monotonic()))
TOKEN_LINES = [
    shlex.split(line.split("=", 1)[1], comments=True)
    for line in Path(".env").read_text().splitlines()
    if re.match(r"^(?:export\s+)?APIFY_API_TOKEN\s*=", line.strip())
]
if len(TOKEN_LINES) != 1 or len(TOKEN_LINES[0]) != 1:
    raise RuntimeError("missing or ambiguous APIFY_API_TOKEN in dotenv")
TOKEN = TOKEN_LINES[0][0]


def request(method, path, body=None):
    payload = None if body is None else json.dumps(body).encode()
    req = Request(
        "https://api.apify.com/v2" + path,
        data=payload,
        headers={
            "Authorization": "Bearer " + TOKEN,
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urlopen(req, timeout=max(0.001, min(15, DEADLINE - time.monotonic()))) as response:
        return json.load(response)["data"]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def current():
    actor = request("GET", ACTOR)
    require(actor["id"] == ACTOR.split("/")[-1], "actor identity mismatch")
    return actor["taggedBuilds"][TAG]["buildId"]


def snapshot(build_id):
    build = request("GET", "/actor-builds/" + build_id)
    require(
        build["id"] == build_id and build["actId"] == ACTOR.split("/")[-1] and build["status"] == "SUCCEEDED",
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
        version_number = json.loads(files[".actor/actor.json"])["version"]
        require(re.fullmatch(r"\d+\.\d+", version_number), "invalid worker version")
        version = {
            "versionNumber": version_number,
            "sourceType": "SOURCE_FILES",
            "buildTag": None,
            "envVars": [],
            "applyEnvVarsToBuild": False,
            "sourceFiles": [
                {"name": name, "format": "BASE64", "content": base64.b64encode(body).decode()}
                for name, body in files.items()
            ],
        }
        exists = any(item["versionNumber"] == version_number for item in actor["versions"])
        request(
            "PUT" if exists else "POST",
            ACTOR + ("/versions/" + version_number if exists else "/versions"),
            version,
        )
        build = request("POST", ACTOR + "/builds?version=" + version_number + "&tag=tank-" + sys.argv[2])
        candidate = build["id"]
        while build["status"] in {"READY", "RUNNING"} and time.monotonic() < DEADLINE:
            build = request("GET", "/actor-builds/" + candidate + "?waitForFinish=10")
        require(build["status"] == "SUCCEEDED", "worker build failed or timed out")
        require(snapshot(candidate) == files, "immutable build source mismatch")
    require(current() == previous, "worker tag changed during preparation")
    temporary = PENDING.with_suffix(".next")
    temporary.write_text(
        json.dumps(
            {
                "actor": ACTOR,
                "tag": TAG,
                "previous": previous,
                "candidate": candidate,
                "previous_release": Path("deployed").read_text().strip(),
                "previous_compose": "worker.previous-compose.yaml",
                "release": sys.argv[2],
                "committed": False,
            }
        )
    )
    temporary.replace(PENDING)


def tag(build_id):
    active = current()
    pending = json.loads(PENDING.read_text())
    require(active in {pending["previous"], pending["candidate"]}, "worker tag ownership lost")
    if active != build_id:
        try:
            request("PUT", ACTOR, {"taggedBuilds": {TAG: {"buildId": build_id}}})
        except TimeoutError:
            raise
        except Exception:
            pass
    require(current() == build_id, "worker tag verification failed")


def main():
    action = sys.argv[1]
    if action == "prepare":
        prepare()
        return
    if action == "rollback" and not PENDING.exists():
        return
    pending = json.loads(PENDING.read_text())
    if action == "previous-release":
        require(re.fullmatch(r"[0-9a-f]{40}", pending["previous_release"]), "invalid recovery release")
        print(pending["previous_release"])
    elif action == "previous-compose":
        require(
            pending.get("previous_compose") == "worker.previous-compose.yaml"
            and Path("worker.previous-compose.yaml").is_file(),
            "previous Compose input unavailable",
        )
    elif action == "settled":
        require(
            pending["committed"]
            and Path("deployed").read_text().strip() == pending["release"]
            and current() == pending["candidate"],
            "unfinished deployment",
        )
        PENDING.unlink()
    elif action == "activate":
        require(current() == pending["previous"], "worker changed before activation")
        tag(pending["candidate"])
    elif action == "rollback":
        tag(pending["previous"])
    elif action == "finish":
        require(current() == pending["candidate"], "worker changed before commit")
        pending["committed"] = True
        temporary = PENDING.with_suffix(".next")
        temporary.write_text(json.dumps(pending))
        temporary.replace(PENDING)
    elif action == "clear":
        PENDING.unlink(missing_ok=True)
    else:
        raise RuntimeError("unknown worker deployment action")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        print("Apify deployment failed: " + reason + "; transaction retained", file=sys.stderr)
        sys.exit(1)
