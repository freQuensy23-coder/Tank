import os
import subprocess
import sys
from pathlib import Path

import yaml

TRANSPORT = r"""
ssh() {
    local command=${!#}
    [[ ${*: -2:1} == fixture@fixture.invalid ]] || return 1
    case "$command" in
        "/srv/tank-deploy/deploy.sh $SHA") printf '%s\n' "$SHA" > "$HOST/dispatch" ;;
        *) "$ENGINE" run --rm -i --pull=never --network none --read-only --cap-drop=ALL \
            --security-opt no-new-privileges --user "$(id -u):$(id -g)" \
            --label "com.docker.compose.project=$PROJECT" --mount "type=bind,source=$HOST,target=/srv/tank-deploy" \
            --entrypoint /bin/bash "$IMAGE" -eu -o pipefail -c "$command" ;;
    esac
}
scp() {
    local dest=${!#}
    [[ $dest == fixture@fixture.invalid:/srv/tank-deploy/ ]] || return 1
    while [[ $1 == -o || $1 == -i ]]; do shift 2; done
    cp -- "${@:1:$#-1}" "$HOST/"
}
"""


def stage(root, host, scratch):
    sha = os.environ["RELEASE_SHA"]
    workflow = yaml.safe_load(
        subprocess.check_output(["git", "show", f"{sha}:.github/workflows/deploy.yml"], cwd=root)
    )
    steps = workflow["jobs"]["deploy"]["steps"]
    select = next(step["run"] for step in steps if step.get("id") == "target")
    deploy = next(step["run"] for step in steps if "SSH_HOST" in step.get("env", {}))
    source = scratch / "checkout"
    subprocess.run(["git", "clone", "-q", "--shared", "--no-checkout", root, source], check=True)

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=source, text=True).strip()

    git(
        "sparse-checkout",
        "set",
        ".github",
        "apify/x-reader",
        "deploy/grafana",
        "deploy/postgres",
        "scripts/deploy",
    )
    git("checkout", "-q", "--detach", sha)
    git("update-ref", "refs/remotes/origin/main", sha)
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(scratch / "home"),
        "RUNNER_TEMP": str(scratch / "runner"),
        "GITHUB_OUTPUT": str(scratch / "output"),
        "SHA": sha,
        "INPUT_SHA": sha,
        "SSH_KEY": "fixture",
        "SSH_KNOWN_HOSTS": "fixture",
        "SSH_USER": "fixture",
        "SSH_HOST": "fixture.invalid",
        "HOST": str(host),
        "ENGINE": os.environ["ENGINE"],
        "IMAGE": os.environ["IMAGE"],
        "PROJECT": os.environ["PROJECT"],
        "DOCKER_CONTEXT": os.environ.get("DOCKER_CONTEXT", "default"),
        "DOCKER_CONFIG": str(Path.home() / ".docker"),
    }
    (host / "dispatch").unlink(missing_ok=True)
    for script in (select, TRANSPORT + deploy):
        subprocess.run(
            ["bash", "-eu", "-o", "pipefail", "-c", script], env=env, cwd=source, check=True, timeout=60
        )
    assert (host / "dispatch").read_text().strip() == sha
    return sha


if __name__ == "__main__":
    print(stage(Path(__file__).resolve().parents[2], Path(sys.argv[1]), Path(sys.argv[2])))
