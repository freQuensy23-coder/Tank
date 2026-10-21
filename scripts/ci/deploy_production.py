"""Deploy already qualified Tank and Go capture images from an approved production branch."""

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

REMOTE = r"""
import json
import os
import subprocess
import tempfile
import time


def command(*args, capture=True):
    return subprocess.run(args, check=True, text=True, capture_output=capture).stdout.strip() if capture else subprocess.run(args, check=True)


os.chdir("/srv/tank")
sha = request["sha"]
if command("git", "rev-parse", "HEAD") != sha:
    raise SystemExit("Production checkout must already match the qualified commit; synchronize it before deployment")
if command("git", "status", "--porcelain", "--untracked-files=no"):
    raise SystemExit("Production checkout has tracked changes; refusing deployment")
configuration = json.loads(command("docker", "compose", "config", "--format", "json"))
services = ("tank", "tank-capture")
images = {}
previous = {}
with tempfile.TemporaryDirectory(prefix="tank-registry-") as registry:
    os.environ["DOCKER_CONFIG"] = registry
    try:
        subprocess.run(["docker", "login", "ghcr.io", "--username", request["user"], "--password-stdin"], input=request["token"], text=True, check=True, stdout=subprocess.DEVNULL)
        for service in services:
            tag = configuration["services"][service]["image"]
            image = f"ghcr.io/{request['repository']}/{service}:{sha}"
            command("docker", "pull", image)
            description = json.loads(command("docker", "image", "inspect", image))[0]
            if description.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.revision") != sha:
                raise SystemExit(f"Image revision does not match the qualified commit: {service}")
            previous[service] = json.loads(command("docker", "image", "inspect", tag))[0]["Id"]
            images[service] = (image, tag, description["Id"])
        for image, tag, _ in images.values():
            command("docker", "tag", image, tag)
        try:
            command("docker", "compose", "up", "--detach", "--no-build", "--no-deps", *services)
            deadline = time.monotonic() + 180
            while True:
                healthy = True
                for service in services:
                    container = command("docker", "compose", "ps", "--all", "--quiet", service)
                    if not container or "\n" in container:
                        healthy = False
                        continue
                    state = json.loads(command("docker", "inspect", container))[0]
                    expected = images[service][2]
                    health = state["State"].get("Health", {}).get("Status")
                    healthy &= state["Image"] == expected and state["State"].get("Running", False)
                    healthy &= health == "healthy" if service == "tank-capture" else health in (None, "healthy")
                if healthy:
                    print(json.dumps({"deployed_sha": sha, "services": list(services)}))
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("Qualified containers did not become healthy within 180 seconds")
                time.sleep(3)
        except BaseException:
            for service, old_image in previous.items():
                command("docker", "tag", old_image, images[service][1])
            command("docker", "compose", "up", "--detach", "--no-build", "--no-deps", *services)
            raise
    finally:
        command("docker", "logout", "ghcr.io")
"""


def qualified_sha(value: str, default: str) -> str:
    sha = value or default
    if re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        raise ValueError("Deployment requires a full lowercase 40-character commit SHA")
    return sha


def target(user: str, host: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user) is None:
        raise ValueError("Invalid production SSH user")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host) is None:
        raise ValueError("Invalid production SSH host")
    return f"{user}@{host}"


def main() -> None:
    branch = os.environ["GITHUB_REF_NAME"]
    if branch not in {"main", "prod-main"}:
        raise SystemExit("Deploy this workflow only from main or prod-main")
    sha = qualified_sha(os.environ.get("DEPLOY_SHA", ""), os.environ["GITHUB_SHA"])
    subprocess.run(["git", "merge-base", "--is-ancestor", sha, f"origin/{branch}"], check=True)
    repository = os.environ["GITHUB_REPOSITORY"].lower()
    if re.fullmatch(r"[a-z0-9_.-]+/[a-z0-9_.-]+", repository) is None:
        raise ValueError("Invalid image repository")
    destination = target(os.environ["PROD_SSH_USER"], os.environ["PROD_SSH_HOST"])
    request = {
        "sha": sha,
        "repository": repository,
        "user": repository.split("/")[0],
        "token": os.environ["GHCR_TOKEN"],
    }
    payload = f"request = __import__('json').loads({json.dumps(request)!r})\n" + REMOTE
    with tempfile.TemporaryDirectory(prefix="tank-deploy-") as temporary:
        directory = Path(temporary)
        key = directory / "key"
        hosts = directory / "known_hosts"
        key.write_text(os.environ["PROD_SSH_KEY"].rstrip() + "\n")
        key.chmod(0o600)
        hosts.write_text(os.environ["PROD_SSH_KNOWN_HOSTS"].rstrip() + "\n")
        hosts.chmod(0o600)
        subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                f"UserKnownHostsFile={hosts}",
                "-i",
                str(key),
                destination,
                "python3",
                "-",
            ],
            input=payload,
            text=True,
            check=True,
        )


if __name__ == "__main__":
    main()
