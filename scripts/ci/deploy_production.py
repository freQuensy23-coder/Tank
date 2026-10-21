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


COMPOSE = (
    "docker", "compose", "-p", "tank", "--project-directory", "/srv/tank",
    "-f", "/srv/tank/compose.yaml",
    "-f", "/etc/tank/compose.server.yaml",
    "-f", "/srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20/pg.resources.yaml",
    "--env-file", "/etc/tank/compose.env",
)


def compose_images(configuration):
    if configuration.get("name") != "tank":
        raise ValueError("Refusing a Compose project other than the existing production project tank")
    result = {}
    for service in ("tank", "tank-capture"):
        image = configuration.get("services", {}).get(service, {}).get("image")
        if not isinstance(image, str) or not image:
            raise ValueError(f"Production Compose service lacks its existing image: {service}")
        result[service] = image
    return result


def containers_healthy(states, images):
    for service in ("tank", "tank-capture"):
        container = states.get(service, {})
        state = container.get("State", {})
        if container.get("Image") != images[service][2] or not state.get("Running", False):
            return False
        health = state.get("Health", {}).get("Status")
        if service == "tank-capture" and health != "healthy":
            return False
        if service == "tank" and health not in (None, "healthy"):
            return False
    return True


os.chdir("/srv/tank")
sha = request["sha"]
if command("git", "rev-parse", "HEAD") != sha:
    raise SystemExit("Production checkout must already match the qualified commit; synchronize it before deployment")
if command("git", "status", "--porcelain", "--untracked-files=no"):
    raise SystemExit("Production checkout has tracked changes; refusing deployment")
configuration = json.loads(command(*COMPOSE, "config", "--format", "json"))
existing_images = compose_images(configuration)
services = ("tank", "tank-capture")
images = {}
previous = {}
with tempfile.TemporaryDirectory(prefix="tank-registry-") as registry:
    os.environ["DOCKER_CONFIG"] = registry
    try:
        subprocess.run(["docker", "login", "ghcr.io", "--username", request["user"], "--password-stdin"], input=request["token"], text=True, check=True, stdout=subprocess.DEVNULL)
        for service in services:
            tag = existing_images[service]
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
            command(*COMPOSE, "up", "--detach", "--no-build", "--no-deps", *services)
            deadline = time.monotonic() + 180
            while True:
                states = {}
                for service in services:
                    container = command(*COMPOSE, "ps", "--all", "--quiet", service)
                    if not container or "\n" in container:
                        continue
                    states[service] = json.loads(command("docker", "inspect", container))[0]
                if containers_healthy(states, images):
                    print(json.dumps({"deployed_sha": sha, "services": list(services)}))
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("Qualified containers did not become healthy within 180 seconds")
                time.sleep(3)
        except BaseException:
            for service, old_image in previous.items():
                command("docker", "tag", old_image, images[service][1])
            command(*COMPOSE, "up", "--detach", "--no-build", "--no-deps", *services)
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
