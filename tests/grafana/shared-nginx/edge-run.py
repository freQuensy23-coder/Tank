import os
import secrets
import subprocess
import sys
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
HOST = "grafana.127-0-0-1.sslip.io"


def run(*args, check=True):
    return subprocess.run(args, check=check, text=True)


def main():
    endpoint = subprocess.check_output(
        ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], text=True
    ).strip()
    if not endpoint.startswith("unix://") or not os.environ.get("DOCKER_HOST", "unix://").startswith(
        "unix://"
    ):
        raise SystemExit("A local Docker daemon is required")
    name = "tank-edge-" + uuid.uuid4().hex[:12]
    image = name + "-image"
    env = os.environ.copy()
    for key in (
        "GRAFANA_ADMIN_PASSWORD",
        "GRAFANA_SECRET_KEY",
        "GRAFANA_STORE_PASSWORD",
        "GRAFANA_READER_PASSWORD",
        "TEST_VIEWER_PASSWORD",
        "TEST_POSTGRES_PASSWORD",
    ):
        env[key] = secrets.token_urlsafe(32)
    env.update(EDGE_IMAGE=image, GRAFANA_EXTERNAL_HOST=HOST)
    compose = [
        "docker",
        "compose",
        "--env-file",
        "/dev/null",
        "--project-directory",
        str(ROOT),
        "-p",
        name,
        "-f",
        str(ROOT / "compose.grafana.yaml"),
        "-f",
        str(ROOT / "deploy/grafana/nginx/compose.yaml"),
        "-f",
        str(HERE / "compose.yaml"),
    ]
    try:
        run("docker", "build", "-t", image, "-f", str(HERE / "Dockerfile"), str(HERE))
        subprocess.run([*compose, "up", "-d", "--wait"], check=True, env=env)
        edge = subprocess.check_output([*compose, "ps", "-q", "edge"], text=True, env=env).strip()
        run("docker", "cp", str(HERE / "edge-inner.py"), edge + ":/edge-inner.py")
        run("docker", "cp", str(ROOT / "deploy/grafana/nginx"), edge + ":/fixture-helper")
        run("docker", "exec", edge, "python3", "/edge-inner.py")
    finally:
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False, env=env)
        run("docker", "image", "rm", image, check=False)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode)
