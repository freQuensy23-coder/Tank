import json
import subprocess
import uuid
from pathlib import Path

import pytest


@pytest.mark.integration
@pytest.mark.process
def test_backend_release_reconciliation(tmp_path):
    project = "tank-backend-" + uuid.uuid4().hex
    repo = "localhost/" + project
    services = ("postgres", "clickhouse", "openma")
    base, alias, revision, changed, missing, unlabeled = (c * 40 for c in "abcdef")
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    functions = source[source.index("grafana_enabled() {") : source.index("\nroll() {")]
    tags = []

    def run(*args):
        return subprocess.check_output(args, cwd=tmp_path, text=True, timeout=45).strip()

    def docker(*args):
        return run("docker", *args)

    def tag(image, ref):
        docker("tag", image, ref)
        tags.append(ref)

    def stage(sha, seconds="600", volume="slot", bind=False):
        stack = tmp_path / "releases" / sha / "stack"
        stack.mkdir(parents=True, exist_ok=True)
        (stack / "proof").write_text(sha)
        spec = {}
        for svc in services:
            worker = svc == "openma"
            image = "worker" if worker else "db"
            spec[svc] = {
                "image": f"{repo}/{image}:${{TANK_VERSION:?}}",
                "entrypoint": ["sleep"],
                "command": [seconds if worker else "600"],
                "network_mode": "none",
                "environment": {"MODE": "${FIXTURE_MODE:-blue}" if worker else "blue"},
                "volumes": (["data:/data"] if worker else [])
                + (["./proof:/proof:ro"] if worker and bind else []),
                "tmpfs": ["/var/lib/postgresql/data"],
                "stop_grace_period": "1s",
            }
        spec["openma"]["depends_on"] = ["postgres"]
        doc = {"name": project, "services": spec, "volumes": {"data": {"name": f"{project}-{volume}"}}}
        (stack / "compose.yaml").write_text(json.dumps(doc))

    def reconcile(sha, mode="blue", seconds="600", volume="slot"):
        (tmp_path / ".env").write_text(f"FIXTURE_MODE={mode}\n")
        run(
            "bash",
            "-euo",
            "pipefail",
            "-c",
            functions + '\nfor s in postgres clickhouse openma; do start_backend "$1" "$s"; done',
            "deploy",
            sha,
        )
        state = json.loads(docker("inspect", *(f"{project}-{s}-1" for s in services)))
        assert all(c["State"]["Running"] for c in state)
        worker = state[-1]
        assert docker("exec", worker["Id"], "printenv", "MODE") == mode
        assert worker["Config"]["Cmd"] == [seconds]
        assert any(
            m.get("Name") == f"{project}-{volume}" and m["Destination"] == "/data" for m in worker["Mounts"]
        )
        return [(c["Id"], c["State"]["StartedAt"], c["RestartCount"]) for c in state[:2]], worker

    seed = docker("create", "--network", "none", "postgres:16-bookworm")
    try:
        for sha, content in [
            (base, "1"),
            (revision, "1"),
            (changed, "2"),
            (missing, None),
            (unlabeled, None),
        ]:
            ref = f"{repo}/worker:{sha}"
            labels = ["--change", f"LABEL org.tank.content={content * 64}"] if content else []
            docker("commit", *labels, "--change", f"LABEL org.opencontainers.image.revision={sha}", seed, ref)
            tags.append(ref)
        tag(f"{repo}/worker:{base}", f"{repo}/worker:{alias}")
        for sha in (base, alias, revision, changed, missing, unlabeled):
            tag(f"{repo}/worker:{base}", f"{repo}/db:{sha}")
            stage(sha)
        db, first = reconcile(base)

        def roll(sha, **settings):
            current_db, worker = reconcile(sha, **settings)
            assert current_db == db
            return worker

        for sha in (alias, revision):
            assert roll(sha)["Id"] == first["Id"]
        docker("image", "rm", first["Config"]["Image"])
        assert roll(revision)["Id"] == first["Id"]
        env = roll(revision, mode="green")
        assert env["Id"] != first["Id"] and env["Image"] == first["Image"]
        stage(revision, seconds="601")
        cmd = roll(revision, mode="green", seconds="601")
        assert cmd["Id"] != env["Id"]
        stage(revision, seconds="601", volume="other")
        mount = roll(revision, mode="green", seconds="601", volume="other")
        assert mount["Id"] != cmd["Id"]
        assert roll(revision, mode="green", seconds="601", volume="other")["Id"] == mount["Id"]
        for sha in (alias, revision):
            stage(sha, bind=True)
            assert docker("exec", roll(sha)["Id"], "cat", "/proof") == sha
        replacement = roll(changed)
        assert replacement["Config"]["Image"] == f"{repo}/worker:{changed}"
        assert replacement["Image"] != first["Image"]
        restored = roll(base)
        assert restored["Image"] == first["Image"] and restored["Id"] != first["Id"]
        tag(replacement["Image"], restored["Config"]["Image"])
        assert roll(revision)["Config"]["Image"] == f"{repo}/worker:{revision}"
        assert (
            docker("image", "inspect", "-f", "{{.Id}}", restored["Config"]["Image"]) == replacement["Image"]
        )
        for sha in (missing, unlabeled):
            assert roll(sha)["Config"]["Image"] == f"{repo}/worker:{sha}"
    finally:
        for args, remove in [(("ps", "-aq"), ("rm", "-fv")), (("volume", "ls", "-q"), ("volume", "rm"))]:
            owned = docker(*args, "--filter", f"label=com.docker.compose.project={project}").splitlines()
            if owned:
                docker(*remove, *owned)
        docker("rm", "-v", seed)
        if tags:
            docker("image", "rm", *dict.fromkeys(tags))
