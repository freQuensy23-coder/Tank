from pathlib import Path

DOCKERFILE = Path(__file__).resolve().parents[1] / "vendor/openma/Dockerfile"


def stage(text, name):
    lines, current = {}, None
    for line in text.splitlines():
        if line.startswith("FROM "):
            current = line.split(" AS ")[-1].strip() if " AS " in line else None
            lines[current] = []
        elif current is not None:
            lines[current].append(line.strip())
    return lines[name]


def test_runtime_starts_without_pnpm_dependency_reinstall():
    """The install layer is cached on manifests while later layers copy fresher package.json files,
    so pnpm's pre-run dependency check would try `pnpm install` as the unprivileged runtime user
    and the server would exit at once; the runtime stage turns that check off."""
    text = DOCKERFILE.read_text()
    runtime = stage(text, "runtime")
    setting = "ENV pnpm_config_verify_deps_before_run=false"
    assert setting in runtime
    assert runtime.index(setting) < runtime.index('CMD ["pnpm", "start"]')
    assert "pnpm install --frozen-lockfile" in "\n".join(stage(text, "dependencies"))
    assert "COPY packages packages" in stage(text, "deps")
