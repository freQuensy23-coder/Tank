"""Guarded stable skill alias stage; no service, VM, model or source mutation."""

import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
TARGET = "packages/control-plane-node/src/lib/node-managed-session-inputs.ts"
INPUT_SHA256 = "265f4c3ba423a31b5286f5a5035c09efd8520aff8b6429f2c539e53fe87b5156"
ASSETS = {
    "stable-skill-paths.ts": "node-managed-stable-skills.ts",
    "claim-skill-paths.py": "node-managed-skill-aliases.py",
}


def apply(root):
    root = Path(root).resolve()
    path = root / TARGET
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError("Stable skill materializer target is not a plain scoped file")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != INPUT_SHA256:
        raise ValueError("Stable skill materializer source guard mismatch")
    manifest = json.loads((BASE / "manifest.json").read_text())
    for name, expected in manifest["assets_sha256"].items():
        if hashlib.sha256((BASE / name).read_bytes()).hexdigest() != expected:
            raise ValueError("Stable skill materializer asset integrity mismatch")
    for target in ASSETS.values():
        if path.with_name(target).exists() or path.with_name(target).is_symlink():
            raise ValueError("Stable skill materializer asset already exists")
    source = raw.decode()
    marker = "    for (const skill of input.session.agent.skills) {"
    if source.count(marker) != 1:
        raise ValueError("Stable skill materializer anchor mismatch")
    start = source.index(marker)
    end = source.index("\n  }\n\n  private async materializeRepository(", start)
    source = (
        source[:start]
        + """    const stableSkills: StableSkillPlan[] = [];
    for (const skill of input.session.agent.skills) {
      if (skill.type !== "custom") continue;
      const requestedVersion = skill.version ?? "latest";
      const resolved = await resolveConcreteSkillVersion(
        this.dependencies.skillVersions, skill.skillId, requestedVersion,
      );
      if (!resolved) throw new Error(`Managed Skill ${skill.skillId} has no version`);
      const downloaded = await this.dependencies.skillVersions.downloadSkillVersion({
        skillId: skill.skillId, version: resolved.version,
      });
      if (downloaded.type !== "found") {
        throw new Error(`Managed Skill ${skill.skillId}@${resolved.version} was not found`);
      }
      stableSkills.push(stableSkillPlan({
        skillId: skill.skillId, version: resolved.version, requestedVersion,
        name: resolved.name, archive: downloaded.file.content,
      }));
    }
    await materializeStableSkills(input.sandbox, stableSkills, assertCommandSucceeded);"""
        + source[end:]
    )
    anchor = 'import { skillArchiveManifest } from "./node-managed-skill-manifest.js";'
    if source.count(anchor) != 1:
        raise ValueError("Stable skill resolver import anchor mismatch")
    source = source.replace(
        anchor,
        anchor
        + '\nimport { stableSkillPlan, materializeStableSkills, type StableSkillPlan } from "./node-managed-stable-skills.js";',
        1,
    )
    if hashlib.sha256(source.encode()).hexdigest() != manifest["patched_sha256"]:
        raise ValueError("Stable skill materializer output integrity mismatch")
    path.write_text(source)
    for asset, target in ASSETS.items():
        path.with_name(target).write_bytes((BASE / asset).read_bytes())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("prepared_source", type=Path)
    apply(parser.parse_args().prepared_source)
