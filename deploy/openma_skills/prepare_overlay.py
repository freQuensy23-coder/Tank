"""Prepare-only guarded fix; does not modify deployment until explicitly called.

The shared prepare_openma_source owner can invoke apply(root) after review.
"""

import hashlib
import shutil
from pathlib import Path

TARGET = "packages/control-plane-node/src/lib/node-managed-session-inputs.ts"
ORIGINAL_SHA256 = "93bb57eb4deb6681a81e00dc887f5ca4efdeb8214fd8a8a678f313c03939127b"


def apply(root):
    target = Path(root) / TARGET
    source = target.read_text()
    if hashlib.sha256(target.read_bytes()).hexdigest() != ORIGINAL_SHA256:
        raise ValueError("Managed skill resolver source hash mismatch")
    start = source.index("function skillMdBodyFromArchive(")
    end = source.index("\nasync function resolveConcreteSkillVersion(", start)
    source = source[:start] + source[end + 1 :]
    source = source.replace(
        'import { unzipSync } from "fflate";',
        'import { unzipSync } from "fflate";\nimport { skillArchiveManifest } from "./node-managed-skill-manifest.js";',
        1,
    )
    old = """    let body: string | undefined;
    if (fetchBodies) {
      const downloaded = await skillVersions.downloadSkillVersion({
        skillId: skill.skillId,
        version: resolved.version,
      });
      if (downloaded.type === "found") {
        body = skillMdBodyFromArchive(downloaded.file.content);
      }
    }
"""
    new = """    // Progressive exposure also needs the archive: its manifest may live
    // beneath a compiler-added directory. Materialization preserves that path.
    const downloaded = await skillVersions.downloadSkillVersion({
      skillId: skill.skillId,
      version: resolved.version,
    });
    if (downloaded.type !== "found") {
      throw new Error(`Managed Skill ${skill.skillId}@${resolved.version} archive was not found`);
    }
    const manifest = skillArchiveManifest(unzipSync(downloaded.file.content));
    const manifestRoot = `${mountRoot}${manifest.directory}`;
    const body = fetchBodies ? manifest.body : undefined;
"""
    if source.count(old) != 1:
        raise ValueError("Managed skill resolver body anchor mismatch")
    source = source.replace(old, new, 1)
    old = """      description: resolved.description || `Custom skill mounted at ${mountRoot}`,
      mountRoot,
"""
    new = """      description: resolved.description || `Custom skill mounted at ${manifestRoot}`,
      mountRoot: manifestRoot,
"""
    if source.count(old) != 1:
        raise ValueError("Managed skill descriptor anchor mismatch")
    source = source.replace(old, new, 1)
    target.write_text(source)
    shutil.copyfile(
        Path(__file__).with_name("archive-manifest.ts"), target.with_name("node-managed-skill-manifest.ts")
    )
    tests = Path(root) / "apps/main-node/test"
    tests.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        Path(__file__).with_name("archive-manifest.test.ts"), tests / "tank-skill-archive.test.ts"
    )
    (tests / "fixtures").mkdir(exist_ok=True)
    shutil.copyfile(
        Path(__file__).with_name("registered-tank-data.zip"), tests / "fixtures/tank-registered-skill.zip"
    )


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("root")
    args = p.parse_args()
    apply(args.root)
