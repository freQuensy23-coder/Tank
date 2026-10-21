"""Verify bundles restored from a backup into a separate volume against its manifest."""
import hashlib
import json
import sys
from pathlib import Path

import zstandard

backup, restored = Path(sys.argv[1]), Path(sys.argv[2])
manifest = json.loads((backup / "manifest.json").read_text())
rows = []
for path in sorted(restored.glob("*.json.zst")):
    file_hash, body_hash = hashlib.sha256(), hashlib.sha256()
    with path.open("rb") as handle:
        file_hash.update(handle.read())
        handle.seek(0)
        with zstandard.ZstdDecompressor().stream_reader(handle) as reader:
            while chunk := reader.read(1 << 20):
                body_hash.update(chunk)
    name = path.name.removesuffix(".json.zst")
    file_sha = file_hash.hexdigest()
    rows.append(
        {
            "file": path.name,
            "bytes": path.stat().st_size,
            "manifest_sha256_match": manifest["files"].get("bundles/" + path.name) == file_sha,
            "name_is_sha256": name in {file_sha, body_hash.hexdigest()},
        }
    )
listed = sorted(k for k in manifest["files"] if k.startswith("bundles/"))
summary = {
    "manifest_bundles": len(listed),
    "restored_bundles": len(rows),
    "all_match_manifest": all(r["manifest_sha256_match"] for r in rows) and len(rows) == len(listed),
    "all_content_addressed": all(r["name_is_sha256"] for r in rows),
    "bundles": rows,
}
print(json.dumps(summary, indent=2))
sys.exit(0 if summary["all_match_manifest"] else 1)
