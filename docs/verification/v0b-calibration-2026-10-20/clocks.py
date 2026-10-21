import collections
import json
import re
import subprocess

F = "/Users/a.mametyev/Downloads/Tank_V0_MVP_Docs/data/calibration/publisher-organization-successor-1403-20261019/raw-items.publisher-enriched.1403.jsonl"
rows = [json.loads(l) for l in open(F)]
out = {}
how = collections.Counter()
for r in rows:
    if r.get("published_at"):
        out[r["item_id"]] = {"clock": r["published_at"], "basis": "published_at"}
        how["published_at"] += 1
        continue
    if r["source_id"] == "S25":
        raw = subprocess.run(["zstd", "-dc", r["raw_ref"]], capture_output=True).stdout.decode(
            "utf8", "replace"
        )
        ext = r["external_id"]
        m = re.search(r"<id>" + re.escape(ext) + r"</id>(.*?)</entry>", raw, re.S)
        u = m and re.search(r"<updated>([^<]+)</updated>", m.group(1))
        if u:
            out[r["item_id"]] = {
                "clock": u.group(1),
                "basis": "atom_entry_updated",
                "raw_sha256": r["raw_sha256"],
            }
            how["atom"] += 1
            continue
    if r["source_id"] == "S05":
        m = re.search(r"(20\d\d)(\d\d)(\d\d)[a-z]?\.htm", r.get("url") or "")
        if m:
            out[r["item_id"]] = {
                "clock": f"{m[1]}-{m[2]}-{m[3]}T00:00:00Z",
                "basis": "url_date_day_precision",
            }
            how["url"] += 1
            continue
    out[r["item_id"]] = {"clock": r["observed_at"], "basis": "observed_at_fallback"}
    how["fallback"] += 1
print(how)
json.dump(out, open("clocks.json", "w"), indent=0)
