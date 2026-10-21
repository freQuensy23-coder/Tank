"""Main-thread busy % and scheduler share from a py-spy --idle speedscope profile."""

import collections
import json
import re
import sys

d = json.load(open(sys.argv[1]))
frames = d["shared"]["frames"]
prof = [p for p in d["profiles"] if "MainThread" in p["name"] or "0x" in p["name"]]
prof = max(
    prof,
    key=lambda p: sum(1 for s in p["samples"] if any("scheduler.py" in frames[i].get("file", "") for i in s)),
)
samples = prof["samples"]
n = len(samples)


def name(i):
    f = frames[i]
    return f"{f['name']} ({re.sub(r'.*/', '', f.get('file', ''))}:{f.get('line', '')})"


def idle(s):
    leaf = frames[s[-1]] if s else {}
    return (
        leaf.get("name") in {"select", "poll", "epoll", "_run_once"}
        and "selectors" in leaf.get("file", "")
        or not s
    )


busy = [s for s in samples if not idle(s)]
mine = collections.Counter()
below = collections.Counter()
for s in busy:
    idx = [k for k, i in enumerate(s) if "scheduler.py" in frames[i].get("file", "")]
    if not idx:
        continue
    mine[frames[s[idx[0]]]["name"]] += 1
    j = idx[-1]
    below[name(s[j]) + " -> " + (name(s[j + 1]) if j + 1 < len(s) else "(self)")] += 1
print(f"thread {prof['name']}: samples {n}, busy {100 * len(busy) / n:.1f}%")
tot = sum(mine.values())
print(f"scheduler share of busy: {100 * tot / len(busy):.1f}%  (of wall: {100 * tot / n:.1f}%)")
for k, v in mine.most_common(6):
    print(f"  {100 * v / len(busy):5.1f}%  {k}")
for k, v in below.most_common(14):
    print(f"  {100 * v / len(busy):5.1f}%  {k}")
