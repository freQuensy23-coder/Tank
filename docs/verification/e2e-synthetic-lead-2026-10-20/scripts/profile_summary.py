"""Summarize a py-spy raw (collapsed) GIL profile: share per thread, and inclusive app frames."""

import collections
import re
import sys

threads = collections.Counter()
inclusive = collections.defaultdict(collections.Counter)
leaf_lib = collections.defaultdict(collections.Counter)
total = 0
for line in open(sys.argv[1]):
    stack, _, count = line.rstrip("\n").rpartition(" ")
    if not count.isdigit():
        continue
    n = int(count)
    frames = stack.split(";")
    thread = frames[0]
    total += n
    threads[thread] += n
    seen = set()
    for f in frames[1:]:
        m = re.match(r"(.+?) \((?:/app/src/)?(tank/[^:]+):(\d+)\)", f)
        if m:
            key = f"{m.group(2).removeprefix('tank/')}:{m.group(1)}"
            if key not in seen:
                inclusive[thread][key] += n
                seen.add(key)
    leaf = re.sub(r":\d+\)$", ")", frames[-1])
    leaf_lib[thread][leaf] += n
print("samples", total)
for thread, n in threads.most_common(6):
    print(f"\n== {thread}: {n} samples, {n / total:.1%} of GIL samples")
    for key, m in inclusive[thread].most_common(14):
        print(f"   {m / n:6.1%}  {key}")
    print("   leaf frames:")
    for key, m in leaf_lib[thread].most_common(6):
        print(f"   {m / n:6.1%}  {key[:150]}")
