"""Read-only access to the deployed Tank database for acceptance measurements.

Every query runs inside a READ ONLY transaction through psql in the Postgres
container, so these scripts can never write production data and need no secrets.
"""
import json
import os
import statistics
import subprocess
from datetime import UTC, datetime

PG_CONTAINER = os.environ.get("TANK_PG_CONTAINER", "tank-postgres-1")
PG_DATABASE = os.environ.get("TANK_PG_DATABASE", "tank")


def query(sql, params=None):
    """Return rows of `sql` as dicts. `params` are substituted as SQL literals by psql (:'name')."""
    args = ["docker", "exec", "-i", "-u", "postgres", PG_CONTAINER, "psql", "-d", PG_DATABASE,
            "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1"]
    for key, value in (params or {}).items():
        args += ["-v", f"{key}={value}"]
    script = (
        "BEGIN TRANSACTION READ ONLY;\n"
        f"SELECT coalesce(json_agg(t), '[]'::json) FROM ({sql}) t;\n"
        "COMMIT;\n"
    )
    out = subprocess.run(args, input=script, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError("psql failed: " + out.stderr.strip()[-500:])
    return json.loads(out.stdout)


def parse(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))


def seconds(later, earlier):
    if later is None or earlier is None:
        return None
    return (parse(later) - parse(earlier)).total_seconds()


def percentile(values, q):
    """Nearest-rank percentile (q in 0..100); None for an empty sample."""
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    rank = max(1, -(-len(values) * q // 100))
    return values[int(rank) - 1]


def summary(values, target=None, stat="p95"):
    known = [v for v in values if v is not None]
    result = {
        "n": len(values),
        "measured": len(known),
        "unmeasured": len(values) - len(known),
        "min": min(known) if known else None,
        "median": statistics.median(known) if known else None,
        "p95": percentile(known, 95),
        "max": max(known) if known else None,
        "target_s": target,
        "statistic": stat,
    }
    if target is not None:
        if stat == "p95":
            value = result["p95"]
        elif stat == "median":
            value = result["median"]
        else:  # every sample
            value = result["max"]
        result["within_target"] = sum(1 for v in known if v <= target)
        if not values:
            result["status"] = "NO_SAMPLES"
        elif result["unmeasured"]:
            result["status"] = "UNVERIFIED"
        else:
            result["status"] = "PASS" if value <= target else "FAIL"
    return result


def utcnow():
    return datetime.now(UTC)


def iso(ts):
    return parse(ts).astimezone(UTC).isoformat().replace("+00:00", "Z") if ts else None


def write_json(path, value):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(value, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")
