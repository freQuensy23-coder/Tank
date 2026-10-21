#!/usr/bin/env python3
"""Analyze immutable CI benchmark artifacts and raw Actions run/job JSON; no network."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path, PurePosixPath
import sys
import zipfile

KINDS = {"tank", "capture", "sandbox", "openma"}
SCENARIOS = {"cold", "no-change", "source-edit", "dependency-text-edit-same-versions"}
EXPECTED_JOBS = {"go", "parity", "Focused deployment tests", "Full image (tank)",
                 "Full image (capture)", "Full image (sandbox)", "Full OpenMA image"}
CONTROLLED_SCOPE = ("Controlled seven-job verification graph: original Go/parity commands, "
                    "53 focused deployment tests, full Tank/Capture/Sandbox/OpenMA runtime images "
                    "and identical offline smokes. OpenMA API-only SKIP_CONSOLE=1; no new KVM acceptance. "
                    "Stronger than historical/ordinary deps-only Docker coverage; historical capture-only "
                    "runs are not valid before/after baselines.")
class Rejected(ValueError): pass
def require(ok, message):
    if not ok: raise Rejected(message)
def sha(raw): return hashlib.sha256(raw).hexdigest()
def canonical_sha(value): return sha(json.dumps(value, sort_keys=True).encode())
def load_json(path): return json.loads(Path(path).read_bytes())

class Artifact:
    def __init__(self, source):
        self.source = Path(source)
        self.zip = zipfile.ZipFile(self.source) if self.source.suffix.lower() == ".zip" else None
        if self.zip:
            self.names = [x for x in self.zip.namelist() if not x.endswith("/")]
        elif self.source.is_dir():
            self.names = [p.relative_to(self.source).as_posix() for p in self.source.rglob("*") if p.is_file()]
        else:
            raise Rejected(f"Artifact must be extracted directory or ZIP: {source}")
    def read(self, name):
        return self.zip.read(name) if self.zip else (self.source / name).read_bytes()
    def json(self, name): return json.loads(self.read(name))
    def close(self):
        if self.zip: self.zip.close()
    def root_result(self):
        found = []
        for name in self.names:
            if PurePosixPath(name).name != "result.json": continue
            value = self.json(name)
            if isinstance(value, dict) and value.get("kind") in KINDS and ("arms" in value or "results" in value):
                found.append((name, value))
        require(len(found) == 1, f"{self.source}: expected exactly one completed root result, got {len(found)}")
        return found[0]
    def find_inputs(self, result_name):
        parent = PurePosixPath(result_name).parent
        name = (parent / "inputs.json").as_posix()
        require(name in self.names, f"{self.source}: missing captured shared inputs beside root result")
        return self.read(name), self.json(name)
    def environment(self, result_name):
        name = (PurePosixPath(result_name).parent / "environment.json").as_posix()
        return self.json(name) if name in self.names else None

def read_artifacts(sources, expected_variants, required_kinds=KINDS, openma_profile='full'):
    by_kind = {}
    for source in sources:
        view = Artifact(source)
        try:
            result_name, result = view.root_result()
            kind = result["kind"]
            require(kind not in by_kind, f"Duplicate artifact for {kind}")
            raw_inputs, inputs = view.find_inputs(result_name)
            environment = view.environment(result_name)
            if environment:
                require(environment.get("inputs_sha256") == canonical_sha(inputs),
                        f"{kind}: environment inputs SHA does not match captured input object")
            arms = result.get("arms", result.get("results"))
            require(isinstance(arms, list), f"{kind}: invalid completed arms")
            variants = [x.get("variant") for x in arms]
            require(len(variants) == len(set(variants)) and set(variants) == set(expected_variants),
                    f"{kind}: missing/duplicate/failed arms: {variants}")
            indexed = {x["variant"]: x for x in arms}
            for variant, arm in indexed.items():
                require(arm.get("kind") == kind, f"{kind}/{variant}: wrong arm kind")
                require(arm.get("reference", arm.get("recipe_ref")) == inputs[variant],
                        f"{kind}/{variant}: recipe commit differs from frozen inputs")
                if kind != "openma":
                    require(arm.get("product_ref") == inputs["after"], f"{kind}: application source ref differs")
                scenarios = arm.get("scenarios")
                require(isinstance(scenarios, list) and scenarios, f"{kind}/{variant}: no complete scenarios")
                actual = {x.get("scenario") for x in scenarios}
                want = set(SCENARIOS) if len(expected_variants) == 2 else {"cold"}
                if kind == "openma" and len(expected_variants) == 2:
                    require(result.get("paired_profile", "full") == openma_profile and
                            arm.get("paired_profile", "full") == openma_profile,
                            f"openma/{variant}: artifact profile differs from explicitly requested profile")
                    if openma_profile == "cold-source":
                        want = {"cold", "source-edit"}
                        require(set(arm.get("measured_scenarios", [])) == want and
                                set(result.get("measured_scenarios", [])) == want and
                                set(arm.get("omitted_unmeasured_scenarios", [])) == SCENARIOS - want and
                                set(result.get("omitted_unmeasured_scenarios", [])) == SCENARIOS - want,
                                f"openma/{variant}: bounded measured/UNMEASURED declarations are incomplete")
                if kind == "sandbox" and len(expected_variants) == 2:
                    want.remove("source-edit"); want.add("worker-edit")
                require(len(scenarios) == len(actual) and actual == want,
                        f"{kind}/{variant}: incomplete scenario set {actual}, expected {want}")
                for scenario in scenarios:
                    label = f"{kind}/{variant}/{scenario.get('scenario')}"
                    require(isinstance(scenario.get("wall_seconds"), (int, float)) and scenario["wall_seconds"] > 0,
                            label + ": missing/nonpositive build/load duration")
                    require(scenario.get("image_id") and scenario.get("vertices"), label + ": missing real image/timing proof")
                    require(not any(v.get("error") for v in scenario["vertices"]), label + ": failed BuildKit vertex")
                    if kind == "openma":
                        smoke = scenario.get("offline_runtime_smoke", {})
                        require(smoke.get("native_checksums") == "PASS" and smoke.get("native_platform") == "amd64"
                                and smoke.get("node_major") == 24 and smoke.get("pnpm") == "11.0.8"
                                and smoke.get("tsx") == 42 and smoke.get("network") == "none",
                                label + ": missing/failed full runtime smoke")
                    else:
                        require(isinstance(scenario.get("smoke_seconds"), (int, float)) and scenario["smoke_seconds"] >= 0,
                                label + ": missing successful shared-smoke duration")
            by_kind[kind] = {"artifact": str(source), "captured_inputs_sha256": sha(raw_inputs),
                             "canonical_inputs_sha256": canonical_sha(inputs), "inputs": inputs,
                             "arms": indexed, "root_result": result}
        finally:
            view.close()
    require(set(by_kind) == set(required_kinds), f"Missing/unexpected requested image artifacts: expected {sorted(required_kinds)}, got {sorted(by_kind)}")
    canonical = {x["canonical_inputs_sha256"] for x in by_kind.values()}
    raw = {x["captured_inputs_sha256"] for x in by_kind.values()}
    require(len(canonical) == 1 and len(raw) == 1, "Input SHA differs across image artifacts")
    return by_kind

def match_contract(kind, old, new):
    if kind in ("tank", "sandbox"):
        require(isinstance(old.get("runtime_inventory"), dict) and old["runtime_inventory"],
                f"{kind}: baseline runtime inventory absent")
        require(old["runtime_inventory"] == new.get("runtime_inventory"),
                f"{kind}: runtime dependency/OS inventory differs")
        return {"kind": "exact Python/OS runtime inventory", "match": True}
    if kind == "openma":
        fields = ("frozen_lock_sha256", "workspace_sha256", "entrypoint_sha256",
                  "native_shim_sha256", "native_provenance_sha256", "node_base")
        for key in fields:
            require(old.get(key) and old[key] == new.get(key), f"openma: runtime contract mismatch: {key}")
        return {"kind": "exact dependency/workspace lock, entrypoint, native bytes and shared Node base",
                "match": True, "full_OS_or_npm_inventory": "not recorded; no broader inventory equivalence claim"}
    require(old.get("product_ref") and old["product_ref"] == new.get("product_ref"),
            "capture: source snapshots differ")
    return {"kind": "same Go source + identical static ELF/version/config/health smoke",
            "match": True, "Python_runtime_inventory": "not applicable to static Go executable"}

def comparison(before, after):
    require(before > 0 and after >= 0, "Invalid comparison duration")
    return {"before": before, "after": after, "change_percent": (after / before - 1) * 100,
            "reduction_percent": (1 - after / before) * 100,
            "after_over_before": after / before, "speedup_before_over_after": before / after if after else None}

def compare_images(old_set, new_set=None):
    rows = {}
    if new_set is None:
        old = {k: v["arms"]["before"] for k, v in old_set.items()}
        new = {k: v["arms"]["after"] for k, v in old_set.items()}
    else:
        require({v["canonical_inputs_sha256"] for v in old_set.values()} ==
                {v["canonical_inputs_sha256"] for v in new_set.values()}, "Before/after CI input SHA mismatch")
        require({v["captured_inputs_sha256"] for v in old_set.values()} ==
                {v["captured_inputs_sha256"] for v in new_set.values()}, "Before/after CI captured bytes differ")
        old = {k: v["arms"]["before"] for k, v in old_set.items()}
        new = {k: v["arms"]["after"] for k, v in new_set.items()}
    require(set(old) == set(new), "Requested before/after image kinds differ")
    for kind in sorted(old):
        runtime = match_contract(kind, old[kind], new[kind])
        a = {x["scenario"]: x for x in old[kind]["scenarios"]}
        b = {x["scenario"]: x for x in new[kind]["scenarios"]}
        require(set(a) == set(b), f"{kind}: before/after scenarios differ")
        rows[kind] = {"runtime_contract": runtime,
                      "measured_scenarios": list(a),
                      "omitted_unmeasured_scenarios": {
                          name: {"status": "UNMEASURED", "before_seconds": None, "after_seconds": None}
                          for name in sorted(({"cold", "no-change", "worker-edit", "dependency-text-edit-same-versions"}
                                              if kind == "sandbox" else SCENARIOS) - set(a))
                      },
                      "build_load_seconds": {name: comparison(a[name]["wall_seconds"], b[name]["wall_seconds"])
                                             for name in a},
                      "image_bytes": {name: {"before": a[name]["image_bytes"], "after": b[name]["image_bytes"],
                                            "delta": b[name]["image_bytes"] - a[name]["image_bytes"]} for name in a},
                      "context_probe": {"scope": "Neutral scratch COPY export, not production transfer savings",
                                        "before_logical_bytes": old[kind]["context_probe"]["logical_bytes"],
                                        "after_logical_bytes": new[kind]["context_probe"]["logical_bytes"]}}
    return rows

def parse_time(value):
    require(isinstance(value, str), "Missing Actions timestamp")
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))

def ci_metrics(run_path, jobs_path):
    run, job_data = load_json(run_path), load_json(jobs_path)
    jobs = job_data["jobs"] if isinstance(job_data, dict) else job_data
    if isinstance(job_data, dict):
        require(job_data.get("total_count", len(jobs)) == len(jobs), "Jobs JSON pagination incomplete")
    require(run.get("status") == "completed" and run.get("conclusion") == "success",
            f"Whole-CI run {run.get('id')} is missing/failed/incomplete")
    require("docker-ci-comparison.yml" in run.get("path", ""), "Run uses a different verification workflow")
    require(len(jobs) == 7 and {j["name"] for j in jobs} == EXPECTED_JOBS, "Not the controlled seven-job graph")
    for job in jobs:
        require(job.get("status") == "completed" and job.get("conclusion") == "success",
                f"Missing/failed/incomplete job: {job['name']}")
    created, started = parse_time(run["created_at"]), parse_time(run["run_started_at"])
    first = min(parse_time(j["started_at"]) for j in jobs)
    end = max(parse_time(j["completed_at"]) for j in jobs)
    durations = {j["name"]: (parse_time(j["completed_at"]) - parse_time(j["started_at"])).total_seconds()
                 for j in jobs}
    require(all(x >= 0 for x in durations.values()) and created <= started <= end and created <= first,
            "Inconsistent Actions timing order")
    return {"run_id": run["id"], "head_sha": run["head_sha"], "event": run.get("event"),
            "created_at": run["created_at"], "run_started_at": run["run_started_at"],
            "last_required_job_completed_at": end.isoformat(),
            "time_to_green_seconds": (end - created).total_seconds(),
            "dispatch_to_run_start_seconds": (started - created).total_seconds(),
            "queue_to_first_job_seconds": (first - created).total_seconds(),
            "execution_window_seconds": (end - started).total_seconds(),
            "job_envelope_seconds": (end - first).total_seconds(),
            "max_job_seconds": max(durations.values()), "sum_job_seconds": sum(durations.values()),
            "job_seconds": durations,
            "note": "Sum of overlapping job durations is not whole-CI elapsed wall time."}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--paired-artifact", type=Path, action="append", default=[])
    p.add_argument("--kinds", choices=sorted(KINDS), nargs="+", default=sorted(KINDS), help="Requested paired-image kinds; full CI always requires all four")
    p.add_argument("--openma-paired-profile", choices=("full", "cold-source"), default="full",
                   help="Explicitly require full or bounded cold/source OpenMA measurements")
    p.add_argument("--ci-before-artifact", type=Path, action="append", default=[])
    p.add_argument("--ci-after-artifact", type=Path, action="append", default=[])
    p.add_argument("--ci-before-run", type=Path)
    p.add_argument("--ci-before-jobs", type=Path)
    p.add_argument("--ci-after-run", type=Path)
    p.add_argument("--ci-after-jobs", type=Path)
    p.add_argument("--expected-inputs-sha256", default="bc52df82d004ad13f9d6732512c9de5c6a9948a26c0f63a5cc56f120264fbcde")
    p.add_argument("--focused-count", type=int, default=53, help="Actual same focused-test count in both compared runs")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    scope = CONTROLLED_SCOPE.replace("53 focused deployment tests", f"{args.focused_count} focused deployment tests")
    if not any((args.ci_before_run, args.ci_before_jobs, args.ci_after_run, args.ci_after_jobs)):
        scope = ("Paired image-only experiments on a shared runner per image; independent before/after builders, frozen inputs, same application source and offline smokes. Measured and explicitly UNMEASURED scenarios are listed per image. No whole-CI wall-time observation in this section.")
    report = {"status": "REJECTED", "scope": scope,
              "interpretation": "One observed before/after pair. No statistically stable median or typical-speed claim.",
              "delta_definition": "Negative change_percent / positive reduction_percent means faster. Ratios are explicitly named."}
    try:
        require(args.focused_count > 0, "Focused-test count must be positive")
        require(args.paired_artifact or args.ci_before_run, "No benchmark observations supplied")
        hashes = set()
        if args.paired_artifact:
            paired = read_artifacts(args.paired_artifact, {"before", "after"}, set(args.kinds),
                                    openma_profile=args.openma_paired_profile)
            report["requested_paired_kinds"] = sorted(set(args.kinds))
            hashes.update(v["captured_inputs_sha256"] for v in paired.values())
            report["paired_images"] = compare_images(paired)
        ci_args = (args.ci_before_run, args.ci_before_jobs, args.ci_after_run, args.ci_after_jobs)
        if any(ci_args) or args.ci_before_artifact or args.ci_after_artifact:
            require(all(ci_args), "Whole CI needs both raw run and full jobs JSON for before and after")
            require(args.ci_before_artifact and args.ci_after_artifact,
                    "Whole CI needs all four image artifacts per arm to verify inventories and inputs")
            old = read_artifacts(args.ci_before_artifact, {"before"})
            new = read_artifacts(args.ci_after_artifact, {"after"})
            hashes.update(v["captured_inputs_sha256"] for v in old.values())
            hashes.update(v["captured_inputs_sha256"] for v in new.values())
            report["ci_images"] = compare_images(old, new)
            a = ci_metrics(args.ci_before_run, args.ci_before_jobs)
            b = ci_metrics(args.ci_after_run, args.ci_after_jobs)
            require(a["head_sha"] == b["head_sha"], "Workflow/helper source commit changed between CI runs")
            report["whole_ci"] = {"before": a, "after": b,
                                 "time_to_green": comparison(a["time_to_green_seconds"], b["time_to_green_seconds"]),
                                 "execution_window": comparison(a["execution_window_seconds"], b["execution_window_seconds"]),
                                 "graph": scope}
        require(hashes == {args.expected_inputs_sha256}, f"Captured input bytes not the frozen reviewed inputs: {sorted(hashes)}")
        report["captured_inputs_sha256"] = args.expected_inputs_sha256
        report["status"] = "PASS"
    except (Rejected, KeyError, TypeError, ValueError, OSError, zipfile.BadZipFile) as error:
        report["error"] = str(error)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "output": str(args.output), "error": report.get("error")}, indent=2))
    return 0 if report["status"] == "PASS" else 1
if __name__ == "__main__":
    raise SystemExit(main())

