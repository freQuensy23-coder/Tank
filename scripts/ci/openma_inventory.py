"""Inventory Tank-authored vendor programs against the pinned upstream archive."""

import argparse
import ast
import hashlib
import json
import subprocess
import tarfile
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Literal

from pydantic import BaseModel, ConfigDict

from scripts.ci.refactor_inventory import EmbeddedPython, Kind, Scan

UPSTREAM_COMMIT = "85d5361f535c7221de975838dc48444e468cc02e"
ARCHIVE_SHA256 = "a1bca52a11dd2e621b9a451e204abe5055b70fc45a8db017fd00587ebce618e9"
SOURCE_EXTENSIONS = frozenset({".py", ".go", ".js", ".mjs", ".jsx", ".ts", ".mts", ".cts", ".tsx"})
JAVASCRIPT_EXTENSIONS = SOURCE_EXTENSIONS - {".py", ".go"}


class Contract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class AuthoredRange(Contract):
    start: int
    end: int


class SourceFinding(Contract):
    path: str
    line: int
    symbol: str
    kind: Kind | Literal["object", "embedded_python", "embedded_python_command"]
    expression: str
    embedded_line: int | None = None


class Program(Contract):
    path: str
    classification: Literal["TANK_ADDED", "MODIFIED_UPSTREAM"]
    sha256: str
    upstream_sha256: str | None
    authored_ranges: list[AuthoredRange]
    removed_upstream_lines: int
    owner: Literal["agents", "quality", "foundation", "UNASSIGNED"]
    assignment_status: str | None
    reviewed_input_sha256: str | None
    assignment_source_matches: bool
    completion_review: Literal["OPEN"]
    caller_review: Literal["OPEN"]
    caller_ledger: str | None
    authored_findings: list[SourceFinding]
    preserved_upstream_findings: list[SourceFinding]
    embedded_python: list[EmbeddedPython]


class JavascriptView(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    findings: list[SourceFinding]
    embedded: list[EmbeddedPython]


class SemanticCallerRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    path: str


class SemanticCallerLedger(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    records: list[SemanticCallerRecord]


class OwnershipAssignment(Contract):
    path: str
    owner: Literal["agents", "quality", "foundation"]
    classification: Literal["TANK_ADDED", "MODIFIED_UPSTREAM"]
    reviewed_input_sha256: str
    scope: str
    status: str


class OwnershipCounts(Contract):
    quality: int
    agents: int
    foundation: int


class OwnershipManifest(Contract):
    source_coverage_sha256: str
    coverage_checkout_commit: str
    upstream_commit: str
    archive_sha256: str
    priority: str
    assignments: list[OwnershipAssignment]
    counts: OwnershipCounts


class VendorInventory(Contract):
    schema_version: Literal["tank.openma-authored-inventory.v2"]
    checkout_commit: str
    ownership_manifest: str
    ownership_sha256: str
    upstream_commit: str
    archive_sha256: str
    scanner_sha256: dict[str, str]
    coverage: list[str]
    tracked_programs: int
    unchanged_upstream_programs: int
    programs: list[Program]
    authored_counts: dict[str, int]
    preserved_upstream_counts: dict[str, int]


def changed_ranges(before: str, after: str) -> tuple[list[AuthoredRange], int]:
    ranges: list[AuthoredRange] = []
    removed = 0
    for operation, old_start, old_end, new_start, new_end in SequenceMatcher(
        a=before.splitlines(), b=after.splitlines(), autojunk=False
    ).get_opcodes():
        if operation == "equal":
            continue
        removed += old_end - old_start
        if new_end > new_start:
            ranges.append(AuthoredRange(start=new_start + 1, end=new_end))
    return ranges, removed


def archive_sources(archive: Path) -> dict[str, bytes]:
    with archive.open("rb") as archive_stream:
        actual = hashlib.file_digest(archive_stream, "sha256").hexdigest()
    if actual != ARCHIVE_SHA256:
        raise ValueError("Upstream archive differs from the pinned checksum")
    result: dict[str, bytes] = {}
    prefix = "open-managed-agents-" + UPSTREAM_COMMIT
    with tarfile.open(archive, "r:gz") as source:
        for member in source:
            if not member.isfile():
                continue
            relative = PurePosixPath(member.name).relative_to(prefix)
            if relative.suffix not in SOURCE_EXTENSIONS:
                continue
            stream = source.extractfile(member)
            if stream is None:
                raise ValueError("Upstream archive member has no regular-file stream")
            with stream:
                result["vendor/openma/" + str(relative)] = stream.read()
    return result


def authored(program: Program, line: int) -> bool:
    return any(span.start <= line <= span.end for span in program.authored_ranges)


def load_ownership(path: Path) -> OwnershipManifest:
    ownership = OwnershipManifest.model_validate_json(path.read_bytes())
    if ownership.upstream_commit != UPSTREAM_COMMIT or ownership.archive_sha256 != ARCHIVE_SHA256:
        raise ValueError("Ownership manifest refers to a different upstream baseline")
    assignments = {assignment.path: assignment for assignment in ownership.assignments}
    if len(assignments) != len(ownership.assignments):
        raise ValueError("Ownership manifest contains duplicate program assignments")
    owner_counts = Counter(assignment.owner for assignment in ownership.assignments)
    if (owner_counts["quality"], owner_counts["agents"], owner_counts["foundation"]) != (
        ownership.counts.quality,
        ownership.counts.agents,
        ownership.counts.foundation,
    ):
        raise ValueError("Ownership counts do not describe the actual assignments")
    return ownership


def program_inventory(root: Path, archive: Path, node: Path) -> VendorInventory:
    baseline = archive_sources(archive)
    ledger_path = root / "docs/refactor/agents-openma-provenance.json"
    callers = SemanticCallerLedger.model_validate_json(ledger_path.read_bytes())
    agents = {record.path for record in callers.records}
    ownership_path = root / "docs/refactor/openma-ownership.json"
    ownership = load_ownership(ownership_path)
    assignments = {assignment.path: assignment for assignment in ownership.assignments}
    tracked = (
        subprocess.check_output(["git", "-C", str(root), "ls-files", "-z", "--", "vendor/openma"])
        .decode()
        .split("\0")
    )
    programs: list[Program] = []
    total = 0
    unchanged = 0
    for name in tracked:
        if not name or Path(name).suffix not in SOURCE_EXTENSIONS:
            continue
        total += 1
        path = root / name
        if path.is_symlink():
            raise ValueError("Executable vendor symlinks need an explicit provenance classification")
        current = path.read_bytes()
        original = baseline.get(name)
        if original == current:
            unchanged += 1
            continue
        if original is None:
            ranges = [AuthoredRange(start=1, end=max(1, len(current.splitlines())))]
            removed = 0
            classification: Literal["TANK_ADDED", "MODIFIED_UPSTREAM"] = "TANK_ADDED"
        else:
            ranges, removed = changed_ranges(original.decode(), current.decode())
            classification = "MODIFIED_UPSTREAM"
        assignment = assignments.get(name)
        owner: Literal["agents", "quality", "foundation", "UNASSIGNED"] = "UNASSIGNED"
        if assignment is not None:
            if assignment.classification != classification:
                raise ValueError("Ownership classification differs for " + name)
            owner = assignment.owner
        caller_ledger: str | None = None
        if name in agents:
            caller_ledger = "docs/refactor/agents-openma-provenance.json:" + name
        elif name in {
            "vendor/openma/tank-deploy/openma_entrypoint.mjs",
            "vendor/openma/tank-deploy/openma_entrypoint.mts",
        }:
            caller_ledger = "docs/refactor/quality.md:Typed OpenMA deployment entrypoint"
        programs.append(
            Program(
                path=name,
                classification=classification,
                sha256=hashlib.sha256(current).hexdigest(),
                upstream_sha256=None if original is None else hashlib.sha256(original).hexdigest(),
                authored_ranges=ranges,
                removed_upstream_lines=removed,
                owner=owner,
                assignment_status=None if assignment is None else assignment.status,
                reviewed_input_sha256=None if assignment is None else assignment.reviewed_input_sha256,
                assignment_source_matches=assignment is not None
                and assignment.reviewed_input_sha256 == hashlib.sha256(current).hexdigest(),
                completion_review="OPEN",
                caller_review="OPEN",
                caller_ledger=caller_ledger,
                authored_findings=[],
                preserved_upstream_findings=[],
                embedded_python=[],
            )
        )
    with TemporaryDirectory(prefix="tank-vendor-inventory-") as temporary:
        directory = Path(temporary)
        paths = directory / "paths.txt"
        paths.write_text(
            "".join(
                program.path + "\n"
                for program in programs
                if Path(program.path).suffix in JAVASCRIPT_EXTENSIONS
            )
        )
        report_path = directory / "javascript.json"
        subprocess.run(
            [
                str(node),
                str(root / "scripts/ci/refactor_inventory.mjs"),
                "--output",
                str(report_path),
                "--additional-paths",
                str(paths),
            ],
            cwd=root,
            check=True,
        )
        javascript = JavascriptView.model_validate_json(report_path.read_bytes())
    by_path = {program.path: program for program in programs}
    observations = list(javascript.findings)
    for snippet in javascript.embedded:
        program = by_path.get(snippet.path)
        if program is None:
            continue
        program.embedded_python.append(snippet)
        scan = Scan(snippet.path, line_offset=snippet.line - 1, scope=(snippet.symbol,))
        scan.visit(ast.parse(snippet.source, filename=snippet.path + ":" + snippet.symbol))
        for python_finding in scan.findings:
            observations.append(
                SourceFinding(
                    path=python_finding.path,
                    line=snippet.line if snippet.encoding == "decoded-literal" else python_finding.line,
                    symbol=python_finding.symbol,
                    kind=python_finding.kind,
                    expression=python_finding.expression,
                    embedded_line=python_finding.line - snippet.line + 1,
                )
            )
    for program in programs:
        if Path(program.path).suffix == ".go":
            raise ValueError("Tank-authored vendor Go requires explicit scanner support")
        if Path(program.path).suffix == ".py":
            scan = Scan(program.path)
            scan.visit(ast.parse((root / program.path).read_text(), filename=program.path))
            observations.extend(SourceFinding.model_validate(item.model_dump()) for item in scan.findings)
    for finding in observations:
        finding = finding.model_copy(update={"expression": finding.expression[:240]})
        program = by_path.get(finding.path)
        if program is None:
            continue
        destination = (
            program.authored_findings
            if authored(program, finding.line)
            else program.preserved_upstream_findings
        )
        destination.append(finding)
    return VendorInventory(
        schema_version="tank.openma-authored-inventory.v2",
        ownership_manifest="docs/refactor/openma-ownership.json",
        ownership_sha256=hashlib.sha256(ownership_path.read_bytes()).hexdigest(),
        checkout_commit=subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"])
        .decode()
        .strip(),
        upstream_commit=UPSTREAM_COMMIT,
        archive_sha256=ARCHIVE_SHA256,
        scanner_sha256={
            name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in (
                "scripts/ci/openma_inventory.py",
                "scripts/ci/refactor_inventory.py",
                "scripts/typescript/inventory.mts",
            )
        },
        coverage=[
            "Every tracked vendor/openma Python, Go, JavaScript and TypeScript program compared byte-for-byte with pinned upstream",
            "Whole Tank-added programs and exact added/replaced target-line ranges of modified upstream programs",
            "Syntax candidates outside authored ranges retained separately as preserved-upstream observations",
            "Static raw-template and decoded-literal Python workers retain physical container and embedded Python line coordinates",
            "Ownership and semantic caller evidence explicit where assigned; remaining ownership/caller review stays open",
            "Syntax observations do not prove strict typing or behavior; unchanged upstream source is preserved",
        ],
        tracked_programs=total,
        unchanged_upstream_programs=unchanged,
        programs=programs,
        authored_counts=dict(
            sorted(Counter(item.kind for program in programs for item in program.authored_findings).items())
        ),
        preserved_upstream_counts=dict(
            sorted(
                Counter(
                    item.kind for program in programs for item in program.preserved_upstream_findings
                ).items()
            )
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--upstream-archive", type=Path, required=True)
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = program_inventory(args.root.resolve(), args.upstream_archive, args.node)
    args.output.write_text(report.model_dump_json(indent=2) + "\n")
    print(
        json.dumps(
            {
                "tracked_programs": report.tracked_programs,
                "unchanged_upstream_programs": report.unchanged_upstream_programs,
                "tank_added": sum(program.classification == "TANK_ADDED" for program in report.programs),
                "modified_upstream": sum(
                    program.classification == "MODIFIED_UPSTREAM" for program in report.programs
                ),
                "authored_counts": report.authored_counts,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
