"""Inventory refactor candidates without pretending syntax findings prove behavior."""

import argparse
import ast
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

Kind = Literal[
    "untyped_parameter",
    "untyped_return",
    "any",
    "none_default",
    "optional",
    "fallback",
    "dictionary",
    "dynamic_go",
    "assert_constant",
    "assert_self_comparison",
]
EXCLUDED = frozenset(
    {
        ".git",
        ".venv",
        "node_modules",
        "vendor",
        "upstream",
        "history",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
    }
)


class Finding(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    path: str
    line: int
    symbol: str
    kind: Kind
    expression: str


class ReferenceConfiguration(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    role: Literal["historical-oracle-recipe"]
    sha256: str
    git_blob: str
    reference_commit: str
    reason: str
    output_consumer: str


class ReferenceProgram(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    provenance: ReferenceConfiguration
    findings: list[Finding]


REFERENCE_PROGRAMS = {
    "capture/internal/budget/testdata/gen_vectors.py": ReferenceConfiguration(
        role="historical-oracle-recipe",
        sha256="5aa37e93275faad4f8c256002e1efce27e772aa07fa7a871d94c5c2c6de1dc0e",
        git_blob="b6f81d1e94f1e2c2f46b45d330776cbf76616317",
        reference_commit="ef68a822ceafeca3e37b405535a9cb310e4fa35a",
        reason="Unchanged historical Python request-budget planner recipe; current imports are unavailable. Do not restore retired planner APIs.",
        output_consumer="capture/internal/budget/budget_test.go:loadVectors reads testdata/vectors.json; CI does not execute this recipe",
    ),
    "capture/internal/rest/testdata/gen_vectors.py": ReferenceConfiguration(
        role="historical-oracle-recipe",
        sha256="ee12c5940f249c5f39b0124015119b38868bd84c805dd6986c6f1c9e1ce92907",
        git_blob="ff9b6c0962d9cf65e10cdc9447ed49621e529a5a",
        reference_commit="ef68a822ceafeca3e37b405535a9cb310e4fa35a",
        reason="Unchanged historical VenueAdapter/MarketService._request recipe; those Python exchange producers are retired.",
        output_consumer="capture/internal/rest/rest_test.go:loadVectors reads testdata/vectors.json; CI does not execute this recipe",
    ),
}


class EmbeddedPython(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    path: str
    line: int
    symbol: str
    language: Literal["python"]
    source: str


class JavascriptReport(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    embedded: list[EmbeddedPython]


class Inventory(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    files: int
    coverage: list[str]
    embedded_python: int
    counts: dict[str, int]
    findings: list[Finding]
    areas: dict[str, dict[str, int]]
    modules: dict[str, dict[str, int]]
    symbols: dict[str, dict[str, int]]
    imports: dict[str, list[str]]
    cycles: list[list[str]]
    reference_programs: dict[str, ReferenceProgram]


class Scan(ast.NodeVisitor):
    def __init__(self, path: str, line_offset: int = 0, scope: tuple[str, ...] = ()) -> None:
        self.path = path
        self.line_offset = line_offset
        self.scope = list(scope)
        self.findings: list[Finding] = []

    def record(self, node: ast.expr | ast.stmt | ast.arg, kind: Kind, expression: str) -> None:
        self.findings.append(
            Finding(
                path=self.path,
                line=node.lineno + self.line_offset,
                symbol=".".join(self.scope),
                kind=kind,
                expression=expression,
            )
        )

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.scope.append(node.name)
        parameters = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        parameters.extend(
            parameter for parameter in (node.args.vararg, node.args.kwarg) if parameter is not None
        )
        for parameter in parameters:
            if parameter.annotation is None and parameter.arg not in {"self", "cls"}:
                self.record(parameter, "untyped_parameter", parameter.arg)
        if node.returns is None:
            self.record(node, "untyped_return", node.name)
        for default in [*node.args.defaults, *node.args.kw_defaults]:
            if isinstance(default, ast.Constant) and default.value is None:
                self.record(default, "none_default", "None")
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in {"Any", "Optional"}:
            self.record(node, "any" if node.id == "Any" else "optional", node.id)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in {"Any", "Optional"}:
            self.record(node, "any" if node.attr == "Any" else "optional", ast.unparse(node))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr == "get":
            self.record(node, "fallback", ast.unparse(node))
        if isinstance(node.func, ast.Name) and node.func.id == "dict":
            self.record(node, "dictionary", ast.unparse(node))
        self.generic_visit(node)

    def visit_BoolOp(self, node: ast.BoolOp) -> None:
        if isinstance(node.op, ast.Or):
            self.record(node, "fallback", ast.unparse(node))
        self.generic_visit(node)

    def visit_Dict(self, node: ast.Dict) -> None:
        self.record(node, "dictionary", ast.unparse(node))
        self.generic_visit(node)

    def visit_Assert(self, node: ast.Assert) -> None:
        if isinstance(node.test, ast.Constant):
            self.record(node, "assert_constant", ast.unparse(node.test))
        if isinstance(node.test, ast.Compare) and len(node.test.comparators) == 1:
            left = node.test.left
            right = node.test.comparators[0]
            if not any(
                isinstance(child, ast.Call) for value in (left, right) for child in ast.walk(value)
            ) and ast.dump(left) == ast.dump(right):
                self.record(node, "assert_self_comparison", ast.unparse(node.test))
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if isinstance(node.value, ast.Name) and node.value.id == "dict":
            self.record(node, "dictionary", ast.unparse(node))
        self.generic_visit(node)


def dependencies(root: Path) -> tuple[dict[str, list[str]], list[list[str]]]:
    modules: dict[str, Path] = {}
    for path in sorted((root / "src").rglob("*.py")):
        relative = path.relative_to(root / "src").with_suffix("")
        if EXCLUDED.intersection(relative.parts):
            continue
        parts = relative.parts[:-1] if relative.name == "__init__" else relative.parts
        modules[".".join(parts)] = path
    graph: dict[str, list[str]] = {}
    for name, path in modules.items():
        edges: set[str] = set()
        for node in ast.walk(ast.parse(path.read_text())):
            candidates: list[str] = []
            if isinstance(node, ast.Import):
                candidates = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                candidates = [node.module + "." + alias.name for alias in node.names]
                candidates.append(node.module)
            for candidate in candidates:
                while candidate:
                    if candidate in modules:
                        if candidate != name:
                            edges.add(candidate)
                        break
                    candidate = candidate.rpartition(".")[0]
        graph[name] = sorted(edges)
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    active: set[str] = set()
    cycles: list[list[str]] = []

    def visit(name: str) -> None:
        nonlocal index
        indices[name] = lowlinks[name] = index
        index += 1
        stack.append(name)
        active.add(name)
        for target in graph[name]:
            if target not in indices:
                visit(target)
                lowlinks[name] = min(lowlinks[name], lowlinks[target])
            elif target in active:
                lowlinks[name] = min(lowlinks[name], indices[target])
        if lowlinks[name] == indices[name]:
            component: list[str] = []
            while True:
                target = stack.pop()
                active.remove(target)
                component.append(target)
                if target == name:
                    break
            if len(component) > 1:
                cycles.append(sorted(component))

    for name in graph:
        if name not in indices:
            visit(name)
    return graph, sorted(cycles)


def inventory(root: Path, embedded: tuple[EmbeddedPython, ...] = ()) -> Inventory:
    findings: list[Finding] = []
    references: dict[str, ReferenceProgram] = {}
    count = 0
    for directory in ("src", "tests", "scripts", "deploy", "capture"):
        for path in sorted((root / directory).rglob("*")):
            relative = path.relative_to(root)
            if (
                EXCLUDED.intersection(relative.parts)
                or path.suffix not in {".py", ".go"}
                or not path.is_file()
            ):
                continue
            count += 1
            source = path.read_text()
            if path.suffix == ".py":
                scan = Scan(str(relative))
                scan.visit(ast.parse(source, filename=str(relative)))
                reference = REFERENCE_PROGRAMS.get(str(relative))
                if reference is not None:
                    if hashlib.sha256(path.read_bytes()).hexdigest() != reference.sha256:
                        raise ValueError(f"Historical reference program changed: {relative}")
                    references[str(relative)] = ReferenceProgram(provenance=reference, findings=scan.findings)
                else:
                    findings.extend(scan.findings)
            else:
                for line, value in enumerate(source.splitlines(), 1):
                    if re.search(r"\bany\b|interface\s*\{", value):
                        findings.append(
                            Finding(
                                path=str(relative),
                                line=line,
                                symbol="",
                                kind="dynamic_go",
                                expression=value.strip(),
                            )
                        )
    for snippet in embedded:
        scan = Scan(snippet.path, line_offset=snippet.line - 1, scope=(snippet.symbol,))
        scan.visit(ast.parse(snippet.source, filename=snippet.path + ":" + snippet.symbol))
        findings.extend(scan.findings)
    areas: defaultdict[str, Counter[str]] = defaultdict(Counter)
    modules: defaultdict[str, Counter[str]] = defaultdict(Counter)
    symbols: defaultdict[str, Counter[str]] = defaultdict(Counter)
    for finding in findings:
        areas[finding.path.split("/")[0]][finding.kind] += 1
        modules[finding.path][finding.kind] += 1
        symbols[finding.path + ":" + finding.symbol][finding.kind] += 1
    graph, cycles = dependencies(root)
    return Inventory(
        files=count - len(references),
        coverage=[
            "active Python files",
            "active Go files",
            "explicit embedded Python snippets from JavaScript report",
        ],
        embedded_python=len(embedded),
        counts=dict(sorted(Counter(item.kind for item in findings).items())),
        findings=findings,
        areas={key: dict(value) for key, value in sorted(areas.items())},
        modules={key: dict(value) for key, value in sorted(modules.items())},
        symbols={key: dict(value) for key, value in sorted(symbols.items())},
        imports=graph,
        cycles=cycles,
        reference_programs=references,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--details", action="store_true")
    parser.add_argument("--javascript-report", type=Path)
    args = parser.parse_args()
    embedded: tuple[EmbeddedPython, ...] = ()
    if args.javascript_report is not None:
        embedded = tuple(JavascriptReport.model_validate_json(args.javascript_report.read_text()).embedded)
    report = inventory(args.root, embedded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report.model_dump(exclude=set() if args.details else {"findings"}), indent=2) + "\n"
        )
    print(
        json.dumps(
            {
                "files": report.files,
                "counts": report.counts,
                "reference_programs": len(report.reference_programs),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
