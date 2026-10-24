"""Inventory refactor candidates without pretending syntax findings prove behavior."""

import argparse
import ast
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
]
EXCLUDED = frozenset(
    {
        ".git",
        ".venv",
        "node_modules",
        "vendor",
        "upstream",
        "history",
        "testdata",
        "goldens",
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


class Inventory(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    files: int
    counts: dict[str, int]
    findings: list[Finding]
    areas: dict[str, dict[str, int]]
    modules: dict[str, dict[str, int]]
    symbols: dict[str, dict[str, int]]
    imports: dict[str, list[str]]
    cycles: list[list[str]]


class Scan(ast.NodeVisitor):
    def __init__(self, path: str) -> None:
        self.path = path
        self.scope: list[str] = []
        self.findings: list[Finding] = []

    def record(self, node: ast.expr | ast.stmt | ast.arg, kind: Kind, expression: str) -> None:
        self.findings.append(
            Finding(
                path=self.path,
                line=node.lineno,
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


def inventory(root: Path) -> Inventory:
    findings: list[Finding] = []
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
    areas: defaultdict[str, Counter[str]] = defaultdict(Counter)
    modules: defaultdict[str, Counter[str]] = defaultdict(Counter)
    symbols: defaultdict[str, Counter[str]] = defaultdict(Counter)
    for finding in findings:
        areas[finding.path.split("/")[0]][finding.kind] += 1
        modules[finding.path][finding.kind] += 1
        symbols[finding.path + ":" + finding.symbol][finding.kind] += 1
    graph, cycles = dependencies(root)
    return Inventory(
        files=count,
        counts=dict(sorted(Counter(item.kind for item in findings).items())),
        findings=findings,
        areas={key: dict(value) for key, value in sorted(areas.items())},
        modules={key: dict(value) for key, value in sorted(modules.items())},
        symbols={key: dict(value) for key, value in sorted(symbols.items())},
        imports=graph,
        cycles=cycles,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--details", action="store_true")
    args = parser.parse_args()
    report = inventory(args.root)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report.model_dump(exclude=set() if args.details else {"findings"}), indent=2) + "\n"
        )
    print(json.dumps({"files": report.files, "counts": report.counts}, sort_keys=True))


if __name__ == "__main__":
    main()
