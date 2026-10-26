"""Render Tank's PostgreSQL and ClickHouse database schemas as ER diagrams.

The model is parsed from src/tank/migrations/*.sql, src/tank/market_schema.py and
src/tank/clickhouse_market.py; no database is needed. Run after every schema change and
commit diagrams/tank_postgres_schema.{svg,png} and diagrams/tank_clickhouse_schema.{svg,png}.
"""

import argparse
import ast
import copy
import html
import re
import subprocess
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

TYPE_MAP = [
    (r"timestamp with time zone", "timestamptz"),
    (r"double precision", "float8"),
    (r"boolean", "bool"),
    (r"integer", "int4"),
    (r"bigint", "int8"),
    (r"smallint", "int2"),
    (r"real", "float4"),
    (r"character\((\d+)\)", r"char(\1)"),
]

TRIGGER_FLAGS = {
    "ops.protect_immutable": "immutable-guard",
    "ana.guard_decision_state_head": "revision-chain guard",
    "ops.forbid_change": "append-only",
    "ops.notify_capture": "NOTIFY tank_capture",
    "ops.stamp_job_lifecycle": "lifecycle stamp",
}


def norm_type(t: str) -> str:
    t = t.strip()
    for a, b in TYPE_MAP:
        t = re.sub("^" + a, b, t)
    return t


def split_top(body: str) -> list[str]:
    parts, depth, cur = [], 0, ""
    for ch in body:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return parts


def cols_of(text: str) -> list[str]:
    return [c.strip() for c in text.split(",")]


def statements(sql: str) -> Iterator[str]:
    sql = re.sub(r"CREATE FUNCTION.*?\$\$.*?\$\$;", "", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = sql.replace('"', "")
    for s in sql.split(";"):
        s = " ".join(s.split())
        if s:
            yield s


class SchemaRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


type SQLColumnName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]


class KeyConstraint(SchemaRecord):
    kind: Literal["PK", "UK"]
    columns: tuple[SQLColumnName, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_columns(self) -> Self:
        if len(set(self.columns)) != len(self.columns):
            raise ValueError("Constraint columns must be unique")
        return self


class ForeignKeyConstraint(SchemaRecord):
    kind: Literal["FK"]
    columns: tuple[SQLColumnName, ...] = Field(min_length=1)
    target_table: Annotated[str, StringConstraints(pattern=r"^[A-Za-z_]\w*\.[A-Za-z_]\w*$")]
    target_columns: tuple[SQLColumnName, ...] = Field(min_length=1)
    options: str

    @model_validator(mode="after")
    def matching_scope(self) -> Self:
        if len(self.columns) != len(self.target_columns):
            raise ValueError("Foreign key source and target arity must match")
        if len(set(self.columns)) != len(self.columns) or len(set(self.target_columns)) != len(
            self.target_columns
        ):
            raise ValueError("Foreign key columns must be unique")
        return self


class CheckConstraint(SchemaRecord):
    kind: Literal["CK", "REQ"]
    columns: tuple[SQLColumnName, ...]
    valid: bool

    @model_validator(mode="after")
    def required_target(self) -> Self:
        if self.kind == "REQ" and len(self.columns) != 1:
            raise ValueError("Required-column checks must name exactly one target")
        return self


class ForeignKeyLink(SchemaRecord):
    name: str = Field(min_length=1)
    source_table: str
    source_columns: tuple[SQLColumnName, ...] = Field(min_length=1)
    target_table: str
    target_columns: tuple[SQLColumnName, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def matching_scope(self) -> Self:
        if len(self.source_columns) != len(self.target_columns):
            raise ValueError("Foreign key link source and target arity must match")
        return self


type Constraint = KeyConstraint | ForeignKeyConstraint | CheckConstraint


def key_columns(statement: str) -> list[str]:
    match = re.search(r"\((.*?)\)", statement)
    if match is None:
        raise ValueError("Unsupported key constraint syntax")
    return cols_of(match.group(1))


class Table:
    constraints: dict[str, Constraint]

    def __init__(self, name):
        self.name = name
        self.schema = name.split(".")[0]
        self.cols = []
        self.constraints = {}
        self.unique_indexes = []
        self.indexes = 0
        self.triggers = []
        self.unlogged = False
        self.storage = None
        self.note = None

    def col(self, name):
        for c in self.cols:
            if c["name"] == name:
                return c
        raise KeyError(f"{self.name}.{name}")


def new_col(name, typ):
    return {"name": name, "type": norm_type(typ), "nn": False, "default": None, "prec": False, "ref": None}


def parse_col_def(t, p):
    name, rest = p.split(" ", 1)
    m = re.search(r" (NOT NULL|DEFAULT|PRIMARY KEY|UNIQUE|CONSTRAINT|CHECK|REFERENCES)\b", " " + rest)
    typ = (" " + rest)[: m.start()].strip() if m else rest
    c = new_col(name, typ)
    tail = rest[len(typ) :]
    if "NOT NULL" in tail:
        c["nn"] = True
    d = re.search(r"DEFAULT (\S+)", tail)
    if d:
        c["default"] = d.group(1)
    if "PRIMARY KEY" in tail:
        t.constraints[f"{t.name}_pkey"] = KeyConstraint(kind="PK", columns=(name,))
    t.cols.append(c)


def parse_constraint(t: Table, text: str, name: str | None = None) -> None:
    if name is not None and re.fullmatch(r"[A-Za-z_]\w*", name) is None:
        raise ValueError("Unsupported explicit constraint name")
    m = re.match(r"CONSTRAINT (\w+) (.*)$", text)
    if m:
        name, text = m.group(1), m.group(2)
    if text.startswith("PRIMARY KEY"):
        cols = key_columns(text)
        t.constraints[name if name is not None else f"{t.name}_pkey"] = KeyConstraint(
            kind="PK", columns=tuple(cols)
        )
    elif text.startswith("UNIQUE"):
        cols = key_columns(text)
        t.constraints[name if name is not None else f"{t.name}_{'_'.join(cols)}_key"] = KeyConstraint(
            kind="UK", columns=tuple(cols)
        )
    elif text.startswith("FOREIGN KEY"):
        m = re.fullmatch(r"FOREIGN KEY\s*\((.*?)\)\s+REFERENCES\s+(\w+\.\w+)\s*\((.*?)\)(.*)", text)
        if m is None:
            raise ValueError("Unsupported foreign key constraint syntax")
        if name is None:
            raise ValueError("Diagram foreign keys require an explicit constraint name")
        t.constraints[name] = ForeignKeyConstraint(
            kind="FK",
            columns=tuple(cols_of(m.group(1))),
            target_table=m.group(2),
            target_columns=tuple(cols_of(m.group(3))),
            options=m.group(4).strip(),
        )
    elif text.startswith("CHECK"):
        expr = text[len("CHECK") :].strip()
        valid = not expr.endswith("NOT VALID")
        names = {c["name"] for c in t.cols}
        if name and name.startswith("required_"):
            target = name[len("required_") :]
            if target not in names:
                found = re.search(r"\b(\w+) IS NOT NULL", expr)
                target = found.group(1) if found else None
            if target in names:
                t.constraints[name] = CheckConstraint(kind="REQ", columns=(target,), valid=valid)
                return
        used = [w for w in dict.fromkeys(re.findall(r"\b[a-z_][a-z0-9_]*\b", expr)) if w in names]
        t.constraints[name if name is not None else f"check_{len(t.constraints)}"] = CheckConstraint(
            kind="CK", columns=tuple(used), valid=valid
        )


def drop_column(t, name):
    """Remove a column with the constraints and indexes that use it, as PostgreSQL does."""
    t.cols = [c for c in t.cols if c["name"] != name]
    t.constraints = {k: v for k, v in t.constraints.items() if name not in v.columns}
    kept = [u for u in t.unique_indexes if name not in u[1]]
    t.indexes -= len(t.unique_indexes) - len(kept)
    t.unique_indexes = kept


def parse_pg(sources):
    tables, schemas, owners = {}, [], {}
    for source in sources:
        for s in statements(source):
            if m := re.match(r"CREATE SCHEMA (IF NOT EXISTS )?(\w+)", s):
                schemas.append(m.group(2))
            elif m := re.match(r"DROP TABLE (IF EXISTS )?(\w+\.\w+)$", s):
                tables.pop(m.group(2), None)
            elif m := re.match(r"DROP INDEX (IF EXISTS )?(\w+)\.(\w+)$", s):
                owner = owners.pop((m.group(2), m.group(3)), None)
                if owner in tables:
                    t = tables[owner]
                    t.indexes -= 1
                    t.unique_indexes = [u for u in t.unique_indexes if u[0] != m.group(3)]
            elif m := re.match(
                r"CREATE (UNLOGGED )?TABLE (IF NOT EXISTS )?(\w+\.\w+) \((.*?)\)( WITH \((.*)\))?$", s
            ):
                t = tables.setdefault(m.group(3), Table(m.group(3)))
                t.unlogged = bool(m.group(1))
                t.storage = m.group(6)
                for part in split_top(m.group(4)):
                    if re.match(r"(CONSTRAINT|UNIQUE|PRIMARY KEY) ", part):
                        parse_constraint(t, part)
                    else:
                        parse_col_def(t, part)
            elif (m := re.match(r"ALTER TABLE (ONLY )?(\w+\.\w+) (DROP COLUMN .*)$", s)) and m.group(
                2
            ) in tables:
                for action in split_top(m.group(3)):
                    if a := re.match(r"DROP COLUMN (IF EXISTS )?(\w+)$", action):
                        drop_column(tables[m.group(2)], a.group(2))
            elif m := re.match(r"ALTER TABLE (ONLY )?(\w+\.\w+) (.*)$", s):
                t, action = tables[m.group(2)], m.group(3)
                if a := re.match(r"ADD CONSTRAINT (.*)$", action):
                    parse_constraint(t, "CONSTRAINT " + a.group(1))
                elif a := re.match(r"ADD COLUMN (IF NOT EXISTS )?(.*)$", action):
                    parse_col_def(t, a.group(2))
                elif a := re.match(r"DROP CONSTRAINT (IF EXISTS )?(\w+)", action):
                    t.constraints.pop(a.group(2), None)
                elif a := re.match(r"ALTER COLUMN (\w+) SET DEFAULT nextval", action):
                    t.col(a.group(1))["default"] = "nextval"
            elif m := re.match(
                r"CREATE (UNIQUE )?INDEX (IF NOT EXISTS )?(\w+) ON (\w+\.\w+)( USING (\w+))? ?\((.*?)\)( WHERE .*)?$",
                s,
            ):
                t = tables[m.group(4)]
                t.indexes += 1
                owners[(m.group(4).split(".")[0], m.group(3))] = m.group(4)
                if m.group(1):
                    t.unique_indexes.append((m.group(3), cols_of(m.group(7)), bool(m.group(8))))
            elif m := re.match(r"CREATE TRIGGER (\w+) .* ON (\w+\.\w+) .* FUNCTION ([\w.]+)\(", s):
                tables[m.group(2)].triggers.append(m.group(3))
    return tables, schemas


def collapse_prec(t):
    names = {c["name"] for c in t.cols}
    drop = set()
    for c in t.cols:
        if c["type"] != "timestamptz":
            continue
        bases = [c["name"]] + ([c["name"][:-3]] if c["name"].endswith("_at") else [])
        for b in bases:
            pair = {b + "_prec", b + "_prec_reason"} & names
            if pair:
                drop |= pair
                c["prec"] = True
                break
    t.cols = [c for c in t.cols if c["name"] not in drop]


LOGICAL = {
    "event_id": "evt.events",
    "route_event_id": "evt.events",
    "token_id": "reg.tokens",
    "instrument_id": "reg.instruments",
    "entity_id": "reg.entities",
    "source_id": "reg.sources",
    "current_ems_id": "evt.ems_assessments",
    "opened_by_alert": "out.alerts",
    "evidence_bundle_id": "out.evidence_bundles",
    "question_id": "out.user_questions",
}
SPECIFIC = {
    ("ana.decisions", "call_id"): "ana.calls",
    ("evt.state_changes", "edge_id"): "evt.beneficiary_edges",
    ("evt.beneficiary_edges", "map_version"): "evt.beneficiary_maps",
    ("out.ops_cards", "item_id"): "ingest.source_items",
    ("out.user_questions", "session_id"): "ops.agent_sessions",
    ("ops.telegram_files", "session_id"): "ops.agent_sessions",
    ("out.alerts", "decisions"): "ana.decisions[]",
}


def finish_pg(tables):
    t = Table("ops.schema_migrations")
    t.cols = [new_col("version", "text"), new_col("applied_at", "timestamptz")]
    t.cols[1]["nn"] = True
    t.cols[1]["default"] = "now()"
    t.constraints["schema_migrations_pkey"] = KeyConstraint(kind="PK", columns=("version",))
    t.note = "created by storage.py migrate()"
    tables[t.name] = t
    for t in tables.values():
        collapse_prec(t)
        for c in t.cols:
            c.update(pk=False, fk=False, uk=None, ck=False, req=False)
        groups = [
            list(constraint.columns) for constraint in t.constraints.values() if constraint.kind == "UK"
        ]
        groups += [cols for _, cols, _ in t.unique_indexes]
        t.groups = []
        for cols in groups:
            if cols not in t.groups:
                t.groups.append(cols)
        composite = [g for g in t.groups if len(g) > 1]
        present = {c["name"] for c in t.cols}
        for constraint in t.constraints.values():
            kind, cols = constraint.kind, constraint.columns
            for n in cols:
                if n not in present:
                    continue
                c = t.col(n)
                if kind == "PK":
                    c["pk"] = True
                    c["nn"] = True
                elif kind == "FK":
                    c["fk"] = True
                elif kind == "CK":
                    c["ck"] = True
                elif kind == "REQ" and not c["nn"]:
                    c["req"] = True
        for g in t.groups:
            label = f"U{composite.index(g) + 1}" if len(g) > 1 else "UK"
            for n in g:
                c = t.col(n)
                if not (c["pk"] and len(g) == 1):
                    c["uk"] = label if c["uk"] is None else c["uk"]
        own = {g[0] for g in t.groups if len(g) == 1}
        for c in t.cols:
            target = SPECIFIC.get((t.name, c["name"])) or LOGICAL.get(c["name"])
            if c["fk"] or not target or target == t.name:
                continue
            if c["name"] in own and c["name"] in ("source_id", "entity_id", "token_id", "instrument_id"):
                continue
            c["ref"] = target
        t.flags = list(dict.fromkeys(TRIGGER_FLAGS[f] for f in t.triggers))
        if t.unlogged:
            t.flags.append("UNLOGGED")
        if t.storage:
            t.flags.append(t.storage.replace("'", ""))
        if not any(v.kind == "PK" for v in t.constraints.values()):
            t.flags.append("no PK")
    return tables


def pg_model_at(root, ref):
    """The finished PostgreSQL model of the migrations committed at a git ref."""

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
        ).stdout

    names = sorted(
        n for n in git("ls-tree", "--name-only", ref, "src/tank/migrations/").split() if n.endswith(".sql")
    )
    tables, _ = parse_pg(git("show", f"{ref}:{n}") for n in names)
    return finish_pg(tables)


def diff_tables(base, tables):
    """The tables to draw for a schema diff against base, each with its change.

    A removed column or table stays in place struck through, an added or altered one is marked;
    change is None for a table that did not change."""
    drawn = {}
    for name, t in tables.items():
        d = copy.copy(t)
        old = base.get(name)
        if old is None:
            d.cols = [dict(c, change="added") for c in t.cols]
            d.change, d.idx_was = "added", None
            drawn[name] = d
            continue
        now = {c["name"]: c for c in t.cols}
        was = {c["name"]: c for c in old.cols}
        cols = []
        for c in old.cols:
            if c["name"] not in now:
                cols.append(dict(c, change="removed", ref=None))
            elif (c["type"], c["nn"]) != (now[c["name"]]["type"], now[c["name"]]["nn"]):
                cols.append(dict(now[c["name"]], change="altered"))
            else:
                cols.append(now[c["name"]])
        cols += [dict(c, change="added") for c in t.cols if c["name"] not in was]
        d.cols = cols
        d.idx_was = index_count(old) if index_count(old) != index_count(t) else None
        altered = d.idx_was is not None or old.flags != t.flags or any(c.get("change") for c in cols)
        d.change = "altered" if altered else None
        drawn[name] = d
    removed = {}
    for name, old in base.items():
        if name not in tables:
            d = copy.copy(old)
            d.cols = [dict(c, change="removed", ref=None) for c in old.cols]
            d.change, d.idx_was = "removed", None
            removed[name] = d
    return drawn, removed


def foreign_keys(tables: Mapping[str, Table]) -> list[ForeignKeyLink]:
    out: list[ForeignKeyLink] = []
    for table in tables.values():
        for name, constraint in table.constraints.items():
            if isinstance(constraint, ForeignKeyConstraint):
                out.append(
                    ForeignKeyLink(
                        name=name,
                        source_table=table.name,
                        source_columns=constraint.columns,
                        target_table=constraint.target_table,
                        target_columns=constraint.target_columns,
                    )
                )
    return out


def literal_assignments(path, names):
    tree = ast.parse(Path(path).read_text())
    found = {}
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target, value = node.target, node.value
        elif (
            isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
        ):
            target, value = node.targets[0], node.value
        else:
            continue
        if target.id in names:
            with suppress(ValueError):
                found[target.id] = ast.literal_eval(value)
    return found


def ch_model(root):
    schema = literal_assignments(root / "src/tank/market_schema.py", {"MARKET_COLUMNS", "MARKET_IDENTITIES"})
    chm = literal_assignments(
        root / "src/tank/clickhouse_market.py",
        {
            "MARKET_TABLES",
            "TRADES",
            "TRADE_COLUMNS",
            "TYPES",
            "ZSTD",
            "TIME_CODEC",
            "VERSION_CODEC",
            "TRADES_TTL_DAYS",
        },
    )
    codec_of = {"timestamptz": chm["TIME_CODEC"], "boolean": "", "bigint": "CODEC(Delta, ZSTD(3))"}
    tables = []
    for name, part in chm["MARKET_TABLES"].items():
        ident = list(schema["MARKET_IDENTITIES"][name])
        cols = [{"name": "storage_key", "type": "String", "codec": "", "kind": "text"}]
        for n, kind in schema["MARKET_COLUMNS"][name].items():
            ch = chm["TYPES"][kind]
            if n not in ident and kind != "text[]":
                ch = f"Nullable({ch})"
            cols.append({"name": n, "type": ch, "codec": codec_of.get(kind, chm["ZSTD"]), "kind": kind})
        cols.append({"name": "_version", "type": "UInt64", "codec": chm["VERSION_CODEC"], "kind": "version"})
        tables.append(
            {
                "name": name,
                "partition": part,
                "order": ident,
                "cols": cols,
                "indexes": [
                    ("storage_key_bf", "storage_key", "bloom_filter", 4),
                    ("recorded_at_mm", "recorded_at", "minmax", 1),
                ],
            }
        )
    cols = []
    for n, kind in chm["TRADE_COLUMNS"].items():
        codec = chm["TIME_CODEC"] if "DateTime64" in kind else "" if "LowCardinality" in kind else chm["ZSTD"]
        cols.append({"name": n, "type": kind, "codec": codec, "kind": "native"})
    cols.append({"name": "_version", "type": "UInt64", "codec": chm["VERSION_CODEC"], "kind": "version"})
    tables.append(
        {
            "name": chm["TRADES"],
            "partition": "ts",
            "order": ["instrument_id", "ts", "trade_id"],
            "cols": cols,
            "indexes": [],
        }
    )
    for t in tables:
        names = {c["name"] for c in t["cols"]}
        drop = set()
        for c in t["cols"]:
            c["prec"] = False
            if "DateTime64" in c["type"]:
                pair = {c["name"] + "_prec", c["name"] + "_prec_reason"} & names
                if pair:
                    drop |= pair
                    c["prec"] = True
        t["cols"] = [c for c in t["cols"] if c["name"] not in drop]
    return tables, chm


MONO = "Menlo, 'DejaVu Sans Mono', monospace"
DIFF_RED = "#E00000"
DIFF_WASH = "#FDE3E3"
SANS = "'Helvetica Neue', Helvetica, Arial, sans-serif"
CW = 0.602
ROW = 17
HEAD = 23
SUB = 15
GAP = 26
INK = "#1A202C"
MUTED = "#718096"
FAINT = "#A0AEC0"
LINE = "#CBD5E0"

SCHEMA = {
    "reg": ("#4A5568", "#EDF2F7", "registry: tokens, instruments, entities, sources"),
    "ingest": ("#2B6CB0", "#EBF4FF", "incoming source items"),
    "evt": ("#2F855A", "#F0FFF4", "events, observations, claims, assessments"),
    "ana": ("#6B46C1", "#FAF5FF", "analysis: market truth, decisions, calls"),
    "out": ("#C05621", "#FFFAF0", "output: alerts, deliveries, notices"),
    "ops": ("#2C7A7B", "#E6FFFA", "operations: agent, jobs, budgets, Telegram"),
    "mkt": ("#B7791F", "#FFFFF0", "market capture control (series live in ClickHouse)"),
    "qa": ("#97266D", "#FFF5F7", "fixtures, replays, reviews"),
    "scout": ("#718096", "#F7FAFC", "expansion proposals"),
}

BADGE = {
    "PK": ("#F6E05E", "#744210"),
    "FK": ("#BEE3F8", "#2A4365"),
    "UK": ("#E9D8FD", "#44337A"),
    "U1": ("#E9D8FD", "#44337A"),
    "U2": ("#E9D8FD", "#44337A"),
    "CK": ("#FED7D7", "#9B2C2C"),
    "SEQ": ("#C6F6D5", "#22543D"),
    "SK1": ("#F6E05E", "#744210"),
    "SK2": ("#F6E05E", "#744210"),
    "SK3": ("#F6E05E", "#744210"),
    "PT": ("#BEE3F8", "#2A4365"),
    "VER": ("#C6F6D5", "#22543D"),
    "BF": ("#E9D8FD", "#44337A"),
    "MM": ("#E9D8FD", "#44337A"),
}


def esc(s: str) -> str:
    return html.escape(s, quote=True)


def tw(s: str, size: float) -> float:
    return len(s) * CW * size


def badge(x: float, y: float, label: str) -> tuple[str, float]:
    fill, ink = BADGE[label]
    w = 6 + len(label) * 5.2
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="11" rx="2.5" fill="{fill}"/>'
        f'<text x="{x + w / 2:.1f}" y="{y + 8.3:.1f}" font-family="{SANS}" font-size="7.6" font-weight="700" '
        f'fill="{ink}" text-anchor="middle">{label}</text>',
        w,
    )


def rounded(points: Sequence[tuple[float, float]], r: float = 6) -> str:
    if len(points) < 2:
        raise ValueError("A diagram route requires at least two points")
    d = f"M{points[0][0]:.1f},{points[0][1]:.1f}"
    for i in range(1, len(points) - 1):
        (x0, y0), (x1, y1), (x2, y2) = points[i - 1], points[i], points[i + 1]
        d1 = max(abs(x1 - x0), abs(y1 - y0))
        d2 = max(abs(x2 - x1), abs(y2 - y1))
        rr = min(r, d1 / 2, d2 / 2)
        if rr < 0.5:
            d += f" L{x1:.1f},{y1:.1f}"
            continue
        ax = x1 - (x1 - x0) / d1 * rr if d1 else x1
        ay = y1 - (y1 - y0) / d1 * rr if d1 else y1
        bx = x1 + (x2 - x1) / d2 * rr if d2 else x1
        by = y1 + (y2 - y1) / d2 * rr if d2 else y1
        d += f" L{ax:.1f},{ay:.1f} Q{x1:.1f},{y1:.1f} {bx:.1f},{by:.1f}"
    d += f" L{points[-1][0]:.1f},{points[-1][1]:.1f}"
    return d


def end_marker(x: float, y: float, s: Literal[-1, 1], kind: Literal["one", "many"], color: str) -> str:
    """IE end marker on a table edge at (x, y); s=+1 points right (away from a right edge)."""
    out = []
    if kind == "many":
        tip = x + s * 11
        out.append(
            f'<path d="M{tip:.1f},{y:.1f} L{x:.1f},{y - 5.5:.1f} M{tip:.1f},{y:.1f} L{x:.1f},{y:.1f} '
            f'M{tip:.1f},{y:.1f} L{x:.1f},{y + 5.5:.1f}" stroke="{color}" stroke-width="1.3" fill="none"/>'
        )
        cx = x + s * 16
    else:
        bx = x + s * 7
        out.append(
            f'<path d="M{bx:.1f},{y - 5.5:.1f} L{bx:.1f},{y + 5.5:.1f}" stroke="{color}" stroke-width="1.5"/>'
        )
        cx = x + s * 13
    out.append(
        f'<circle cx="{cx:.1f}" cy="{y:.1f}" r="3.1" fill="#fff" stroke="{color}" stroke-width="1.3"/>'
    )
    return "".join(out)


class Box:
    """A positioned table box with per-row anchors."""

    def __init__(self, name, x, y, w, rows, head_h):
        self.name, self.x, self.y, self.w = name, x, y, w
        self.rows = rows
        self.head_h = head_h
        self.h = head_h + len(rows) * ROW + 3

    def row_y(self, col):
        i = [r["name"] for r in self.rows].index(col)
        return self.y + self.head_h + i * ROW + ROW / 2


def index_count(t):
    return t.indexes + sum(1 for v in t.constraints.values() if v.kind in ("PK", "UK"))


def pg_metrics(tables):
    name = max(len(c["name"]) for t in tables for c in t.cols)
    typ = max(len(c["type"]) for t in tables for c in t.cols)
    ref = max([len(c["ref"]) + 2 for t in tables for c in t.cols if c["ref"]] or [0])
    name_x = 50
    type_x = name_x + name * CW * 10.8 + 14
    prec_x = type_x + typ * CW * 10 + 4
    nn_x = prec_x + 27
    ref_x = nn_x + 21
    width = (ref_x + ref * CW * 9.2 + 10) if ref else nn_x + 22
    return {"name_x": name_x, "type_x": type_x, "prec_x": prec_x, "nn_x": nn_x, "ref_x": ref_x, "w": width}


def pg_table(t, x, y, w, mx):
    fill, tint, _ = SCHEMA[t.schema]
    change = getattr(t, "change", "")
    if change == "removed":
        fill, tint = DIFF_RED, DIFF_WASH
    schema, short = t.name.split(".")
    box = Box(t.name, x, y, w, t.cols, HEAD + SUB)
    fade = ' opacity="0.3"' if change is None else ""
    out = [f'<g id="{esc(t.name)}"{fade}>']
    out.append(
        f'<rect x="{x + 2}" y="{y + 3}" width="{w}" height="{box.h}" rx="5" fill="#000" opacity="0.07"/>'
    )
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{box.h}" rx="5" fill="#fff" stroke="{fill}" stroke-width="1.2"/>'
    )
    out.append(
        f'<path d="M{x},{y + 5} a5,5 0 0 1 5,-5 h{w - 10} a5,5 0 0 1 5,5 v{HEAD - 5} h{-w} z" fill="{fill}"/>'
    )
    out.append(
        f'<text x="{x + 9}" y="{y + 15.5}" font-family="{SANS}" font-size="12.5" fill="#fff">'
        f'<tspan opacity="0.72">{esc(schema)}.</tspan><tspan font-weight="700">{esc(short)}</tspan></text>'
    )
    out.append(f'<rect x="{x + 0.6}" y="{y + HEAD}" width="{w - 1.2}" height="{SUB}" fill="{tint}"/>')
    flags = " · ".join(t.flags) if t.flags else ""
    note = "removed" if change == "removed" else "added" if change == "added" else t.note or ""
    if change in ("removed", "added"):
        flags = ""
    out.append(
        f'<text x="{x + 9}" y="{y + HEAD + 10.6}" font-family="{SANS}" font-size="8.6" fill="{fill}" font-weight="600">'
        f"{esc(flags or note)}</text>"
    )
    n_idx = index_count(t)
    idx = f"{n_idx} idx" if n_idx else "no idx"
    idx_ink = MUTED
    if getattr(t, "idx_was", None) is not None:
        idx, idx_ink = f"{t.idx_was} → {n_idx} idx", DIFF_RED
    out.append(
        f'<text x="{x + w - 8}" y="{y + HEAD + 10.6}" font-family="{SANS}" font-size="8.6" fill="{idx_ink}" '
        f'{'font-weight="700" ' if idx_ink == DIFF_RED else ""}text-anchor="end">{idx}</text>'
    )
    for i, c in enumerate(t.cols):
        ry = y + HEAD + SUB + i * ROW
        row_change = c.get("change")
        if row_change:
            out.append(f'<rect x="{x + 0.6}" y="{ry}" width="{w - 1.2}" height="{ROW}" fill="{DIFF_WASH}"/>')
        elif i % 2:
            out.append(f'<rect x="{x + 0.6}" y="{ry}" width="{w - 1.2}" height="{ROW}" fill="#F7FAFC"/>')
        bx = x + 6
        labels = []
        if c["pk"]:
            labels.append("PK")
        if c["fk"]:
            labels.append("FK")
        if c["uk"]:
            labels.append(c["uk"])
        if c["ck"]:
            labels.append("CK")
        if c["default"] == "nextval":
            labels.append("SEQ")
        for lab in labels[:3]:
            g, bw = badge(bx, ry + 3, lab)
            out.append(g)
            bx += bw + 2
        envelope = c["name"] in ("document", "immutable") and t.cols[0]["name"] == "storage_key"
        ink = DIFF_RED if row_change else FAINT if envelope else INK
        weight = ' font-weight="700"' if c["pk"] else ""
        nx = max(x + mx["name_x"], bx + 3)
        out.append(
            f'<text x="{nx:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10.8" fill="{ink}"{weight}>{esc(c["name"])}</text>'
        )
        out.append(
            f'<text x="{x + mx["type_x"]:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10" '
            f'fill="{DIFF_RED if row_change else FAINT if envelope else MUTED}">{esc(c["type"])}</text>'
        )
        if row_change == "removed":
            end = x + mx["type_x"] + tw(c["type"], 10)
            out.append(
                f'<path d="M{nx - 2:.1f},{ry + 8.5} H{end + 2:.1f}" stroke="{DIFF_RED}" stroke-width="1.2"/>'
            )
        if c["prec"]:
            px = x + mx["type_x"] + tw(c["type"], 10) + 4
            out.append(
                f'<text x="{px:.1f}" y="{ry + 11.8}" font-family="{SANS}" font-size="8" fill="#DD6B20" font-weight="600">+prec</text>'
            )
        if c["nn"]:
            out.append(
                f'<text x="{x + mx["nn_x"]:.1f}" y="{ry + 11.8}" font-family="{SANS}" font-size="8" '
                f'fill="{FAINT if envelope else "#2D3748"}" font-weight="700">NN</text>'
            )
        elif c["req"]:
            out.append(
                f'<text x="{x + mx["nn_x"]:.1f}" y="{ry + 11.8}" font-family="{SANS}" font-size="8" fill="{FAINT}" font-weight="600">nn</text>'
            )
        if c["ref"]:
            out.append(
                f'<text x="{x + mx["ref_x"]:.1f}" y="{ry + 12}" font-family="{MONO}" font-size="9.2" fill="#3182CE" '
                f'font-style="italic">→ {esc(c["ref"])}</text>'
            )
    out.append("</g>")
    return box, "".join(out)


PG_COLUMNS = [
    [
        "reg.sources",
        "reg.tokens",
        "reg.instruments",
        "reg.entities",
        "reg.entity_aliases",
        "reg.exchange_addresses",
        "reg.sector_baskets",
    ],
    ["ingest.source_items", "qa.reviews", "scout.expansion_proposals"],
    [
        "evt.event_observations",
        "evt.claims",
        "ops.agent_sessions",
        "ops.tool_calls",
        "ops.telegram_messages",
        "ops.telegram_files",
        "ops.telegram_file_payloads",
    ],
    [
        "evt.events",
        "evt.ems_assessments",
        "evt.state_changes",
        "evt.beneficiary_maps",
        "evt.beneficiary_edges",
        "evt.capture_chains",
    ],
    [
        "ana.market_truth",
        "ana.decisions",
        "ana.decision_state_heads",
        "ana.calls",
        "ana.contradictions",
        "ana.watch_conditions",
    ],
    [
        "out.alerts",
        "out.deliveries",
        "out.ops_notices",
        "out.evidence_bundles",
        "out.user_questions",
        "out.reaction_checkpoints",
        "out.ops_cards",
    ],
    [
        "ops.jobs",
        "ops.llm_usage",
        "ops.capture_tier_transitions",
        "ops.scheduler_checkpoints",
        "ops.source_checkpoints",
        "ops.source_escalations",
        "ops.request_budget_blocks",
        "ops.request_budget_ledger",
        "ops.schema_migrations",
    ],
    ["mkt.hot_tokens", "mkt.capture_requests", "mkt.capture_streams", "mkt.capture_gaps"],
    ["mkt.backfill_runs", "mkt.unlocks", "mkt.supply_obs"],
]

PG_GUTTERS = {0: 46, 1: 84, 2: 96, 3: 84, 4: 64, 5: 64, 6: 46, 7: 46}

PG_ROUTES = {
    "decision_state_heads_current_scope_fk": {"side": "R", "lanes": [1]},
    "decision_state_heads_previous_scope_fk": {"side": "R", "lanes": [2]},
    "session_origin": {"lanes": [0]},
    "observation_item": {"lanes": [1]},
    "claim_observation": {"side": "L", "lanes": [2]},
    "tool_session": {"side": "L", "lanes": [3]},
    "observation_event": {"lanes": [0]},
    "session_event": {"lanes": [1], "one_to_one": True},
    "change_event": {"side": "L", "lanes": [2]},
    "assessment_event": {"side": "L", "lanes": [3]},
    "event_merge_target": {"side": "L", "lanes": [4]},
    "call_event": {"lanes": [0]},
    "decision_event": {"lanes": [1]},
    "truth_event": {"lanes": [2]},
    "alert_event": {"lanes": [1, 3]},
    "decision_truth": {"side": "R", "lanes": [0]},
    "delivery_alert": {"side": "R", "lanes": [0]},
    "delivery_notice": {"side": "R", "lanes": [1]},
}
LANES = {0: 1, 1: 4, 2: 5, 3: 4, 4: 2, 5: 2}


def route(fk, boxes, col_of, col_x, col_w, gut, channel_y, spec, color):
    src, dst = boxes[fk.source_table], boxes[fk.target_table]
    ci, cj = col_of[fk.source_table], col_of[fk.target_table]
    sy, dy = src.row_y(fk.source_columns[0]), dst.row_y(fk.target_columns[0])

    def lane_x(g, k):
        left = col_x[g] + col_w[g]
        width = gut[g]
        n = LANES.get(g, 1)
        return left + width * (k + 1) / (n + 1)

    lanes = spec["lanes"]
    if ci == cj:
        side = spec["side"]
        g = ci - 1 if side == "L" else ci
        lx = lane_x(g, lanes[0])
        sx = src.x if side == "L" else src.x + src.w
        dx = dst.x if side == "L" else dst.x + dst.w
        pts = [(sx, sy), (lx, sy), (lx, dy), (dx, dy)]
        s_dir = d_dir = -1 if side == "L" else 1
    elif abs(ci - cj) == 1:
        g = min(ci, cj)
        lx = lane_x(g, lanes[0])
        if ci < cj:
            sx, dx, s_dir, d_dir = src.x + src.w, dst.x, 1, -1
        else:
            sx, dx, s_dir, d_dir = src.x, dst.x + dst.w, -1, 1
        pts = [(sx, sy), (lx, sy), (lx, dy), (dx, dy)]
    else:
        if ci > cj:
            g1, g2 = ci - 1, cj
            sx, dx, s_dir, d_dir = src.x, dst.x + dst.w, -1, 1
        else:
            g1, g2 = ci, cj - 1
            sx, dx, s_dir, d_dir = src.x + src.w, dst.x, 1, -1
        l1, l2 = lane_x(g1, lanes[0]), lane_x(g2, lanes[1])
        pts = [(sx, sy), (l1, sy), (l1, channel_y), (l2, channel_y), (l2, dy), (dx, dy)]
    child = "one" if spec.get("one_to_one") else "many"
    parts = [
        f'<path d="{rounded(pts)}" fill="none" stroke="{color}" stroke-width="1.35" stroke-linejoin="round" opacity="0.92"/>',
        end_marker(pts[0][0], pts[0][1], s_dir, child, color),
        end_marker(pts[-1][0], pts[-1][1], d_dir, "one", color),
    ]
    if len(fk.source_columns) > 1:
        for c in fk.source_columns[1:]:
            y2 = src.row_y(c)
            x2 = pts[0][0] + s_dir * 22
            parts.append(
                f'<path d="M{pts[0][0]:.1f},{y2:.1f} H{x2:.1f} V{sy:.1f}" fill="none" stroke="{color}" stroke-width="1.1" stroke-dasharray="2 2"/>'
            )
    return "".join(parts)


def legend_line(x: float, y: float, kind: Literal["fk", "11", "logical"], color: str = "#2D3748") -> str:
    out = [f'<path d="M{x},{y} H{x + 70}" stroke="{color}" stroke-width="1.35"/>']
    if kind == "fk":
        out.append(end_marker(x, y, 1, "many", color))
        out.append(end_marker(x + 70, y, -1, "one", color))
    elif kind == "11":
        out.append(end_marker(x, y, 1, "one", color))
        out.append(end_marker(x + 70, y, -1, "one", color))
    return "".join(out)


def text(
    x: float,
    y: float,
    s: str,
    size: float = 11,
    fill: str = INK,
    weight: Literal[100, 200, 300, 400, 500, 600, 700, 800, 900] | None = None,
    family: str = SANS,
    anchor: Literal["start", "middle", "end"] | None = None,
    style: Literal["normal", "italic", "oblique"] | None = None,
) -> str:
    attrs = f' font-weight="{weight}"' if weight is not None else ""
    attrs += f' text-anchor="{anchor}"' if anchor is not None else ""
    attrs += f' font-style="{style}"' if style is not None else ""
    return f'<text x="{x:.1f}" y="{y:.1f}" font-family="{family}" font-size="{size}" fill="{fill}"{attrs}>{s}</text>'


REF_TARGET = {
    "reg.tokens": "token_id",
    "reg.instruments": "instrument_id",
    "reg.entities": "entity_id",
    "reg.sources": "source_id",
    "evt.events": "event_id",
    "evt.ems_assessments": "ems_id",
    "out.alerts": "alert_id",
    "out.evidence_bundles": "bundle_id",
    "out.user_questions": "question_id",
    "ana.calls": "call_id",
    "evt.beneficiary_edges": "edge_id",
    "evt.beneficiary_maps": "map_version",
    "ingest.source_items": "item_id",
    "ops.agent_sessions": "session_id",
    "ana.decisions": "dec_id",
}
COMPOSITE = {
    ("ana.calls", "opened_by_alert"): (
        ["opened_by_alert", "opened_by_version"],
        "out.alerts",
        ["alert_id", "version"],
    ),
    ("evt.beneficiary_edges", "map_version"): (
        ["event_id", "map_version"],
        "evt.beneficiary_maps",
        ["event_id", "map_version"],
    ),
}
DESIGN_FKS = []
DESIGN_SERIAL_PKS = [("evt.state_changes", "id"), ("out.deliveries", "id"), ("ops.llm_usage", "id")]


def integrity_gaps(tables, fks):
    """Constraints the DDL leaves to application code, computed from the parsed schema."""
    single = {}
    for t in tables.values():
        keys = {g[0] for g in t.groups if len(g) == 1}
        keys |= {
            constraint.columns[0]
            for constraint in t.constraints.values()
            if constraint.kind == "PK" and len(constraint.columns) == 1
        }
        single[t.name] = keys
    declared = {(f.source_table, f.source_columns[0]) for f in fks}
    by_target, composite, blocked = {}, [], []
    for t in tables.values():
        for c in t.cols:
            if not c["ref"]:
                continue
            if (t.name, c["name"]) in COMPOSITE:
                cols, dst, dcols = COMPOSITE[(t.name, c["name"])]
                assert dcols in tables[dst].groups
                composite.append(f"{t.name}({', '.join(cols)}) → {dst}")
                continue
            dst = c["ref"].rstrip("[]")
            if c["ref"].endswith("[]") or REF_TARGET[dst] not in single[dst]:
                blocked.append(f"{t.name}.{c['name']}")
                continue
            by_target[dst] = by_target.get(dst, 0) + 1
    missing_design = [f"{s}.{c} → {d}" for s, c, d in DESIGN_FKS if (s, c) not in declared]
    serial = [f"{t}.{c}" for t, c in DESIGN_SERIAL_PKS if c not in single[t]]
    only_key = [
        t.name
        for t in tables.values()
        if single[t.name] & {"storage_key"}
        and not [g for g in t.groups if g != ["storage_key"]]
        and not any(
            constraint.kind == "PK" and constraint.columns != ("storage_key",)
            for constraint in t.constraints.values()
        )
    ]
    keyless = [
        t.name
        for t in tables.values()
        if not t.groups and not any(v.kind == "PK" for v in t.constraints.values())
    ]
    no_pk = [t.name for t in tables.values() if not any(v.kind == "PK" for v in t.constraints.values())]
    n_mkt = sum(1 for n in no_pk if n.startswith("mkt."))
    ordered = sorted(by_target.items(), key=lambda kv: -kv[1])
    top = ", ".join(f"{k} ×{v}" for k, v in ordered[:5])
    rest = sum(v for _, v in ordered[5:])
    blocked_text = ", ".join(blocked)
    return [
        esc(f"Design §14 FK never created: {', '.join(missing_design)}")
        if missing_design
        else esc("All design §14 FKs exist"),
        esc(f"Design §14 bigserial PRIMARY KEYs that are not even UNIQUE: {', '.join(serial)}"),
        esc(
            f"{sum(by_target.values())} by-convention references (→ …) have no FK although the target is unique: {top}, others ×{rest}"
        ),
        esc(f"{len(composite)} more would work as composite FKs: {'; '.join(composite)}"),
        esc(
            f"{len(blocked)} cannot be FKs as the schema stands (target not unique, or an array): {blocked_text}"
        ),
        esc(
            f"{len(only_key)} tables are unique only on storage_key; writers put the domain id there (e.g. edge_id, question_id)"
        ),
        esc(
            f"{len(no_pk)} tables have no PRIMARY KEY: {n_mkt} × mkt.* (storage_key UNIQUE NOT NULL), {', '.join(k for k in keyless)} (no key at all)"
        ),
        esc(
            "FK columns are nullable; required_* CHECKs are NOT VALID (enforced on new writes, existing rows unchecked)"
        ),
    ]


def render_pg(root, base_ref=None):
    mig = sorted((root / "src/tank/migrations").glob("*.sql"))
    span = f"{mig[0].name[:3]}–{mig[-1].name[:3]}"
    tables, schemas = parse_pg(p.read_text() for p in mig)
    tables = finish_pg(tables)
    fks = foreign_keys(tables)
    placed = {n for col in PG_COLUMNS for n in col}
    missing = set(tables) - placed
    assert not missing, missing
    drawn, layout, diff_line = tables, PG_COLUMNS, None
    if base_ref is not None:
        base = pg_model_at(root, base_ref)
        drawn, removed = diff_tables(base, tables)
        layout = [list(names) for names in PG_COLUMNS]
        for n in sorted(removed):
            same = [sum(m.split(".")[0] == n.split(".")[0] for m in names) for names in layout]
            layout[same.index(max(same))].append(n)
            drawn[n] = removed[n]
        rows = [c for t in drawn.values() for c in t.cols]
        before = sum(index_count(t) for t in base.values())
        after = sum(index_count(t) for t in tables.values())
        diff_line = (
            f"Diff {base_ref} → working tree: tables {len(base)} → {len(tables)}, "
            f"columns removed {sum(c.get('change') == 'removed' for c in rows)}, "
            f"added {sum(c.get('change') == 'added' for c in rows)}, "
            f"altered {sum(c.get('change') == 'altered' for c in rows)}, indexes {before} → {after}. "
            "Red: removed (struck through), added or altered; unchanged tables are faded."
        )

    margin, top = 40, 330
    channel_y = top - 22
    col_x, col_w, mxs = {}, {}, {}
    x = margin
    for i, names in enumerate(layout):
        mx = pg_metrics([drawn[n] for n in names])
        title = max(tw(n, 12.5) * 1.05 + 70 for n in names)
        col_w[i] = max(mx["w"], title)
        col_x[i] = x
        mxs[i] = mx
        x += col_w[i] + PG_GUTTERS.get(i, 46)
    width = x - PG_GUTTERS.get(len(layout) - 1, 46) + margin

    boxes, col_of, body = {}, {}, []
    bottoms = {}
    for i, names in enumerate(layout):
        y = top
        for n in names:
            box, svg = pg_table(drawn[n], col_x[i], y, col_w[i], mxs[i])
            boxes[n] = box
            col_of[n] = i
            body.append(svg)
            y += box.h + GAP
        bottoms[i] = y

    edges = []
    for fk in fks:
        color = SCHEMA[fk.source_table.split(".")[0]][0]
        edges.append(route(fk, boxes, col_of, col_x, col_w, PG_GUTTERS, channel_y, PG_ROUTES[fk.name], color))

    fx, fy = col_x[1], bottoms[1] + 6
    fw = col_w[1]
    rows = []
    for fk in fks:
        rows.append(
            (
                fk.name,
                f"{fk.source_table}({', '.join(fk.source_columns)})",
                f"{fk.target_table}({', '.join(fk.target_columns)})",
            )
        )
    fh = 44 + len(rows) * 26 + 30
    panel = [
        f'<rect x="{fx}" y="{fy}" width="{fw}" height="{fh}" rx="5" fill="#fff" stroke="#2D3748" stroke-width="1.1"/>',
        f'<path d="M{fx},{fy + 5} a5,5 0 0 1 5,-5 h{fw - 10} a5,5 0 0 1 5,5 v18 h{-fw} z" fill="#2D3748"/>',
        text(fx + 9, fy + 15.5, f"FOREIGN KEY · {len(fks)}", 12, "#fff", 700),
        text(fx + 9, fy + 38, "all: DEFERRABLE INITIALLY DEFERRED · ON DELETE/UPDATE NO ACTION", 8.8, MUTED),
    ]
    for i, (name, a, b) in enumerate(rows):
        ry = fy + 56 + i * 26
        color = SCHEMA[a.split(".")[0]][0]
        panel.append(f'<rect x="{fx + 9}" y="{ry - 8}" width="4" height="20" rx="1" fill="{color}"/>')
        panel.append(text(fx + 19, ry, esc(name), 9.6, INK, 700, MONO))
        panel.append(text(fx + 19, ry + 11.5, esc(f"{a} → {b}"), 8.6, MUTED, None, MONO))
    panel.append(
        text(
            fx + 9,
            fy + fh - 12,
            "by-convention references (→ …) carry no FK constraint",
            8.8,
            "#3182CE",
            None,
            SANS,
            None,
            "italic",
        )
    )

    height = max(max(bottoms.values()), fy + fh + GAP) + 20

    hdr = []
    hdr.append(
        text(
            margin,
            52,
            "Tank · PostgreSQL — database schema <tspan font-weight='400' fill='#4A5568'>tank</tspan>",
            26,
            INK,
            700,
        )
    )
    n_fk = len(fks)
    hdr.append(
        text(
            margin,
            78,
            f"PostgreSQL 16 · migrations src/tank/migrations/{span} + ops.schema_migrations (storage.py) · "
            f"{len(schemas)} schemas · {len(tables)} tables · {n_fk} FK · {sum(index_count(t) for t in tables.values())} indexes · "
            "ER diagram generated from the DDL by scripts/render_db_schema.py · IE (crow’s foot) notation",
            12.5,
            "#4A5568",
        )
    )
    lx, ly = margin, 108
    bw = 600
    hdr.append(f'<rect x="{lx}" y="{ly}" width="{bw}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx + 14, ly + 22, "Relations", 12, INK, 700))
    hdr.append(legend_line(lx + 16, ly + 44, "fk"))
    hdr.append(
        text(lx + 100, ly + 48, "FOREIGN KEY: child 0..N  →  parent 0..1 (the FK column is nullable)", 10.2)
    )
    hdr.append(legend_line(lx + 16, ly + 70, "11", SCHEMA["ops"][0]))
    hdr.append(
        text(lx + 100, ly + 74, "1 : 1 — FK + UNIQUE (ops.agent_sessions.event_id → evt.events)", 10.2)
    )
    hdr.append(
        f'<path d="M{lx + 16},{ly + 96} H{lx + 38} V{ly + 88}" fill="none" stroke="#2D3748" stroke-width="1.1" stroke-dasharray="2 2"/>'
    )
    hdr.append(text(lx + 100, ly + 100, "composite FK: the second column joins with a dotted line", 10.2))
    hdr.append(text(lx + 16, ly + 126, "→ evt.events", 10, "#3182CE", None, MONO, None, "italic"))
    hdr.append(text(lx + 100, ly + 126, "by-convention reference, no FK constraint", 10.2))
    hdr.append(
        text(
            lx + 14,
            ly + 152,
            "Line colour = schema of the child table. Every FK targets a unique",
            10.2,
            MUTED,
        )
    )
    hdr.append(text(lx + 14, ly + 167, "domain id (UNIQUE INDEX), not the storage_key PK.", 10.2, MUTED))

    lx2 = lx + bw + 16
    bw2 = 720
    hdr.append(f'<rect x="{lx2}" y="{ly}" width="{bw2}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx2 + 14, ly + 22, "Columns", 12, INK, 700))
    items = [
        ("PK", "PRIMARY KEY"),
        ("FK", "FOREIGN KEY"),
        ("UK", "UNIQUE (single column)"),
        ("U1", "composite UNIQUE: columns sharing a number"),
        ("CK", "used by a CHECK (enum / consistency)"),
        ("SEQ", "DEFAULT nextval(sequence)"),
    ]
    for i, (lab, desc) in enumerate(items):
        cx = lx2 + 16 + (i % 2) * 318
        cy = ly + 36 + (i // 2) * 20
        g, _ = badge(cx, cy, lab)
        hdr.append(g)
        hdr.append(text(cx + 30, cy + 9, esc(desc), 10.2))
    yy = ly + 108
    hdr.append(text(lx2 + 16, yy + 9, "NN", 8.4, "#2D3748", 700))
    hdr.append(text(lx2 + 46, yy + 9, "NOT NULL", 10.2))
    hdr.append(text(lx2 + 334, yy + 9, "nn", 8.4, FAINT, 700))
    hdr.append(text(lx2 + 364, yy + 9, "CHECK required_…: NOT NULL or a reason in document", 10.2))
    hdr.append(text(lx2 + 16, yy + 31, "+prec", 8.4, "#DD6B20", 700))
    hdr.append(
        text(
            lx2 + 46,
            yy + 31,
            "hides the paired &lt;col&gt;_prec text and &lt;col&gt;_prec_reason text columns (timestamp precision)",
            10.2,
        )
    )
    hdr.append(
        text(
            lx2 + 16,
            yy + 53,
            "Document envelope (grey): storage_key text PK · document jsonb NN · immutable bool NN DEFAULT false",
            10.2,
            MUTED,
        )
    )
    hdr.append(
        text(
            lx2 + 16,
            yy + 70,
            "(DEFAULT true in ops.capture_tier_transitions); mkt.* has no envelope: storage_key is only UNIQUE, no PK",
            10.2,
            MUTED,
        )
    )

    lx3 = lx2 + bw2 + 16
    bw3 = 640
    hdr.append(f'<rect x="{lx3}" y="{ly}" width="{bw3}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx3 + 14, ly + 22, "Table flags (strip under the header)", 12, INK, 700))
    flags = [
        (
            "immutable-guard",
            "trigger ops.protect_immutable: UPDATE/DELETE of a row with immutable=true fails",
        ),
        ("append-only", "trigger ops.forbid_change: every UPDATE/DELETE fails"),
        ("NOTIFY tank_capture", "trigger ops.notify_capture: pg_notify to tank-capture"),
        ("lifecycle stamp", "trigger ops.stamp_job_lifecycle: created_at / finished_at"),
        ("UNLOGGED · fillfactor=50", "table storage parameters"),
        ("no PK", "the table has no PRIMARY KEY"),
        ("N idx", "indexes incl. PK/UNIQUE (btree, GIN on document, partial)"),
    ]
    for i, (f, desc) in enumerate(flags):
        cy = ly + 44 + i * 20
        hdr.append(text(lx3 + 16, cy, esc(f), 9.4, SCHEMA["ops"][0], 700))
        hdr.append(text(lx3 + 160, cy, esc(desc), 10))

    lx4 = lx3 + bw3 + 16
    bw4 = 840
    hdr.append(f'<rect x="{lx4}" y="{ly}" width="{bw4}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx4 + 14, ly + 22, "Schemas", 12, INK, 700))
    counts = {}
    for t in tables.values():
        counts[t.schema] = counts.get(t.schema, 0) + 1
    order = ["reg", "ingest", "evt", "ana", "out", "ops", "mkt", "qa", "scout"]
    for i, s in enumerate(order):
        cx = lx4 + 16 + (i // 3) * ((bw4 - 20) / 3)
        cy = ly + 40 + (i % 3) * 46
        fill, _, desc = SCHEMA[s]
        hdr.append(f'<rect x="{cx}" y="{cy}" width="16" height="16" rx="3" fill="{fill}"/>')
        hdr.append(
            text(
                cx + 24,
                cy + 12.5,
                f"<tspan font-weight='700'>{s}</tspan> · {counts.get(s, 0)} table{'' if counts.get(s, 0) == 1 else 's'}",
                12,
            )
        )
        hdr.append(text(cx + 24, cy + 28, esc(desc), 10, MUTED))

    lx5 = lx4 + bw4 + 16
    bw5 = width - margin - lx5
    hdr.append(
        f'<rect x="{lx5}" y="{ly}" width="{bw5}" height="190" rx="6" fill="#FFF5F5" stroke="#FEB2B2"/>'
    )
    hdr.append(
        text(
            lx5 + 14,
            ly + 22,
            f"Not enforced by the schema <tspan font-weight='400' fill='#718096'>(computed from migrations {span})</tspan>",
            12,
            "#9B2C2C",
            700,
        )
    )
    for i, line in enumerate(integrity_gaps(tables, fks)):
        hdr.append(f'<circle cx="{lx5 + 19}" cy="{ly + 40 + i * 18.5}" r="2.2" fill="#C53030"/>')
        hdr.append(text(lx5 + 28, ly + 44 + i * 18.5, line, 10.2))

    if diff_line:
        hdr = [f'<g opacity="0.3">{"".join(hdr)}</g>', text(margin, 100, esc(diff_line), 14, DIFF_RED, 700)]
        edges = [f'<g opacity="0.3">{"".join(edges)}</g>']
        panel = [f'<g opacity="0.3">{"".join(panel)}</g>']
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        *hdr,
        *edges,
        *body,
        *panel,
        "</svg>",
    ]
    return "\n".join(svg)


CH_COLUMNS = [
    ["mkt.flow_bars_1m"],
    ["mkt.klines_1m", "mkt.liq_obs"],
    ["mkt.trades", "mkt.funding_obs"],
    ["mkt.book_summaries"],
    ["mkt.oi_obs", "mkt.tickers_24h"],
]
CH_RETENTION = {
    "mkt.book_summaries": "kept 72 h (book_summary_retention_hours), no exceptions",
    "mkt.trades": "kept 30 days (trades_retention_days); MAJOR+ event days preserved",
}
CH_WRITER = {"mkt.trades": "written by tank-capture · capture/internal/rawstore/trades.go"}


def ch_type(t):
    return t.replace("DateTime64(6, 'UTC')", "DateTime64(6,'UTC')")


def ch_rows(t):
    rows = []
    for c in t["cols"]:
        labels = []
        if c["name"] in t["order"]:
            labels.append(f"SK{t['order'].index(c['name']) + 1}")
        if c["name"] == t["partition"]:
            labels.append("PT")
        if c["name"] == "_version":
            labels.append("VER")
        for _index, col, kind, _granularity in t["indexes"]:
            if col == c["name"]:
                labels.append("BF" if kind == "bloom_filter" else "MM")
        hint = {"numeric": "dec", "jsonb": "json"}.get(c["kind"])
        ref = {"instrument_id": "pg reg.instruments", "token_id": "pg reg.tokens"}.get(c["name"])
        rows.append(
            {
                "name": c["name"],
                "type": ch_type(c["type"]),
                "labels": labels,
                "hint": hint,
                "prec": c["prec"],
                "ref": ref,
            }
        )
    return rows


def ch_metrics(tables):
    rows = [r for t in tables for r in ch_rows(t)]
    name = max(len(r["name"]) for r in rows)
    typ = max(len(r["type"]) for r in rows)
    name_x = 56
    type_x = name_x + name * CW * 10.8 + 14
    hint_x = type_x + typ * CW * 10 + 6
    ref_x = hint_x + 58
    width = ref_x + len("→ pg reg.instruments") * CW * 9.2 + 10
    return {"name_x": name_x, "type_x": type_x, "hint_x": hint_x, "ref_x": ref_x, "w": width}


CH_FILL = "#1F2328"
CH_ACCENT = "#FFCC00"
CH_TINT = "#FFFBEA"


def ch_table(t, x, y, w, mx, retention):
    rows = ch_rows(t)
    engine = [
        ("ENGINE", "ReplacingMergeTree(_version)"),
        ("PARTITION BY", f"toYYYYMMDD({t['partition']})"),
        ("ORDER BY", f"({', '.join(t['order'])})"),
        (
            "RETENTION",
            retention.get(t["name"], "kept 90 days (base_retention_days); MAJOR+ event days preserved"),
        ),
    ]
    for n, col, k, g in t["indexes"]:
        engine.append(("INDEX", f"{n} {col} TYPE {k} GRANULARITY {g}"))
    if t["name"] in CH_WRITER:
        engine.append(("", CH_WRITER[t["name"]]))
    eng_h = 8 + len(engine) * 13
    box = Box(t["name"], x, y, w, rows, HEAD + eng_h)
    schema, short = t["name"].split(".")
    out = [f'<g id="{esc(t["name"])}">']
    out.append(
        f'<rect x="{x + 2}" y="{y + 3}" width="{w}" height="{box.h}" rx="5" fill="#000" opacity="0.07"/>'
    )
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{box.h}" rx="5" fill="#fff" stroke="{CH_FILL}" stroke-width="1.2"/>'
    )
    out.append(
        f'<path d="M{x},{y + 5} a5,5 0 0 1 5,-5 h{w - 10} a5,5 0 0 1 5,5 v{HEAD - 5} h{-w} z" fill="{CH_FILL}"/>'
    )
    out.append(
        f'<text x="{x + 9}" y="{y + 15.5}" font-family="{SANS}" font-size="12.5" fill="#fff">'
        f'<tspan opacity="0.65">{esc(schema)}.</tspan><tspan font-weight="700" fill="{CH_ACCENT}">{esc(short)}</tspan></text>'
    )
    out.append(
        f'<text x="{x + w - 8}" y="{y + 15.5}" font-family="{SANS}" font-size="9" fill="#A0AEC0" text-anchor="end">{len(t["cols"]) + sum(r["prec"] for r in rows) * 2} cols</text>'
    )
    out.append(f'<rect x="{x + 0.6}" y="{y + HEAD}" width="{w - 1.2}" height="{eng_h}" fill="{CH_TINT}"/>')
    out.append(
        f'<path d="M{x + 0.6},{y + HEAD + eng_h} H{x + w - 0.6}" stroke="#ECC94B" stroke-width="0.8"/>'
    )
    for i, (k, v) in enumerate(engine):
        ey = y + HEAD + 14 + i * 13
        out.append(text(x + 9, ey, esc(k), 8.6, "#975A16", 700, MONO))
        out.append(text(x + 92, ey, esc(v), 8.6, "#2D3748", None, MONO))
    for i, r in enumerate(rows):
        ry = y + HEAD + eng_h + i * ROW
        if i % 2:
            out.append(f'<rect x="{x + 0.6}" y="{ry}" width="{w - 1.2}" height="{ROW}" fill="#F7FAFC"/>')
        bx = x + 6
        for lab in r["labels"][:3]:
            g, bw = badge(bx, ry + 3, lab)
            out.append(g)
            bx += bw + 2
        weight = ' font-weight="700"' if any(lab.startswith("SK") for lab in r["labels"]) else ""
        nx = max(x + mx["name_x"], bx + 3)
        out.append(
            f'<text x="{nx:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10.8" fill="{INK}"{weight}>{esc(r["name"])}</text>'
        )
        tcolor = MUTED if r["type"].startswith(("Nullable", "Array")) else "#2D3748"
        out.append(
            f'<text x="{x + mx["type_x"]:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10" fill="{tcolor}">{esc(r["type"])}</text>'
        )
        hx = x + mx["hint_x"]
        if r["prec"]:
            out.append(text(hx, ry + 11.8, "+prec", 8, "#DD6B20", 600))
        elif r["hint"]:
            out.append(text(hx, ry + 11.8, r["hint"], 8, "#805AD5", 700))
        if r["ref"]:
            out.append(
                text(
                    x + mx["ref_x"], ry + 12, esc("→ " + r["ref"]), 9.2, "#3182CE", None, MONO, None, "italic"
                )
            )
    out.append("</g>")
    return box, "".join(out)


def render_ch(root):
    tables, chm = ch_model(root)
    by = {t["name"]: t for t in tables}
    assert {n for c in CH_COLUMNS for n in c} == set(by)
    margin, top, gutter = 40, 330, 30
    col_x, col_w, mxs = {}, {}, {}
    x = margin
    for i, names in enumerate(CH_COLUMNS):
        mx = ch_metrics([by[n] for n in names])
        col_w[i], col_x[i], mxs[i] = mx["w"], x, mx
        x += mx["w"] + gutter
    width = x - gutter + margin
    body, bottoms = [], {}
    for i, names in enumerate(CH_COLUMNS):
        y = top
        for n in names:
            box, svg = ch_table(by[n], col_x[i], y, col_w[i], mxs[i], CH_RETENTION)
            body.append(svg)
            y += box.h + GAP
        bottoms[i] = y

    px, py, pw = col_x[3], bottoms[3] + 4, col_w[3]
    notes = [
        ("Writes", "tank-capture (Go): INSERT … FORMAT JSONEachRow;"),
        ("", "series: capture/internal/store, prints: rawstore/trades.go"),
        ("Reads", "Tank core and tank-capture: SELECT … FROM t FINAL"),
        ("", "(per ORDER BY key, the row with max _version wins)"),
        ("Cleanup", "no TTL in the DDL: after the backup, prune() runs"),
        ("", "ALTER TABLE … DROP PARTITION per day"),
        ("DDL", "src/tank/clickhouse_market.py ddl(), idempotent"),
        ("Agent", "user tank_agent: readonly=1,"),
        ("", "max_execution_time=120 s, max_memory_usage=1.5 GB"),
        ("Server", "≤ 40% of RAM; system.query_log TTL 3 days,"),
        ("", "all other system logs disabled"),
    ]
    ph = 34 + len(notes) * 15 + 10
    panel = [
        f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" rx="5" fill="#fff" stroke="{CH_FILL}" stroke-width="1.1"/>',
        f'<path d="M{px},{py + 5} a5,5 0 0 1 5,-5 h{pw - 10} a5,5 0 0 1 5,5 v18 h{-pw} z" fill="{CH_FILL}"/>',
        text(
            px + 9,
            py + 15.5,
            f"<tspan fill='{CH_ACCENT}'>Operations</tspan> · ClickHouse 25.8",
            12,
            "#fff",
            700,
        ),
    ]
    for i, (k, v) in enumerate(notes):
        ny = py + 42 + i * 15
        panel.append(text(px + 10, ny, esc(k), 9.6, "#975A16", 700))
        panel.append(text(px + 70, ny, esc(v), 9.6, INK))

    height = max(max(bottoms.values()), py + ph + GAP) + 20
    hdr = [
        text(
            margin,
            52,
            "Tank · ClickHouse — database schema <tspan font-weight='400' fill='#4A5568'>mkt</tspan>",
            26,
            INK,
            700,
        ),
        text(
            margin,
            78,
            f"ClickHouse 25.8 · DDL generated by src/tank/clickhouse_market.py from src/tank/market_schema.py · {len(tables)} tables · "
            "exchange market series and prints (HLD §7.1) · ClickHouse has no foreign keys: relations are logical, by instrument_id / token_id",
            12.5,
            "#4A5568",
        ),
    ]
    lx, ly = margin, 108
    bw = 760
    hdr.append(f'<rect x="{lx}" y="{ly}" width="{bw}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx + 14, ly + 22, "Columns", 12, INK, 700))
    items = [
        ("SK1", "ORDER BY sorting key (number = position)"),
        ("PT", "partition key column (daily partitions)"),
        ("VER", "_version UInt64: ReplacingMergeTree version"),
        ("BF", "bloom_filter skip index (storage_key)"),
        ("MM", "minmax skip index (recorded_at)"),
    ]
    for i, (lab, desc) in enumerate(items):
        cx = lx + 16 + (i % 2) * 370
        cy = ly + 36 + (i // 2) * 20
        g, _ = badge(cx, cy, lab)
        hdr.append(g)
        hdr.append(text(cx + 32, cy + 9, esc(desc), 10.2))
    yy = ly + 106
    hdr.append(text(lx + 16, yy, "dec", 8.4, "#805AD5", 700))
    hdr.append(text(lx + 48, yy, "exact decimal stored as text (String, up to 100 digits)", 10.2))
    hdr.append(text(lx + 16, yy + 18, "json", 8.4, "#805AD5", 700))
    hdr.append(text(lx + 48, yy + 18, "JSON stored as text (String)", 10.2))
    hdr.append(text(lx + 16, yy + 36, "+prec", 8.4, "#DD6B20", 700))
    hdr.append(
        text(
            lx + 48,
            yy + 36,
            "hides the paired &lt;col&gt;_prec and &lt;col&gt;_prec_reason Nullable(String) columns",
            10.2,
        )
    )
    hdr.append(text(lx + 16, yy + 56, "→ pg reg.…", 10, "#3182CE", None, MONO, None, "italic"))
    hdr.append(
        text(lx + 110, yy + 56, "logical reference to a PostgreSQL table (cross-database, unenforced)", 10.2)
    )

    lx2 = lx + bw + 16
    bw2 = 720
    hdr.append(f'<rect x="{lx2}" y="{ly}" width="{bw2}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx2 + 14, ly + 22, "Types and codecs (identical across the series tables)", 12, INK, 700))
    codecs = [
        ("DateTime64(6,'UTC')", "CODEC(DoubleDelta, ZSTD(3))"),
        ("Int64, _version UInt64", "CODEC(Delta, ZSTD(3))"),
        ("Bool", "no codec"),
        ("String, Array(String)", "CODEC(ZSTD(3))"),
        ("mkt.trades: LowCardinality(…)", "no codec; Decimal(38, 18): ZSTD(3)"),
    ]
    for i, (k, v) in enumerate(codecs):
        cy = ly + 46 + i * 19
        hdr.append(text(lx2 + 16, cy, esc(k), 10, INK, None, MONO))
        hdr.append(text(lx2 + 290, cy, esc(v), 10, "#975A16", None, MONO))
    hdr.append(
        text(
            lx2 + 16,
            ly + 150,
            "Columns outside ORDER BY are Nullable(…); arrays are not. The first column is",
            10.2,
            MUTED,
        )
    )
    hdr.append(
        text(
            lx2 + 16,
            ly + 166,
            "storage_key String (except mkt.trades: own explicit schema, no skip indexes).",
            10.2,
            MUTED,
        )
    )

    lx3 = lx2 + bw2 + 16
    bw3 = width - margin - lx3
    hdr.append(f'<rect x="{lx3}" y="{ly}" width="{bw3}" height="190" rx="6" fill="#F7FAFC" stroke="{LINE}"/>')
    hdr.append(text(lx3 + 14, ly + 22, "Engine", 12, INK, 700))
    eng = [
        "All tables: ReplacingMergeTree(_version).",
        "ORDER BY key = record identity: when parts merge, the row",
        "with the highest _version survives; reads always use FINAL.",
        "Daily partitions: toYYYYMMDD(series time).",
        "Capture control tables (hot_tokens, capture_requests,",
        "capture_streams, capture_gaps, backfill_runs, supply, unlocks)",
        "live in PostgreSQL, schema mkt.",
    ]
    for i, s in enumerate(eng):
        hdr.append(text(lx3 + 16, ly + 46 + i * 19, esc(s), 10.2))

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        *hdr,
        *body,
        *panel,
        "</svg>",
    ]
    return "\n".join(svg)


def main():
    parser = argparse.ArgumentParser(
        description="Render diagrams/tank_postgres_schema.svg and diagrams/tank_clickhouse_schema.svg "
        "from src/tank/migrations, src/tank/market_schema.py and src/tank/clickhouse_market.py."
    )
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--check", action="store_true", help="exit 1 when a committed diagram is out of date")
    parser.add_argument(
        "--png", action="store_true", help="also export each SVG as a 2x PNG with rsvg-convert"
    )
    parser.add_argument(
        "--diff",
        metavar=("BASE_REF", "OUT_DIR"),
        nargs=2,
        help="write OUT_DIR/tank_postgres_schema.diff.{svg,png}: the schema against BASE_REF, changes in red",
    )
    args = parser.parse_args()
    if args.diff:
        ref, out = args.diff[0], Path(args.diff[1])
        out.mkdir(parents=True, exist_ok=True)
        path = out / "tank_postgres_schema.diff.svg"
        path.write_text(render_pg(args.root, ref))
        subprocess.run(
            ["rsvg-convert", "-z", "2", str(path), "-o", str(path.with_suffix(".png"))], check=True
        )
        print(path.with_suffix(".png"))
        return 0
    stale = []
    for name, svg in (
        ("tank_postgres_schema", render_pg(args.root)),
        ("tank_clickhouse_schema", render_ch(args.root)),
    ):
        path = args.root / "diagrams" / f"{name}.svg"
        if args.check:
            if not path.exists() or path.read_text() != svg:
                stale.append(f"diagrams/{name}.svg")
            continue
        path.write_text(svg)
        print(path)
        if args.png:
            subprocess.run(
                ["rsvg-convert", "-z", "2", str(path), "-o", str(path.with_suffix(".png"))], check=True
            )
            print(path.with_suffix(".png"))
    if stale:
        print("Out of date: " + ", ".join(stale) + "; run scripts/render_db_schema.py --png", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
