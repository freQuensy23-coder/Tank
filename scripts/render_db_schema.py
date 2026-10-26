"""Render Tank's PostgreSQL and ClickHouse database schemas as ER diagrams.

The model is parsed from src/tank/migrations/*.sql, src/tank/market_schema.py and
src/tank/clickhouse_market.py; no database is needed. Run after every schema change and
commit diagrams/tank_postgres_schema.{svg,png} and diagrams/tank_clickhouse_schema.{svg,png}.
"""

import argparse
import ast
import html
import re
import subprocess
import sys
from collections.abc import Iterator, Mapping, Sequence, Set
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, StringConstraints, model_validator

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
type SQLTableName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_]\w*\.[A-Za-z_]\w*$")]
type NonBlankText = Annotated[str, StringConstraints(min_length=1, pattern=r"\S")]


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
    target_table: SQLTableName
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


class PostgreSQLMeasurements(SchemaRecord):
    name_x: FiniteFloat
    type_x: FiniteFloat
    prec_x: FiniteFloat
    nn_x: FiniteFloat
    ref_x: FiniteFloat
    width: Annotated[FiniteFloat, Field(gt=0)]


class ClickHouseMeasurements(SchemaRecord):
    name_x: FiniteFloat
    type_x: FiniteFloat
    hint_x: FiniteFloat
    ref_x: FiniteFloat
    width: Annotated[FiniteFloat, Field(gt=0)]


class MarketSchemaMetadata(SchemaRecord):
    MARKET_COLUMNS: dict[SQLTableName, dict[SQLColumnName, NonBlankText]]
    MARKET_IDENTITIES: dict[SQLTableName, tuple[SQLColumnName, ...]]

    @model_validator(mode="after")
    def complete_identities(self) -> Self:
        if self.MARKET_COLUMNS.keys() != self.MARKET_IDENTITIES.keys():
            raise ValueError("Market columns and identity table sets must match")
        for name, identity in self.MARKET_IDENTITIES.items():
            if not identity or len(set(identity)) != len(identity):
                raise ValueError("Market identity must contain distinct columns")
            if not set(identity) <= self.MARKET_COLUMNS[name].keys() | {"storage_key"}:
                raise ValueError("Market identity references an undeclared column")
        return self


class ClickHouseMetadata(SchemaRecord):
    MARKET_TABLES: dict[SQLTableName, SQLColumnName]
    TRADES: SQLTableName
    TRADE_COLUMNS: dict[SQLColumnName, NonBlankText]
    TYPES: dict[NonBlankText, NonBlankText]
    ZSTD: NonBlankText
    TIME_CODEC: NonBlankText
    VERSION_CODEC: NonBlankText


class ClickHouseInputs(SchemaRecord):
    market: MarketSchemaMetadata
    backend: ClickHouseMetadata

    @model_validator(mode="after")
    def consumed_columns(self) -> Self:
        for name, partition in self.backend.MARKET_TABLES.items():
            if name not in self.market.MARKET_COLUMNS:
                raise ValueError("ClickHouse series table requires declared market columns")
            columns = self.market.MARKET_COLUMNS[name]
            if partition not in columns:
                raise ValueError("ClickHouse partition requires a declared column")
            if not set(columns.values()) <= self.backend.TYPES.keys():
                raise ValueError("ClickHouse series requires an explicit type mapping")
        return self


class RetentionConfiguration(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)
    base_retention_days: int
    trades_retention_days: int
    book_summary_retention_hours: int


class BaseRetention(SchemaRecord):
    kind: Literal["BASE"]
    days: int


class TradesRetention(SchemaRecord):
    kind: Literal["TRADES"]
    days: int


class BookRetention(SchemaRecord):
    kind: Literal["BOOK"]
    hours: int


type RetentionRule = BaseRetention | TradesRetention | BookRetention


def retention_text(rule: RetentionRule) -> str:
    if isinstance(rule, BookRetention):
        return f"kept {rule.hours} h (book_summary_retention_hours), no exceptions"
    if isinstance(rule, TradesRetention):
        return f"kept {rule.days} days (trades_retention_days); MAJOR+ event days preserved"
    return f"kept {rule.days} days (base_retention_days); MAJOR+ event days preserved"


class ClickHouseColumn(SchemaRecord):
    name: SQLColumnName
    data_type: NonBlankText
    codec: str
    precision: bool


class SQLDerivedColumn(ClickHouseColumn):
    origin: Literal["SQL"]
    sql_kind: NonBlankText


class NativeColumn(ClickHouseColumn):
    origin: Literal["NATIVE"]


class VersionColumn(ClickHouseColumn):
    origin: Literal["VERSION"]
    name: Literal["_version"]
    data_type: Literal["UInt64"]

    @model_validator(mode="after")
    def no_precision_companions(self) -> Self:
        if self.precision:
            raise ValueError("Version columns have no timestamp precision companions")
        return self


type CapturedColumn = SQLDerivedColumn | NativeColumn | VersionColumn


class SkipIndex(SchemaRecord):
    name: SQLColumnName
    column: SQLColumnName
    kind: Literal["bloom_filter", "minmax"]
    granularity: Annotated[int, Field(gt=0)]


class ClickHouseTable(SchemaRecord):
    name: SQLTableName
    partition: SQLColumnName
    order: tuple[SQLColumnName, ...] = Field(min_length=1)
    cols: tuple[CapturedColumn, ...] = Field(min_length=1)
    indexes: tuple[SkipIndex, ...]

    @model_validator(mode="after")
    def declared_keys(self) -> Self:
        columns = {column.name for column in self.cols}
        if len(columns) != len(self.cols):
            raise ValueError("ClickHouse diagram columns must be unique")
        if self.partition not in columns or not set(self.order) <= columns:
            raise ValueError("ClickHouse keys require declared columns")
        if any(index.column not in columns for index in self.indexes):
            raise ValueError("Skip indexes require declared columns")
        return self


class ClickHouseRow(SchemaRecord):
    name: str
    data_type: str
    labels: tuple[str, ...]
    hint: str | None
    precision: bool
    reference: str | None


class ForeignKeyLink(SchemaRecord):
    name: str = Field(min_length=1)
    source_table: SQLTableName
    source_columns: tuple[SQLColumnName, ...] = Field(min_length=1)
    target_table: SQLTableName
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


class SQLColumn(SchemaRecord):
    name: SQLColumnName
    sql_type: NonBlankText
    not_null: bool
    sequence_default: bool
    precision: bool


class UniqueIndex(SchemaRecord):
    name: NonBlankText
    columns: tuple[SQLColumnName, ...] = Field(min_length=1)
    predicate: NonBlankText | None


class ConstraintEntry(SchemaRecord):
    name: NonBlankText
    rule: Constraint


class ParsedTable(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", validate_assignment=True)
    name: SQLTableName
    cols: list[SQLColumn]
    constraints: dict[str, Constraint]
    unique_indexes: list[UniqueIndex]
    indexes: Annotated[int, Field(ge=0)]
    triggers: list[str]
    unlogged: bool
    storage: NonBlankText | None
    note: NonBlankText | None

    @property
    def namespace(self) -> str:
        return self.name.split(".")[0]

    def col(self, name: str) -> SQLColumn:
        for column in self.cols:
            if column.name == name:
                return column
        raise KeyError(f"{self.name}.{name}")


type RowHighlight = Literal["added", "removed", "altered"]
type TableChange = Literal["plain", "unchanged", "added", "removed", "altered"]


class RenderedColumn(SQLColumn):
    primary_key: bool
    foreign_key: bool
    unique_marker: str | None
    check: bool
    required_check: bool
    logical_reference: str | None
    change: RowHighlight | None

    def highlighted(self, change: RowHighlight) -> "RenderedColumn":
        return RenderedColumn(
            name=self.name,
            sql_type=self.sql_type,
            not_null=self.not_null,
            sequence_default=self.sequence_default,
            precision=self.precision,
            primary_key=self.primary_key,
            foreign_key=self.foreign_key,
            unique_marker=self.unique_marker,
            check=self.check,
            required_check=self.required_check,
            logical_reference=self.logical_reference,
            change=change,
        )


class IndexDelta(SchemaRecord):
    before: Annotated[int, Field(ge=0)]
    after: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def changed_count(self) -> Self:
        if self.before == self.after:
            raise ValueError("An index delta requires distinct counts")
        return self


class RenderedTable(SchemaRecord):
    name: SQLTableName
    cols: tuple[RenderedColumn, ...] = Field(min_length=1)
    constraints: tuple[ConstraintEntry, ...]
    unique_indexes: tuple[UniqueIndex, ...]
    indexes: Annotated[int, Field(ge=0)]
    triggers: tuple[str, ...]
    unlogged: bool
    storage: NonBlankText | None
    note: NonBlankText | None
    groups: tuple[tuple[str, ...], ...]
    flags: tuple[str, ...]
    change: TableChange
    index_delta: IndexDelta | None

    @property
    def namespace(self) -> str:
        return self.name.split(".")[0]

    def col(self, name: str) -> RenderedColumn:
        for column in self.cols:
            if column.name == name:
                return column
        raise KeyError(f"{self.name}.{name}")

    def compared(
        self, columns: tuple[RenderedColumn, ...], change: TableChange, delta: IndexDelta | None
    ) -> "RenderedTable":
        return RenderedTable(
            name=self.name,
            cols=columns,
            constraints=self.constraints,
            unique_indexes=self.unique_indexes,
            indexes=self.indexes,
            triggers=self.triggers,
            unlogged=self.unlogged,
            storage=self.storage,
            note=self.note,
            groups=self.groups,
            flags=self.flags,
            change=change,
            index_delta=delta,
        )


def new_table(name: str) -> ParsedTable:
    return ParsedTable(
        name=name,
        cols=[],
        constraints={},
        unique_indexes=[],
        indexes=0,
        triggers=[],
        unlogged=False,
        storage=None,
        note=None,
    )


def new_col(name: str, typ: str, not_null: bool, sequence_default: bool, precision: bool) -> SQLColumn:
    return SQLColumn(
        name=name,
        sql_type=norm_type(typ),
        not_null=not_null,
        sequence_default=sequence_default,
        precision=precision,
    )


def parse_col_def(table: ParsedTable, statement: str) -> None:
    name, rest = statement.split(" ", 1)
    match = re.search(r" (NOT NULL|DEFAULT|PRIMARY KEY|UNIQUE|CONSTRAINT|CHECK|REFERENCES)\b", " " + rest)
    kind = (" " + rest)[: match.start()].strip() if match else rest
    tail = rest[len(kind) :]
    default = re.search(r"DEFAULT (\S+)", tail)
    sequence = default is not None and default.group(1).startswith("nextval")
    column = new_col(name, kind, "NOT NULL" in tail, sequence, False)
    if "PRIMARY KEY" in tail:
        table.constraints[f"{table.name}_pkey"] = KeyConstraint(kind="PK", columns=(name,))
    table.cols.append(column)


def parse_constraint(t: ParsedTable, text: str, name: str | None = None) -> None:
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
        names = {c.name for c in t.cols}
        if name and name.startswith("required_"):
            target: str | None = name[len("required_") :]
            if target not in names:
                found = re.search(r"\b(\w+) IS NOT NULL", expr)
                target = found.group(1) if found else None
            if target is not None and target in names:
                t.constraints[name] = CheckConstraint(kind="REQ", columns=(target,), valid=valid)
                return
        used = [w for w in dict.fromkeys(re.findall(r"\b[a-z_][a-z0-9_]*\b", expr)) if w in names]
        t.constraints[name if name is not None else f"check_{len(t.constraints)}"] = CheckConstraint(
            kind="CK", columns=tuple(used), valid=valid
        )
    else:
        raise ValueError("Unsupported diagram constraint syntax")


def drop_column(table: ParsedTable, name: str) -> None:
    table.cols = [column for column in table.cols if column.name != name]
    table.constraints = {key: value for key, value in table.constraints.items() if name not in value.columns}
    retained = [index for index in table.unique_indexes if name not in index.columns]
    table.indexes -= len(table.unique_indexes) - len(retained)
    table.unique_indexes = retained


def mark_sequence_default(table: ParsedTable, name: str) -> None:
    found = table.col(name)
    table.cols = [
        new_col(column.name, column.sql_type, column.not_null, True, column.precision)
        if column.name == found.name
        else column
        for column in table.cols
    ]


def parse_pg(sources: Iterator[str]) -> tuple[dict[str, ParsedTable], list[str]]:
    tables: dict[str, ParsedTable] = {}
    schemas: list[str] = []
    owners: dict[tuple[str, str], str] = {}
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
                    t.unique_indexes = [u for u in t.unique_indexes if u.name != m.group(3)]
            elif m := re.match(
                r"CREATE (UNLOGGED )?TABLE (IF NOT EXISTS )?(\w+\.\w+) \((.*?)\)( WITH \((.*)\))?$", s
            ):
                t = tables.setdefault(m.group(3), new_table(m.group(3)))
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
                    mark_sequence_default(t, a.group(1))
            elif m := re.match(
                r"CREATE (UNIQUE )?INDEX (IF NOT EXISTS )?(\w+) ON (\w+\.\w+)( USING (\w+))? ?\((.*?)\)( WHERE .*)?$",
                s,
            ):
                t = tables[m.group(4)]
                t.indexes += 1
                owners[(m.group(4).split(".")[0], m.group(3))] = m.group(4)
                if m.group(1):
                    t.unique_indexes.append(
                        UniqueIndex(
                            name=m.group(3),
                            columns=tuple(cols_of(m.group(7))),
                            predicate=m.group(8).removeprefix(" WHERE ") if m.group(8) is not None else None,
                        )
                    )
            elif m := re.match(r"CREATE TRIGGER (\w+) .* ON (\w+\.\w+) .* FUNCTION ([\w.]+)\(", s):
                tables[m.group(2)].triggers.append(m.group(3))
    return tables, schemas


def collapse_prec(table: ParsedTable) -> None:
    names = {column.name for column in table.cols}
    hidden: set[str] = set()
    paired: set[str] = set()
    for column in table.cols:
        if column.sql_type != "timestamptz":
            continue
        bases = [column.name]
        if column.name.endswith("_at"):
            bases.append(column.name[:-3])
        matches: list[set[str]] = []
        for base in bases:
            pair = {base + "_prec", base + "_prec_reason"}
            found = pair & names
            if found and found != pair:
                raise ValueError("Timestamp precision metadata requires both companion columns")
            if found:
                matches.append(found)
        if len(matches) > 1:
            raise ValueError("Timestamp precision metadata has ambiguous companion names")
        if matches:
            hidden.update(matches[0])
            paired.add(column.name)
    table.cols = [
        new_col(column.name, column.sql_type, column.not_null, column.sequence_default, column.name in paired)
        for column in table.cols
        if column.name not in hidden
    ]


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


def finish_pg(tables: Mapping[str, ParsedTable]) -> dict[str, RenderedTable]:
    ledger = new_table("ops.schema_migrations")
    ledger.cols = [
        new_col("version", "text", False, False, False),
        new_col("applied_at", "timestamptz", True, False, False),
    ]
    ledger.constraints["schema_migrations_pkey"] = KeyConstraint(kind="PK", columns=("version",))
    ledger.note = "created by storage.py migrate()"
    parsed = {**tables, ledger.name: ledger}
    finished: dict[str, RenderedTable] = {}
    for table in parsed.values():
        collapse_prec(table)
        groups: list[tuple[str, ...]] = []
        candidates = [
            constraint.columns for constraint in table.constraints.values() if constraint.kind == "UK"
        ]
        candidates += [index.columns for index in table.unique_indexes if index.predicate is None]
        for columns in candidates:
            if columns not in groups:
                groups.append(columns)
        composite = [group for group in groups if len(group) > 1]
        own = {group[0] for group in groups if len(group) == 1}
        rendered: list[RenderedColumn] = []
        for column in table.cols:
            related = [rule for rule in table.constraints.values() if column.name in rule.columns]
            primary = any(rule.kind == "PK" for rule in related)
            foreign = any(rule.kind == "FK" for rule in related)
            not_null = column.not_null or primary
            unique_marker: str | None = None
            for group in groups:
                if column.name in group and not (primary and len(group) == 1):
                    unique_marker = f"U{composite.index(group) + 1}" if len(group) > 1 else "UK"
                    break
            reference = SPECIFIC.get((table.name, column.name))
            if reference is None:
                reference = LOGICAL.get(column.name)
            if (
                foreign
                or reference == table.name
                or (
                    column.name in own
                    and column.name in {"source_id", "entity_id", "token_id", "instrument_id"}
                )
            ):
                reference = None
            rendered.append(
                RenderedColumn(
                    name=column.name,
                    sql_type=column.sql_type,
                    not_null=not_null,
                    sequence_default=column.sequence_default,
                    precision=column.precision,
                    primary_key=primary,
                    foreign_key=foreign,
                    unique_marker=unique_marker,
                    check=any(rule.kind == "CK" for rule in related),
                    required_check=not not_null and any(rule.kind == "REQ" for rule in related),
                    logical_reference=reference,
                    change=None,
                )
            )
        flags = list(dict.fromkeys(TRIGGER_FLAGS[function] for function in table.triggers))
        if table.unlogged:
            flags.append("UNLOGGED")
        if table.storage is not None:
            flags.append(table.storage.replace("'", ""))
        if not any(rule.kind == "PK" for rule in table.constraints.values()):
            flags.append("no PK")
        finished[table.name] = RenderedTable(
            name=table.name,
            cols=tuple(rendered),
            constraints=tuple(
                ConstraintEntry(name=name, rule=rule) for name, rule in table.constraints.items()
            ),
            unique_indexes=tuple(table.unique_indexes),
            indexes=table.indexes,
            triggers=tuple(table.triggers),
            unlogged=table.unlogged,
            storage=table.storage,
            note=table.note,
            groups=tuple(groups),
            flags=tuple(flags),
            change="plain",
            index_delta=None,
        )
    return finished


def pg_model_at(root: Path, ref: str) -> dict[str, RenderedTable]:
    """The finished PostgreSQL model of the migrations committed at a git ref."""

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
        ).stdout

    names = sorted(
        n for n in git("ls-tree", "--name-only", ref, "src/tank/migrations/").split() if n.endswith(".sql")
    )
    tables, _ = parse_pg(git("show", f"{ref}:{n}") for n in names)
    return finish_pg(tables)


def diff_tables(
    base: Mapping[str, RenderedTable], tables: Mapping[str, RenderedTable]
) -> tuple[dict[str, RenderedTable], dict[str, RenderedTable]]:
    drawn: dict[str, RenderedTable] = {}
    for name, table in tables.items():
        old = base.get(name)
        if old is None:
            drawn[name] = table.compared(
                tuple(column.highlighted("added") for column in table.cols), "added", None
            )
            continue
        current = {column.name: column for column in table.cols}
        previous = {column.name: column for column in old.cols}
        columns: list[RenderedColumn] = []
        for column in old.cols:
            if column.name not in current:
                columns.append(column.highlighted("removed"))
            elif column != current[column.name]:
                columns.append(current[column.name].highlighted("altered"))
            else:
                columns.append(current[column.name])
        columns.extend(column.highlighted("added") for column in table.cols if column.name not in previous)
        before, after = index_count(old), index_count(table)
        delta = IndexDelta(before=before, after=after) if before != after else None
        changed = (
            delta is not None
            or old.flags != table.flags
            or any(column.change is not None for column in columns)
        )
        drawn[name] = table.compared(tuple(columns), "altered" if changed else "unchanged", delta)
    removed = {
        name: table.compared(tuple(column.highlighted("removed") for column in table.cols), "removed", None)
        for name, table in base.items()
        if name not in tables
    }
    return drawn, removed


def foreign_keys(tables: Mapping[str, RenderedTable]) -> list[ForeignKeyLink]:
    out: list[ForeignKeyLink] = []
    for table in tables.values():
        for entry in table.constraints:
            name, constraint = entry.name, entry.rule
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


def literal_assignments(path: Path, names: Set[str]) -> dict[str, object]:
    tree = ast.parse(Path(path).read_text())
    found: dict[str, object] = {}
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
            if value is None:
                raise ValueError("Schema assignment requires an explicit literal")
            found[target.id] = ast.literal_eval(value)
    return found


def precision_companions(name: str, data_type: str, names: Set[str]) -> bool:
    if "DateTime64" not in data_type:
        return False
    pair = {name + "_prec", name + "_prec_reason"}
    present = pair & names
    if present and present != pair:
        raise ValueError("Timestamp precision metadata requires both companion columns")
    return present == pair


def ch_model(root: Path) -> tuple[ClickHouseTable, ...]:
    schema = MarketSchemaMetadata.model_validate(
        literal_assignments(root / "src/tank/market_schema.py", {"MARKET_COLUMNS", "MARKET_IDENTITIES"})
    )
    backend = ClickHouseMetadata.model_validate(
        literal_assignments(
            root / "src/tank/clickhouse_market.py",
            {"MARKET_TABLES", "TRADES", "TRADE_COLUMNS", "TYPES", "ZSTD", "TIME_CODEC", "VERSION_CODEC"},
        )
    )
    inputs = ClickHouseInputs(market=schema, backend=backend)
    schema, backend = inputs.market, inputs.backend
    codec_of = {"timestamptz": backend.TIME_CODEC, "boolean": "", "bigint": "CODEC(Delta, ZSTD(3))"}
    tables: list[ClickHouseTable] = []
    for name, partition in backend.MARKET_TABLES.items():
        identity = schema.MARKET_IDENTITIES[name]
        definitions = schema.MARKET_COLUMNS[name]
        columns: list[CapturedColumn] = [
            SQLDerivedColumn(
                origin="SQL",
                name="storage_key",
                data_type="String",
                codec="",
                sql_kind="text",
                precision=False,
            )
        ]
        hidden: set[str] = set()
        for column, kind in definitions.items():
            data_type = backend.TYPES[kind]
            if column not in identity and kind != "text[]":
                data_type = f"Nullable({data_type})"
            paired = precision_companions(column, data_type, definitions.keys())
            if paired:
                hidden.update((column + "_prec", column + "_prec_reason"))
            columns.append(
                SQLDerivedColumn(
                    origin="SQL",
                    name=column,
                    data_type=data_type,
                    codec=codec_of.get(kind, backend.ZSTD),
                    sql_kind=kind,
                    precision=paired,
                )
            )
        columns.append(
            VersionColumn(
                origin="VERSION",
                name="_version",
                data_type="UInt64",
                codec=backend.VERSION_CODEC,
                precision=False,
            )
        )
        tables.append(
            ClickHouseTable(
                name=name,
                partition=partition,
                order=identity,
                cols=tuple(column for column in columns if column.name not in hidden),
                indexes=(
                    SkipIndex(
                        name="storage_key_bf", column="storage_key", kind="bloom_filter", granularity=4
                    ),
                    SkipIndex(name="recorded_at_mm", column="recorded_at", kind="minmax", granularity=1),
                ),
            )
        )
    native: list[CapturedColumn] = []
    hidden = set()
    for name, data_type in backend.TRADE_COLUMNS.items():
        codec = (
            backend.TIME_CODEC
            if "DateTime64" in data_type
            else ""
            if "LowCardinality" in data_type
            else backend.ZSTD
        )
        paired = precision_companions(name, data_type, backend.TRADE_COLUMNS.keys())
        if paired:
            hidden.update((name + "_prec", name + "_prec_reason"))
        native.append(
            NativeColumn(origin="NATIVE", name=name, data_type=data_type, codec=codec, precision=paired)
        )
    native.append(
        VersionColumn(
            origin="VERSION",
            name="_version",
            data_type="UInt64",
            codec=backend.VERSION_CODEC,
            precision=False,
        )
    )
    tables.append(
        ClickHouseTable(
            name=backend.TRADES,
            partition="ts",
            order=("instrument_id", "ts", "trade_id"),
            cols=tuple(column for column in native if column.name not in hidden),
            indexes=(),
        )
    )
    return tuple(tables)


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


type Coordinate = int | FiniteFloat


class Box(SchemaRecord):
    """A positioned table box with per-row anchors."""

    name: str
    x: Coordinate
    y: Coordinate
    w: Annotated[Coordinate, Field(gt=0)]
    rows: tuple[RenderedColumn | ClickHouseRow, ...]
    head_h: Annotated[Coordinate, Field(ge=0)]

    @property
    def h(self) -> Coordinate:
        return self.head_h + len(self.rows) * ROW + 3

    def row_y(self, col: str) -> Coordinate:
        index = [row.name for row in self.rows].index(col)
        return self.y + self.head_h + index * ROW + ROW / 2


def index_count(t: RenderedTable) -> int:
    return t.indexes + sum(1 for entry in t.constraints if entry.rule.kind in ("PK", "UK"))


def pg_metrics(tables: Sequence[RenderedTable]) -> PostgreSQLMeasurements:
    name = max(len(c.name) for t in tables for c in t.cols)
    typ = max(len(c.sql_type) for t in tables for c in t.cols)
    ref = max([len(c.logical_reference) + 2 for t in tables for c in t.cols if c.logical_reference] or [0])
    name_x = 50
    type_x = name_x + name * CW * 10.8 + 14
    prec_x = type_x + typ * CW * 10 + 4
    nn_x = prec_x + 27
    ref_x = nn_x + 21
    width = (ref_x + ref * CW * 9.2 + 10) if ref else nn_x + 22
    return PostgreSQLMeasurements(
        name_x=name_x, type_x=type_x, prec_x=prec_x, nn_x=nn_x, ref_x=ref_x, width=width
    )


def pg_table(
    t: RenderedTable, x: Coordinate, y: Coordinate, w: Coordinate, mx: PostgreSQLMeasurements
) -> tuple[Box, str]:
    fill, tint, _ = SCHEMA[t.namespace]
    change = t.change
    if change == "removed":
        fill, tint = DIFF_RED, DIFF_WASH
    schema, short = t.name.split(".")
    box = Box(name=t.name, x=x, y=y, w=w, rows=t.cols, head_h=HEAD + SUB)
    fade = ' opacity="0.3"' if change == "unchanged" else ""
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
    if t.index_delta is not None:
        idx, idx_ink = f"{t.index_delta.before} → {t.index_delta.after} idx", DIFF_RED
    out.append(
        f'<text x="{x + w - 8}" y="{y + HEAD + 10.6}" font-family="{SANS}" font-size="8.6" fill="{idx_ink}" '
        f'{'font-weight="700" ' if idx_ink == DIFF_RED else ""}text-anchor="end">{idx}</text>'
    )
    for i, c in enumerate(t.cols):
        ry = y + HEAD + SUB + i * ROW
        row_change = c.change
        if row_change:
            out.append(f'<rect x="{x + 0.6}" y="{ry}" width="{w - 1.2}" height="{ROW}" fill="{DIFF_WASH}"/>')
        elif i % 2:
            out.append(f'<rect x="{x + 0.6}" y="{ry}" width="{w - 1.2}" height="{ROW}" fill="#F7FAFC"/>')
        bx = x + 6
        labels = []
        if c.primary_key:
            labels.append("PK")
        if c.foreign_key:
            labels.append("FK")
        if c.unique_marker:
            labels.append(c.unique_marker)
        if c.check:
            labels.append("CK")
        if c.sequence_default:
            labels.append("SEQ")
        for lab in labels[:3]:
            g, bw = badge(bx, ry + 3, lab)
            out.append(g)
            bx += bw + 2
        envelope = c.name in ("document", "immutable") and t.cols[0].name == "storage_key"
        ink = DIFF_RED if row_change else FAINT if envelope else INK
        weight = ' font-weight="700"' if c.primary_key else ""
        nx = max(x + mx.name_x, bx + 3)
        out.append(
            f'<text x="{nx:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10.8" fill="{ink}"{weight}>{esc(c.name)}</text>'
        )
        out.append(
            f'<text x="{x + mx.type_x:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10" '
            f'fill="{DIFF_RED if row_change else FAINT if envelope else MUTED}">{esc(c.sql_type)}</text>'
        )
        if row_change == "removed":
            end = x + mx.type_x + tw(c.sql_type, 10)
            out.append(
                f'<path d="M{nx - 2:.1f},{ry + 8.5} H{end + 2:.1f}" stroke="{DIFF_RED}" stroke-width="1.2"/>'
            )
        if c.precision:
            px = x + mx.type_x + tw(c.sql_type, 10) + 4
            out.append(
                f'<text x="{px:.1f}" y="{ry + 11.8}" font-family="{SANS}" font-size="8" fill="#DD6B20" font-weight="600">+prec</text>'
            )
        if c.not_null:
            out.append(
                f'<text x="{x + mx.nn_x:.1f}" y="{ry + 11.8}" font-family="{SANS}" font-size="8" '
                f'fill="{FAINT if envelope else "#2D3748"}" font-weight="700">NN</text>'
            )
        elif c.required_check:
            out.append(
                f'<text x="{x + mx.nn_x:.1f}" y="{ry + 11.8}" font-family="{SANS}" font-size="8" fill="{FAINT}" font-weight="600">nn</text>'
            )
        if c.logical_reference is not None and row_change != "removed":
            out.append(
                f'<text x="{x + mx.ref_x:.1f}" y="{ry + 12}" font-family="{MONO}" font-size="9.2" fill="#3182CE" '
                f'font-style="italic">→ {esc(c.logical_reference)}</text>'
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

PG_GUTTERS = {0: 46, 1: 84, 2: 96, 3: 84, 4: 64, 5: 64, 6: 46, 7: 46, 8: 46}


class RouteSpec(SchemaRecord):
    side: Literal["L", "R"] | None
    lanes: tuple[Annotated[int, Field(ge=0)], ...] = Field(min_length=1, max_length=2)
    one_to_one: bool


class ColumnPosition(SchemaRecord):
    x: Coordinate
    width: Annotated[Coordinate, Field(gt=0)]


PG_ROUTES = {
    "decision_state_heads_current_scope_fk": RouteSpec(side="R", lanes=(1,), one_to_one=False),
    "decision_state_heads_previous_scope_fk": RouteSpec(side="R", lanes=(2,), one_to_one=False),
    "session_origin": RouteSpec(side=None, lanes=(0,), one_to_one=False),
    "observation_item": RouteSpec(side=None, lanes=(1,), one_to_one=False),
    "claim_observation": RouteSpec(side="L", lanes=(2,), one_to_one=False),
    "tool_session": RouteSpec(side="L", lanes=(3,), one_to_one=False),
    "observation_event": RouteSpec(side=None, lanes=(0,), one_to_one=False),
    "session_event": RouteSpec(side=None, lanes=(1,), one_to_one=True),
    "change_event": RouteSpec(side="L", lanes=(2,), one_to_one=False),
    "assessment_event": RouteSpec(side="L", lanes=(3,), one_to_one=False),
    "event_merge_target": RouteSpec(side="L", lanes=(4,), one_to_one=False),
    "call_event": RouteSpec(side=None, lanes=(0,), one_to_one=False),
    "decision_event": RouteSpec(side=None, lanes=(1,), one_to_one=False),
    "truth_event": RouteSpec(side=None, lanes=(2,), one_to_one=False),
    "alert_event": RouteSpec(side=None, lanes=(1, 3), one_to_one=False),
    "decision_truth": RouteSpec(side="R", lanes=(0,), one_to_one=False),
    "delivery_alert": RouteSpec(side="R", lanes=(0,), one_to_one=False),
    "delivery_notice": RouteSpec(side="R", lanes=(1,), one_to_one=False),
}
LANES = {0: 1, 1: 4, 2: 5, 3: 4, 4: 2, 5: 2}


def route(
    fk: ForeignKeyLink,
    boxes: Mapping[str, Box],
    col_of: Mapping[str, int],
    positions: Mapping[int, ColumnPosition],
    gut: Mapping[int, int],
    channel_y: Coordinate,
    spec: RouteSpec,
    color: str,
) -> str:
    src, dst = boxes[fk.source_table], boxes[fk.target_table]
    ci, cj = col_of[fk.source_table], col_of[fk.target_table]
    sy, dy = src.row_y(fk.source_columns[0]), dst.row_y(fk.target_columns[0])

    def lane_x(g: int, k: int) -> Coordinate:
        left = positions[g].x + positions[g].width
        width = gut[g]
        n = LANES[g]
        return left + width * (k + 1) / (n + 1)

    lanes = spec.lanes
    s_dir: Literal[-1, 1]
    d_dir: Literal[-1, 1]
    if ci == cj:
        side = spec.side
        if side is None or len(lanes) != 1:
            raise ValueError("A same-column route requires a side and one lane")
        g = ci - 1 if side == "L" else ci
        lx = lane_x(g, lanes[0])
        sx = src.x if side == "L" else src.x + src.w
        dx = dst.x if side == "L" else dst.x + dst.w
        pts = [(sx, sy), (lx, sy), (lx, dy), (dx, dy)]
        s_dir = d_dir = -1 if side == "L" else 1
    elif abs(ci - cj) == 1:
        if len(lanes) != 1:
            raise ValueError("An adjacent-column route requires one lane")
        g = min(ci, cj)
        lx = lane_x(g, lanes[0])
        if ci < cj:
            sx, dx, s_dir, d_dir = src.x + src.w, dst.x, 1, -1
        else:
            sx, dx, s_dir, d_dir = src.x, dst.x + dst.w, -1, 1
        pts = [(sx, sy), (lx, sy), (lx, dy), (dx, dy)]
    else:
        if len(lanes) != 2:
            raise ValueError("A multi-column route requires two lanes")
        if ci > cj:
            g1, g2 = ci - 1, cj
            sx, dx, s_dir, d_dir = src.x, dst.x + dst.w, -1, 1
        else:
            g1, g2 = ci, cj - 1
            sx, dx, s_dir, d_dir = src.x + src.w, dst.x, 1, -1
        l1, l2 = lane_x(g1, lanes[0]), lane_x(g2, lanes[1])
        pts = [(sx, sy), (l1, sy), (l1, channel_y), (l2, channel_y), (l2, dy), (dx, dy)]
    child: Literal["one", "many"] = "one" if spec.one_to_one else "many"
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
DESIGN_FKS: tuple[tuple[str, str, str], ...] = ()
DESIGN_SERIAL_PKS = [("evt.state_changes", "id"), ("out.deliveries", "id"), ("ops.llm_usage", "id")]


def integrity_gaps(tables: Mapping[str, RenderedTable], fks: Sequence[ForeignKeyLink]) -> list[str]:
    """Constraints the DDL leaves to application code, computed from the parsed schema."""
    single = {}
    for t in tables.values():
        keys = {g[0] for g in t.groups if len(g) == 1}
        keys |= {
            constraint.columns[0]
            for constraint in (entry.rule for entry in t.constraints)
            if constraint.kind == "PK" and len(constraint.columns) == 1
        }
        single[t.name] = keys
    declared = {(f.source_table, f.source_columns[0]) for f in fks}
    by_target: dict[str, int] = {}
    composite: list[str] = []
    blocked: list[str] = []
    for t in tables.values():
        for c in t.cols:
            if not c.logical_reference:
                continue
            if (t.name, c.name) in COMPOSITE:
                cols, dst, dcols = COMPOSITE[(t.name, c.name)]
                assert tuple(dcols) in tables[dst].groups
                composite.append(f"{t.name}({', '.join(cols)}) → {dst}")
                continue
            dst = c.logical_reference.rstrip("[]")
            if c.logical_reference.endswith("[]") or REF_TARGET[dst] not in single[dst]:
                blocked.append(f"{t.name}.{c.name}")
                continue
            by_target[dst] = by_target.get(dst, 0) + 1
    missing_design = [f"{s}.{c} → {d}" for s, c, d in DESIGN_FKS if (s, c) not in declared]
    serial = [f"{t}.{c}" for t, c in DESIGN_SERIAL_PKS if c not in single[t]]
    only_key = [
        t.name
        for t in tables.values()
        if single[t.name] & {"storage_key"}
        and not [g for g in t.groups if g != ("storage_key",)]
        and not any(
            constraint.kind == "PK" and constraint.columns != ("storage_key",)
            for constraint in (entry.rule for entry in t.constraints)
        )
    ]
    keyless = [
        t.name
        for t in tables.values()
        if not t.groups and not any(v.kind == "PK" for v in (entry.rule for entry in t.constraints))
    ]
    no_pk = [
        t.name
        for t in tables.values()
        if not any(v.kind == "PK" for v in (entry.rule for entry in t.constraints))
    ]
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


def render_pg(root: Path, base_ref: str | None = None) -> str:
    mig = sorted((root / "src/tank/migrations").glob("*.sql"))
    span = f"{mig[0].name[:3]}–{mig[-1].name[:3]}"
    parsed, schemas = parse_pg(p.read_text() for p in mig)
    tables = finish_pg(parsed)
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
        changed_rows = [c for t in drawn.values() for c in t.cols]
        before = sum(index_count(t) for t in base.values())
        after = sum(index_count(t) for t in tables.values())
        diff_line = (
            f"Diff {base_ref} → working tree: tables {len(base)} → {len(tables)}, "
            f"columns removed {sum(c.change == 'removed' for c in changed_rows)}, "
            f"added {sum(c.change == 'added' for c in changed_rows)}, "
            f"altered {sum(c.change == 'altered' for c in changed_rows)}, indexes {before} → {after}. "
            "Red: removed (struck through), added or altered; unchanged tables are faded."
        )

    margin, top = 40, 330
    channel_y = top - 22
    col_x: dict[int, Coordinate] = {}
    col_w: dict[int, Coordinate] = {}
    mxs: dict[int, PostgreSQLMeasurements] = {}
    x: Coordinate = margin
    for i, names in enumerate(layout):
        mx = pg_metrics([drawn[n] for n in names])
        title = max(tw(n, 12.5) * 1.05 + 70 for n in names)
        col_w[i] = max(mx.width, title)
        col_x[i] = x
        mxs[i] = mx
        x += col_w[i] + PG_GUTTERS[i]
    width = x - PG_GUTTERS[len(layout) - 1] + margin

    positions = {i: ColumnPosition(x=col_x[i], width=col_w[i]) for i in col_x}
    boxes: dict[str, Box] = {}
    col_of: dict[str, int] = {}
    body: list[str] = []
    bottoms: dict[int, Coordinate] = {}
    for i, names in enumerate(layout):
        y: Coordinate = top
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
        edges.append(route(fk, boxes, col_of, positions, PG_GUTTERS, channel_y, PG_ROUTES[fk.name], color))

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
    counts: dict[str, int] = {}
    for t in tables.values():
        counts[t.namespace] = counts.get(t.namespace, 0) + 1
    order = ["reg", "ingest", "evt", "ana", "out", "ops", "mkt", "qa", "scout"]
    for i, s in enumerate(order):
        schema_x = lx4 + 16 + (i // 3) * ((bw4 - 20) / 3)
        cy = ly + 40 + (i % 3) * 46
        fill, _, desc = SCHEMA[s]
        hdr.append(f'<rect x="{schema_x}" y="{cy}" width="16" height="16" rx="3" fill="{fill}"/>')
        hdr.append(
            text(
                schema_x + 24,
                cy + 12.5,
                f"<tspan font-weight='700'>{s}</tspan> · {counts.get(s, 0)} table{'' if counts.get(s, 0) == 1 else 's'}",
                12,
            )
        )
        hdr.append(text(schema_x + 24, cy + 28, esc(desc), 10, MUTED))

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
    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        *hdr,
        *edges,
        *body,
        *panel,
        "</svg>",
    ]
    return "\n".join(svg_parts)


CH_COLUMNS = [
    ["mkt.flow_bars_1m"],
    ["mkt.klines_1m", "mkt.liq_obs"],
    ["mkt.trades", "mkt.funding_obs"],
    ["mkt.book_summaries"],
    ["mkt.oi_obs", "mkt.tickers_24h"],
]
CH_WRITER = {"mkt.trades": "written by tank-capture · capture/internal/rawstore/trades.go"}


def ch_type(t: str) -> str:
    return t.replace("DateTime64(6, 'UTC')", "DateTime64(6,'UTC')")


def ch_rows(table: ClickHouseTable) -> tuple[ClickHouseRow, ...]:
    rows: list[ClickHouseRow] = []
    for column in table.cols:
        labels: list[str] = []
        if column.name in table.order:
            labels.append(f"SK{table.order.index(column.name) + 1}")
        if column.name == table.partition:
            labels.append("PT")
        if column.name == "_version":
            labels.append("VER")
        for index in table.indexes:
            if index.column == column.name:
                labels.append("BF" if index.kind == "bloom_filter" else "MM")
        hint = (
            {"numeric": "dec", "jsonb": "json"}.get(column.sql_kind)
            if isinstance(column, SQLDerivedColumn)
            else None
        )
        reference = {"instrument_id": "pg reg.instruments", "token_id": "pg reg.tokens"}.get(column.name)
        rows.append(
            ClickHouseRow(
                name=column.name,
                data_type=ch_type(column.data_type),
                labels=tuple(labels),
                hint=hint,
                precision=column.precision,
                reference=reference,
            )
        )
    return tuple(rows)


def ch_metrics(tables: Sequence[ClickHouseTable]) -> ClickHouseMeasurements:
    rows = [r for t in tables for r in ch_rows(t)]
    name = max(len(r.name) for r in rows)
    typ = max(len(r.data_type) for r in rows)
    name_x = 56
    type_x = name_x + name * CW * 10.8 + 14
    hint_x = type_x + typ * CW * 10 + 6
    ref_x = hint_x + 58
    width = ref_x + len("→ pg reg.instruments") * CW * 9.2 + 10
    return ClickHouseMeasurements(name_x=name_x, type_x=type_x, hint_x=hint_x, ref_x=ref_x, width=width)


CH_FILL = "#1F2328"
CH_ACCENT = "#FFCC00"
CH_TINT = "#FFFBEA"


def ch_table(
    t: ClickHouseTable,
    x: Coordinate,
    y: Coordinate,
    w: Coordinate,
    mx: ClickHouseMeasurements,
    retention: RetentionRule,
) -> tuple[Box, str]:
    rows = ch_rows(t)
    engine = [
        ("ENGINE", "ReplacingMergeTree(_version)"),
        ("PARTITION BY", f"toYYYYMMDD({t.partition})"),
        ("ORDER BY", f"({', '.join(t.order)})"),
        (
            "RETENTION",
            retention_text(retention),
        ),
    ]
    for index in t.indexes:
        engine.append(
            ("INDEX", f"{index.name} {index.column} TYPE {index.kind} GRANULARITY {index.granularity}")
        )
    if t.name in CH_WRITER:
        engine.append(("", CH_WRITER[t.name]))
    eng_h = 8 + len(engine) * 13
    box = Box(name=t.name, x=x, y=y, w=w, rows=tuple(rows), head_h=HEAD + eng_h)
    schema, short = t.name.split(".")
    out = [f'<g id="{esc(t.name)}">']
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
        f'<text x="{x + w - 8}" y="{y + 15.5}" font-family="{SANS}" font-size="9" fill="#A0AEC0" text-anchor="end">{len(t.cols) + sum(r.precision for r in rows) * 2} cols</text>'
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
        for lab in r.labels[:3]:
            g, bw = badge(bx, ry + 3, lab)
            out.append(g)
            bx += bw + 2
        weight = ' font-weight="700"' if any(lab.startswith("SK") for lab in r.labels) else ""
        nx = max(x + mx.name_x, bx + 3)
        out.append(
            f'<text x="{nx:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10.8" fill="{INK}"{weight}>{esc(r.name)}</text>'
        )
        tcolor = MUTED if r.data_type.startswith(("Nullable", "Array")) else "#2D3748"
        out.append(
            f'<text x="{x + mx.type_x:.1f}" y="{ry + 12.2}" font-family="{MONO}" font-size="10" fill="{tcolor}">{esc(r.data_type)}</text>'
        )
        hx = x + mx.hint_x
        if r.precision:
            out.append(text(hx, ry + 11.8, "+prec", 8, "#DD6B20", 600))
        elif r.hint:
            out.append(text(hx, ry + 11.8, r.hint, 8, "#805AD5", 700))
        if r.reference:
            out.append(
                text(
                    x + mx.ref_x, ry + 12, esc("→ " + r.reference), 9.2, "#3182CE", None, MONO, None, "italic"
                )
            )
    out.append("</g>")
    return box, "".join(out)


def render_ch(root: Path) -> str:
    tables = ch_model(root)
    raw_configuration: object = yaml.safe_load((root / "config/thresholds.yaml").read_text())
    configuration = RetentionConfiguration.model_validate(raw_configuration)
    retentions: dict[str, RetentionRule] = {}
    for table in tables:
        if table.name == "mkt.book_summaries":
            retentions[table.name] = BookRetention(
                kind="BOOK", hours=configuration.book_summary_retention_hours
            )
        elif table.name == "mkt.trades":
            retentions[table.name] = TradesRetention(kind="TRADES", days=configuration.trades_retention_days)
        else:
            retentions[table.name] = BaseRetention(kind="BASE", days=configuration.base_retention_days)
    by = {t.name: t for t in tables}
    assert {n for c in CH_COLUMNS for n in c} == set(by)
    margin, top, gutter = 40, 330, 30
    col_x: dict[int, Coordinate] = {}
    col_w: dict[int, Coordinate] = {}
    mxs: dict[int, ClickHouseMeasurements] = {}
    x: Coordinate = margin
    for i, names in enumerate(CH_COLUMNS):
        mx = ch_metrics([by[n] for n in names])
        col_w[i], col_x[i], mxs[i] = mx.width, x, mx
        x += mx.width + gutter
    width = x - gutter + margin
    body: list[str] = []
    bottoms: dict[int, Coordinate] = {}
    for i, names in enumerate(CH_COLUMNS):
        y: Coordinate = top
        for n in names:
            box, svg = ch_table(by[n], col_x[i], y, col_w[i], mxs[i], retentions[n])
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

    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">',
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        *hdr,
        *body,
        *panel,
        "</svg>",
    ]
    return "\n".join(svg_parts)


class RendererArguments(SchemaRecord):
    root: Path
    check: bool
    png: bool
    diff: Annotated[list[NonBlankText], Field(min_length=2, max_length=2)] | None


def main() -> int:
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
    args = RendererArguments.model_validate(vars(parser.parse_args()))
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
