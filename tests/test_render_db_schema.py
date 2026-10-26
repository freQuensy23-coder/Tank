from pathlib import Path

import pytest

from scripts.render_db_schema import (
    ForeignKeyConstraint,
    literal_assignments,
    new_table,
    parse_constraint,
    render_ch,
    render_pg,
)


def test_foreign_key_accepts_sql_whitespace_and_preserves_composite_scope() -> None:
    table = new_table("ana.decision_state_heads")
    parse_constraint(
        table,
        "CONSTRAINT current_scope FOREIGN KEY (event_id, token_id, dec_id) "
        "REFERENCES ana.decisions (event_id, token_id, dec_id) DEFERRABLE INITIALLY DEFERRED",
    )
    constraint = table.constraints["current_scope"]
    assert isinstance(constraint, ForeignKeyConstraint)
    assert constraint.columns == ("event_id", "token_id", "dec_id")
    assert constraint.target_table == "ana.decisions"
    assert constraint.target_columns == ("event_id", "token_id", "dec_id")
    assert constraint.options == "DEFERRABLE INITIALLY DEFERRED"
    with pytest.raises(ValueError, match="Unsupported foreign key constraint syntax"):
        parse_constraint(table, "FOREIGN KEY (event_id) REFERENCES missing_scope")


def test_literal_schema_reader_preserves_annotated_metadata_without_execution(tmp_path: Path) -> None:
    source = tmp_path / "schema.py"
    source.write_text(
        'MARKET_COLUMNS: dict[str, str] = {"id": "text"}\nraise RuntimeError("not executable")\n'
    )
    assert literal_assignments(source, {"MARKET_COLUMNS"}) == {"MARKET_COLUMNS": {"id": "text"}}


def test_current_diagrams_cover_actual_decision_heads_and_market_schema() -> None:
    root = Path(__file__).resolve().parents[1]
    postgres = render_pg(root)
    clickhouse = render_ch(root)
    assert "decision_state_heads" in postgres
    assert "revision-chain guard" in postgres
    assert "flow_bars_1m" in clickhouse
    assert postgres == (root / "diagrams/tank_postgres_schema.svg").read_text()
    assert clickhouse == (root / "diagrams/tank_clickhouse_schema.svg").read_text()


def test_svg_route_geometry_preserves_endpoints_and_bounds_corner_radius() -> None:
    from scripts.render_db_schema import rounded

    assert rounded([(0, 0), (10, 0), (10, 20)]) == ("M0.0,0.0 L5.0,0.0 Q10.0,0.0 10.0,5.0 L10.0,20.0")
    assert rounded([(0, 0), (0, 0), (10, 0)]) == "M0.0,0.0 L0.0,0.0 L10.0,0.0"
    with pytest.raises(ValueError, match="requires at least two points"):
        rounded([(0, 0)])


def test_svg_text_omits_unrequested_attributes_and_escapes_column_text() -> None:
    from xml.etree.ElementTree import fromstring

    from scripts.render_db_schema import esc, text

    implicit = fromstring(text(1, 2, esc('a&b < "quoted"')))
    assert implicit.text == 'a&b < "quoted"'
    assert "font-weight" not in implicit.attrib
    assert "text-anchor" not in implicit.attrib
    assert "font-style" not in implicit.attrib
    explicit = fromstring(text(1, 2, "Label", weight=600, anchor="end", style="italic"))
    assert explicit.attrib["font-weight"] == "600"
    assert explicit.attrib["text-anchor"] == "end"
    assert explicit.attrib["font-style"] == "italic"


def test_sql_constraint_refusal_does_not_install_partial_scope() -> None:
    table = new_table("ana.decision_state_heads")
    with pytest.raises(ValueError, match="arity must match"):
        parse_constraint(
            table,
            "CONSTRAINT wrong_scope FOREIGN KEY (event_id, token_id) REFERENCES ana.decisions (event_id)",
        )
    assert "wrong_scope" not in table.constraints
    with pytest.raises(ValueError, match="columns must be unique"):
        parse_constraint(table, "CONSTRAINT repeated UNIQUE (event_id, event_id)")
    assert "repeated" not in table.constraints


def test_constraint_flags_and_column_scope_are_strict_and_immutable() -> None:
    from pydantic import ValidationError

    from scripts.render_db_schema import CheckConstraint, KeyConstraint

    with pytest.raises(ValidationError):
        CheckConstraint.model_validate({"kind": "CK", "columns": (), "valid": 1})
    with pytest.raises(ValidationError):
        KeyConstraint.model_validate({"kind": "PK", "columns": ()})
    with pytest.raises(ValidationError):
        CheckConstraint.model_validate({"kind": "REQ", "columns": (), "valid": True})


@pytest.mark.parametrize(("attribute", "value"), [("columns", ("token_id",)), ("kind", "UK")])
def test_runtime_constraint_mutations_are_refused(attribute: str, value: str | tuple[str, ...]) -> None:
    from pydantic import ValidationError

    from scripts.render_db_schema import KeyConstraint

    parsed = KeyConstraint(kind="PK", columns=("event_id",))
    with pytest.raises(ValidationError, match="frozen"):
        setattr(parsed, attribute, value)


@pytest.mark.parametrize(
    "changes",
    [{"width": 0}, {"width": float("inf")}, {"name_x": float("nan")}, {"ref_x": True}],
)
def test_layout_measurements_refuse_invalid_geometry(changes: dict[str, int | float]) -> None:
    from pydantic import ValidationError

    from scripts.render_db_schema import PostgreSQLMeasurements

    raw = {"name_x": 1, "type_x": 2, "prec_x": 3, "nn_x": 4, "ref_x": 5, "width": 6}
    with pytest.raises(ValidationError):
        PostgreSQLMeasurements.model_validate({**raw, **changes})


def test_market_metadata_does_not_invent_missing_identity_columns() -> None:
    from pydantic import ValidationError

    from scripts.render_db_schema import MarketSchemaMetadata

    with pytest.raises(ValidationError, match="undeclared column"):
        MarketSchemaMetadata.model_validate(
            {
                "MARKET_COLUMNS": {"mkt.example": {"ts": "timestamptz"}},
                "MARKET_IDENTITIES": {"mkt.example": ("missing",)},
            }
        )


def test_partial_timestamp_precision_metadata_is_not_rendered_as_complete() -> None:
    from scripts.render_db_schema import precision_companions

    with pytest.raises(ValueError, match="requires both companion columns"):
        precision_companions("ts", "DateTime64(6,'UTC')", {"ts", "ts_prec"})
    assert not precision_companions("ts", "DateTime64(6,'UTC')", {"ts"})
    assert precision_companions("ts", "DateTime64(6,'UTC')", {"ts", "ts_prec", "ts_prec_reason"})


def test_clickhouse_input_refuses_an_unmapped_declared_sql_type() -> None:
    from pydantic import ValidationError

    from scripts.render_db_schema import ClickHouseInputs, ClickHouseMetadata, MarketSchemaMetadata

    market = MarketSchemaMetadata(
        MARKET_COLUMNS={"mkt.example": {"ts": "timestamptz"}},
        MARKET_IDENTITIES={"mkt.example": ("ts",)},
    )
    backend = ClickHouseMetadata(
        MARKET_TABLES={"mkt.example": "ts"},
        TRADES="mkt.trades",
        TRADE_COLUMNS={"ts": "DateTime64(6,'UTC')"},
        TYPES={"text": "String"},
        ZSTD="CODEC(ZSTD(3))",
        TIME_CODEC="CODEC(DoubleDelta,ZSTD(3))",
        VERSION_CODEC="CODEC(Delta,ZSTD(3))",
    )
    with pytest.raises(ValidationError, match="explicit type mapping"):
        ClickHouseInputs(market=market, backend=backend)


def test_renderer_uses_declared_retention_values_without_policy_fallback(tmp_path: Path) -> None:
    import shutil

    import yaml
    from pydantic import ValidationError

    from scripts.render_db_schema import RetentionConfiguration

    root = Path(__file__).resolve().parents[1]
    for name in ("market_schema.py", "clickhouse_market.py"):
        target = tmp_path / "src/tank" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / "src/tank" / name, target)
    actual = RetentionConfiguration.model_validate(
        yaml.safe_load((root / "config/thresholds.yaml").read_text())
    )
    selected = {
        "base_retention_days": actual.base_retention_days + 1,
        "trades_retention_days": actual.trades_retention_days + 1,
        "book_summary_retention_hours": actual.book_summary_retention_hours + 1,
    }
    (tmp_path / "config").mkdir()
    (tmp_path / "config/thresholds.yaml").write_text(yaml.safe_dump(selected))
    rendered = render_ch(tmp_path)
    assert f"kept {selected['base_retention_days']} days (base_retention_days)" in rendered
    assert f"kept {selected['trades_retention_days']} days (trades_retention_days)" in rendered
    assert f"kept {selected['book_summary_retention_hours']} h (book_summary_retention_hours)" in rendered
    selected["base_retention_days"] = 0
    (tmp_path / "config/thresholds.yaml").write_text(yaml.safe_dump(selected))
    assert "kept 0 days (base_retention_days)" in render_ch(tmp_path)
    with pytest.raises(ValidationError):
        RetentionConfiguration.model_validate({"base_retention_days": 90})


def test_partial_unique_index_does_not_claim_unconditional_identity() -> None:
    from scripts.render_db_schema import finish_pg, index_count, parse_pg

    tables, _ = parse_pg(
        iter(
            [
                "CREATE TABLE qa.example (id text, active boolean);"
                "CREATE UNIQUE INDEX active_id ON qa.example (id) WHERE active;"
            ]
        )
    )
    table = finish_pg(tables)["qa.example"]
    assert table.unique_indexes[0].predicate == "active"
    assert table.groups == ()
    assert table.col("id").unique_marker is None
    assert index_count(table) == 1


def test_sql_column_nullability_and_sequence_changes_are_independent_diff_facts() -> None:
    from scripts.render_db_schema import diff_tables, finish_pg, parse_pg

    before, _ = parse_pg(iter(["CREATE TABLE qa.example (id bigint);"]))
    after, _ = parse_pg(
        iter(["CREATE TABLE qa.example (id bigint NOT NULL DEFAULT nextval('qa.example_id_seq'));"])
    )
    drawn, removed = diff_tables(finish_pg(before), finish_pg(after))
    column = drawn["qa.example"].col("id")
    assert not removed
    assert column.change == "altered"
    assert column.not_null
    assert column.sequence_default
    assert column.sql_type == "int8"


def test_rendered_column_does_not_coerce_missing_flags_or_boolean_numbers() -> None:
    from pydantic import ValidationError

    from scripts.render_db_schema import SQLColumn

    with pytest.raises(ValidationError):
        SQLColumn.model_validate({"name": "id", "sql_type": "text"})
    with pytest.raises(ValidationError):
        SQLColumn.model_validate(
            {"name": "id", "sql_type": "text", "not_null": 1, "sequence_default": False, "precision": False}
        )
