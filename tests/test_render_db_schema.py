from pathlib import Path

import pytest

from scripts.render_db_schema import (
    ForeignKeyConstraint,
    Table,
    literal_assignments,
    parse_constraint,
    render_ch,
    render_pg,
)


def test_foreign_key_accepts_sql_whitespace_and_preserves_composite_scope() -> None:
    table = Table("ana.decision_state_heads")
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
    table = Table("ana.decision_state_heads")
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
