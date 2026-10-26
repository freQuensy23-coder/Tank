from pathlib import Path

import pytest

from scripts.render_db_schema import Table, literal_assignments, parse_constraint, render_ch, render_pg


def test_foreign_key_accepts_sql_whitespace_and_preserves_composite_scope() -> None:
    table = Table("ana.decision_state_heads")
    parse_constraint(
        table,
        "CONSTRAINT current_scope FOREIGN KEY (event_id, token_id, dec_id) "
        "REFERENCES ana.decisions (event_id, token_id, dec_id) DEFERRABLE INITIALLY DEFERRED",
    )
    assert table.constraints["current_scope"] == (
        "FK",
        ["event_id", "token_id", "dec_id"],
        "ana.decisions",
        ["event_id", "token_id", "dec_id"],
        "DEFERRABLE INITIALLY DEFERRED",
    )
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
