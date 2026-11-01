from decimal import Decimal, localcontext

import pytest

from tank import clickhouse_market as market


@pytest.mark.parametrize("capitals", [0, 1])
def test_canonical_money_text_keeps_exact_standard_decimal_spelling(capitals):
    texts = ["", ".12", "12.", "+1.0", "00.1", "-00", "1E-8", "NaN", "Infinity", "١٢.٣"]
    texts.extend(("0E-7", "0E-8", "0E-100", "0E-101", "0E-6", "0E-007", "0e-8", "-0E-8", "0E-٨"))
    for sign in ("", "-"):
        for integer in ("0", "1", "123456789"):
            for zeros in range(12):
                texts.extend((sign + integer + "." + "0" * zeros + "12", sign + integer + "." + "0" * zeros))
    admitted = 0
    with localcontext() as context:
        context.prec = 9
        context.capitals = capitals
        context.clear_flags()
        before = dict(context.flags)
        for text in texts:
            result = market._canonical_numeric_text(text)
            if result is not None:
                assert result is text
                assert str(Decimal(text)) == text
                admitted += 1
        assert admitted >= 100
        assert dict(context.flags) == before


def test_column_decoder_avoids_only_redundant_canonical_decimal_constructors(monkeypatch):
    texts = ["12.3400", "0.00", "-0.000000", "0.0000010", "0.00000010", "0001.00", "1E-8"]
    columns = {
        "ts": ["2026-10-20T00:00:00Z"] * len(texts),
        "oi_native": texts,
        "instrument_id": ["canonical-test-instrument"] * len(texts),
        "storage_key": [f"canonical-{index}" for index in range(len(texts))],
    }
    with localcontext() as context:
        context.prec = 9
        context.clear_flags()
        expected = market._wide_row_records("mkt.oi_obs", columns, "ts")
        flags = dict(context.flags)
    observed = []

    def counted(value):
        observed.append(value)
        return Decimal(value)

    monkeypatch.setattr(market, "Decimal", counted)
    with localcontext() as context:
        context.prec = 9
        context.clear_flags()
        actual = market._column_wide_records("mkt.oi_obs", columns, "ts")
        assert actual == expected
        assert dict(context.flags) == flags
    assert observed == texts[4:]
