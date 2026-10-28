from collections import Counter
from copy import deepcopy
from datetime import timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_EVEN, Decimal, Inexact, Rounded, localcontext
from weakref import ref

import pytest

import tank.analysis.truth as truth
from tank.util import iso, parse_time


ORIGIN = parse_time("2026-10-20T00:00:00Z")
CATEGORY = "oi_native_single_sided"


def _at(minute):
    return ORIGIN + timedelta(minutes=minute)


def _cohort():
    return [
        (
            {"instrument_id": f"{venue}:perp:T", "venue": venue},
            [
                {"ts": iso(_at(minute)), "oi_native": native, "oi_usd": usd}
                for minute in range(12)
            ],
            CATEGORY,
        )
        for venue, native, usd in [("binance", "100", "1000"), ("bybit", "200", "2000")]
    ]


def _flat_expected(start=8, end=11, *, native=Decimal(300), usd=Decimal(3000), venues=None):
    return {
        "requested_window_duration_s": (end - start) * 60.0,
        "observed_window_duration_s": (end - start) * 60.0,
        "native_oi_vs_normal": "WITHIN",
        "precision": None,
        "oi_native_start": native,
        "oi_native_end": native,
        "oi_native_chg_pct": 0.0,
        "oi_usd_start": usd,
        "oi_usd_end": usd,
        "oi_usd_chg_pct": 0.0 if usd is not None else None,
        "oi_usd_reason": None if usd is not None else "INCOMPLETE",
        "oi_usd_detail": None if usd is not None else "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
        "decomposition": (
            {"position": Decimal(0), "markup": Decimal(0), "interaction": Decimal(0), "pos_share": None}
            if usd is not None
            else {
                "position": None,
                "markup": None,
                "interaction": None,
                "pos_share": None,
                "reason": "INCOMPLETE",
                "detail": "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
            }
        ),
        "oi_class": "MIXED" if usd is not None else None,
        "classification_ready": usd is not None,
        "native_normal_range": [0.0, 0.0],
        "usd_normal_range": [0.0, 0.0],
        "native_beyond": False,
        "usd_beyond": False if usd is not None else None,
        "start_observed_at": iso(_at(start)),
        "end_observed_at": iso(_at(end)),
        "venues": ["binance", "bybit"] if venues is None else venues,
    }


@pytest.fixture
def assessment():
    variables = [
        (truth._ASSESSMENT_INPUTS, {}),
        (truth._TIMES, {}),
        (truth._BASELINE_CACHE, None),
        (truth._BASELINE_DEPENDENCY, None),
        (truth._PARAMETERS, {"history_min_days": 0, "oi_sync_tolerance_s": 0}),
    ]
    tokens = [(variable, variable.set(value)) for variable, value in variables]
    try:
        with localcontext():
            yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


@pytest.fixture
def measured(monkeypatch):
    counts = Counter()
    original_rows = {}
    original_time = truth._time

    def counted_time(row):
        if original_rows.get(id(row)) is row:
            counts["original_time"] += 1
        return original_time(row)

    monkeypatch.setattr(truth, "_time", counted_time)

    def delegate(name, key):
        original = getattr(truth, name)

        def counted(*args, **kwargs):
            counts[key] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(truth, name, counted)

    for name, key in [
        ("bisect_left", "selection"),
        ("_sum", "sums"),
        ("decimal", "decimal"),
        ("_composite_history", "history"),
        ("_oi_stats", "stats"),
    ]:
        delegate(name, key)

    def run(sync, start=8, end=11):
        original_rows.clear()
        original_rows.update((id(row), row) for _, rows, _ in sync for row in rows)
        counts.clear()
        result = truth.synchronized_oi(sync, _at(start), _at(end))
        return result, counts.copy()

    return run


def _assert_structural_hit(cold, warm):
    assert warm["sums"] == cold["sums"]
    assert warm["decimal"] == cold["decimal"]
    assert warm["history"] == cold["history"] == 1
    assert warm["stats"] == cold["stats"] == 1
    assert warm["original_time"] < cold["original_time"], (cold, warm)
    assert warm["selection"] < cold["selection"], (cold, warm)


def test_preparation_reuses_real_alignment_without_reusing_numeric_work(assessment, measured):
    sync = _cohort()
    before = deepcopy(sync)
    first, cold = measured(sync)
    middle, _ = measured([(dict(pack), rows, category) for pack, rows, category in sync], start=9)
    last, warm = measured([(dict(pack), rows, category) for pack, rows, category in sync])
    assert first == last == _flat_expected()
    assert middle == _flat_expected(start=9)
    assert sync == before
    assert cold["sums"] == warm["sums"] == 24
    last["venues"].append("not-an-input-venue")
    assert first == _flat_expected()
    _assert_structural_hit(cold, warm)


@pytest.mark.parametrize("change", ["order", "cohort", "end", "tolerance", "row_list_identity"])
def test_preparation_keys_do_not_alias_other_alignment_inputs(assessment, measured, change):
    sync = _cohort()
    primed, _ = measured(sync)
    assert primed == _flat_expected()
    candidate = sync
    end = 11
    expected = _flat_expected()
    if change == "order":
        candidate = list(reversed(sync))
        expected = _flat_expected(venues=["bybit", "binance"])
    elif change == "cohort":
        candidate = sync[:1]
        expected = _flat_expected(native=Decimal(100), usd=Decimal(1000), venues=["binance"])
    elif change == "end":
        end = 10
        expected = _flat_expected(end=10)
    elif change == "tolerance":
        truth._PARAMETERS.get()["oi_sync_tolerance_s"] = 1
    else:
        candidate = [(dict(pack), list(rows), category) for pack, rows, category in sync]
    token = truth._ASSESSMENT_INPUTS.set(None)
    try:
        reference, uncached = measured(candidate, end=end)
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
    changed, miss = measured(candidate, end=end)
    repeated, hit = measured(candidate, end=end)
    assert reference == changed == repeated == expected
    assert miss["original_time"] == uncached["original_time"]
    assert miss["selection"] == uncached["selection"]
    _assert_structural_hit(miss, hit)


@pytest.mark.parametrize("scope", ["absent", "fresh_assessment"])
def test_preparation_does_not_escape_its_assessment(assessment, measured, scope):
    sync = _cohort()
    first_token = truth._ASSESSMENT_INPUTS.set(None if scope == "absent" else {})
    try:
        first, cold = measured(sync)
    finally:
        truth._ASSESSMENT_INPUTS.reset(first_token)
    second_token = truth._ASSESSMENT_INPUTS.set(None if scope == "absent" else {})
    try:
        second, rebuilt = measured(sync)
    finally:
        truth._ASSESSMENT_INPUTS.reset(second_token)
    assert first == second == _flat_expected()
    assert rebuilt == cold


class _RetainedRows(list):
    pass


def test_preparation_retains_original_lists_only_for_its_assessment(assessment, measured):
    sync = [(pack, _RetainedRows(rows), category) for pack, rows, category in _cohort()]
    references = [ref(rows) for _, rows, _ in sync]
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        result, _ = measured(sync)
        assert result == _flat_expected()
        del sync
        assert all(reference() is not None for reference in references)
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
    assert all(reference() is None for reference in references)


def test_new_assessment_reconstructs_mutated_original_rows(assessment, measured):
    sync = _cohort()
    first_token = truth._ASSESSMENT_INPUTS.set({})
    try:
        first, _ = measured(sync)
    finally:
        truth._ASSESSMENT_INPUTS.reset(first_token)
    for row in sync[0][1]:
        row.update(oi_native="400", oi_usd="4000")
    second_token = truth._ASSESSMENT_INPUTS.set({})
    try:
        second, _ = measured(sync)
    finally:
        truth._ASSESSMENT_INPUTS.reset(second_token)
    assert first == _flat_expected()
    assert second == _flat_expected(native=Decimal(600), usd=Decimal(6000))


def test_each_window_readmits_venues_before_reusing_preparation(assessment, measured):
    sync = _cohort()
    sync[1][1][:] = sync[1][1][6:]
    recent, cold = measured(sync)
    older, _ = measured(sync, start=2)
    repeated, warm = measured(sync)
    assert recent == repeated == _flat_expected()
    assert older == _flat_expected(start=2, native=Decimal(100), usd=Decimal(1000), venues=["binance"])
    assert cold["sums"] == warm["sums"] == 12
    _assert_structural_hit(cold, warm)


@pytest.mark.parametrize("category", [None, "different_native_unit"])
def test_cached_alignment_cannot_bypass_category_admission(assessment, measured, category):
    sync = _cohort()
    first, _ = measured(sync)
    incompatible = [sync[0], (dict(sync[1][0]), sync[1][1], category)]
    rejected, work = measured(incompatible)
    restored, _ = measured(sync)
    assert first == restored == _flat_expected()
    assert rejected is None
    assert work["sums"] == work["stats"] == work["history"] == 0


@pytest.mark.parametrize(
    ("precision", "rounding", "trap", "final_precision"),
    [(7, ROUND_FLOOR, False, 414), (768, ROUND_HALF_EVEN, True, 768)],
)
def test_structural_hits_replay_decimal_context_effects(
    assessment, measured, precision, rounding, trap, final_precision
):
    sync = _cohort()
    for row in sync[0][1]:
        row.update(oi_native=Decimal("1e150"), oi_usd="1e151")
    for row in sync[1][1]:
        row.update(oi_native={"value": "1e-40"}, oi_usd="1e-39")
    with localcontext() as context:
        context.prec = 512
        native = Decimal("1e150") + Decimal("1e-40")
        usd = Decimal("1e151") + Decimal("1e-39")
    expected = _flat_expected(native=native, usd=usd)

    def observe():
        with localcontext() as context:
            context.prec = precision
            context.rounding = rounding
            context.traps[Inexact] = trap
            context.clear_flags()
            context.flags[Rounded] = True
            result, work = measured(sync)
            state = (
                context.prec,
                context.rounding,
                context.Emin,
                context.Emax,
                context.capitals,
                context.clamp,
                dict(context.flags),
                dict(context.traps),
            )
            assert context.prec == final_precision
            assert context.flags[Rounded] is True
            assert context.flags[Inexact] is False
            assert context.traps[Inexact] is trap
            return result, work, state

    token = truth._ASSESSMENT_INPUTS.set(None)
    try:
        reference, uncached, reference_state = observe()
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
    first, cold, cold_state = observe()
    repeated, warm, warm_state = observe()
    assert reference == first == repeated == expected
    assert reference_state == cold_state == warm_state
    assert cold["decimal"] == warm["decimal"] == uncached["decimal"]
    assert cold["sums"] == warm["sums"] == uncached["sums"] == 24
    for result in (reference, first, repeated):
        assert result["oi_native_start"].as_tuple() == native.as_tuple()
        assert result["oi_usd_start"].as_tuple() == usd.as_tuple()
    _assert_structural_hit(cold, warm)


def test_missing_usd_and_unresolved_rows_survive_structural_reuse(assessment, measured):
    sync = _cohort()
    for row in sync[1][1]:
        row["oi_usd"] = None
    sync[0][1].extend(
        [
            {
                "ts": iso(_at(8)),
                "ts_prec": "BUCKET(60s)",
                "ts_bucket_anchor": "UNSPECIFIED",
                "oi_native": "999999",
                "oi_usd": "9999990",
            },
            {"ts": iso(_at(12)), "oi_native": "999999", "oi_usd": "9999990"},
        ]
    )
    first, cold = measured(sync)
    repeated, warm = measured(sync)
    assert first == repeated == _flat_expected(usd=None)
    assert cold["sums"] == warm["sums"] == 24
    _assert_structural_hit(cold, warm)


def test_alignment_retains_boundary_ties_and_excludes_future_candidates(assessment, measured):
    sync = _cohort()
    for index, row in enumerate(sync[1][1]):
        native = "210" if index == 1 else "990" if index == 2 else "200"
        row.update(
            ts=iso(_at(index) + timedelta(seconds=30)),
            oi_native=native,
            oi_usd=str(int(native) * 10),
        )
    sync[1][1][-1].update(oi_native="999999", oi_usd="9999990")
    truth._PARAMETERS.get().update(oi_sync_tolerance_s=30, history_min_days=999)
    first, cold = measured(sync, start=2)
    repeated, warm = measured(sync, start=2)
    expected = _flat_expected(start=2)
    expected.update(
        native_oi_vs_normal=None,
        oi_native_start=Decimal(310),
        oi_native_chg_pct=pytest.approx(-1 / 31),
        oi_usd_start=Decimal(3100),
        oi_usd_chg_pct=pytest.approx(-1 / 31),
        decomposition={
            "position": Decimal(-100),
            "markup": Decimal(0),
            "interaction": Decimal(0),
            "pos_share": 1.0,
        },
        oi_class=None,
        classification_ready=False,
        native_normal_range=None,
        usd_normal_range=None,
        native_beyond=None,
        usd_beyond=None,
    )
    assert first == repeated == expected
    _assert_structural_hit(cold, warm)
