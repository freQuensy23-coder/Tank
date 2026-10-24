"""Registry sector roles and baskets survive identity masking without name guesses."""

import copy
from pathlib import Path

import pytest
import yaml
from test_truth_start_fixture import compact_start_snapshot

import tank.analysis.truth as truth
from tank.util import parse_time

ROOT = Path(__file__).resolve().parents[1]
registry = yaml.safe_load((ROOT / "config/registry.yaml").read_text())


def packet_for(token):
    source = compact_start_snapshot()

    def rename(value):
        if isinstance(value, str):
            return token["token_id"] if value == "T" else value.replace(":spot:T", ":spot:" + token["symbol"])
        if isinstance(value, list):
            return [rename(x) for x in value]
        if isinstance(value, dict):
            return {k: rename(v) for k, v in value.items()}
        return value

    packet = rename(source)
    packet["tokens"] = copy.deepcopy(registry["tokens"])

    for domain in ("oi", "funding", "books", "semantics"):
        packet.pop(domain, None)

    first_venue = "A:spot:" + token["symbol"]
    peers = [
        row
        for row in packet["klines"]
        if row["token_id"] == token["token_id"] and row["instrument_id"] == first_venue
    ]
    end = parse_time(packet["cutoff"])
    boundaries = {end - truth.timedelta(seconds=seconds) for seconds in (0, 900, 3600, 14400, 86400)}
    boundaries.add(parse_time(packet["anchor"]["ts"]))
    for index, peer in enumerate(registry["tokens"]):
        if peer["token_id"] == token["token_id"]:
            continue
        for original in peers:
            close = parse_time(original["open_time"]) + truth.timedelta(seconds=original["interval_s"])

            if peer["symbol"] not in {"BTC", "ETH"} and close not in boundaries:
                continue
            row = dict(original, token_id=peer["token_id"], instrument_id="peer:spot:" + peer["symbol"])

            elapsed = (parse_time(row["open_time"]) - parse_time(packet["anchor"]["ts"])).total_seconds()
            row["c"] = str(float(row["c"]) * (1 + (index + 1) * elapsed / 1e9))
            packet["klines"].append(row)
    return packet


@pytest.mark.parametrize("symbol", ["WLD", "UNI", "AVAX", "ENA"])
@pytest.mark.parametrize("explicit_baskets", [False, True])
def test_identity_mask_preserves_all_relative_values_coverage_and_reasons(symbol, explicit_baskets) -> None:
    token = next(t for t in registry["tokens"] if t["symbol"] == symbol)
    packet = packet_for(token)
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    if explicit_baskets:
        packet["sector_baskets"] = copy.deepcopy(registry["sector_baskets"])
        for row in packet["tokens"]:
            row.pop("sector", None)
    original = truth.relative_truth(packet, start, end)
    before, after = token["token_id"], "masked-asset-k"

    def mask(v):
        if isinstance(v, str):
            return v.replace(before, after).replace(symbol, "ASSETK")
        if isinstance(v, list):
            return [mask(x) for x in v]
        if isinstance(v, dict):
            return {mask(k): mask(x) for k, x in v.items()}
        return v

    assert truth.relative_truth(mask(packet), start, end) == mask(original)


def test_explicit_captured_basket_controls_measured_peer_returns() -> None:
    token = next(t for t in registry["tokens"] if t["symbol"] == "UNI")
    packet = packet_for(token)
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    implicit = truth.relative_truth(packet, start, end)
    packet["sector_baskets"] = copy.deepcopy(registry["sector_baskets"])
    assert truth.relative_truth(packet, start, end) == implicit

    basket = next(b for b in packet["sector_baskets"] if b["sector"] == "DEX_EXECUTION")
    basket["constituents"] = [token["token_id"], "aave:AAVE", "morpho:MORPHO"]
    changed = truth.relative_truth(packet, start, end)
    assert changed["windows"]["EVENT"]["r_sector_adj"] != implicit["windows"]["EVENT"]["r_sector_adj"]


@pytest.mark.parametrize("token", registry["tokens"], ids=lambda r: r["symbol"])
def test_authoritative_full_baskets_work_when_optional_token_sector_is_omitted(token) -> None:
    packet = packet_for(token)
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    expected = truth.relative_truth(packet, start, end)
    assert expected["windows"]["EVENT"]["r_raw"] is not None
    if token["sector"] == "IDENTITY_PAYMENTS":
        assert expected["windows"]["EVENT"]["sector_reason"] == "NOT_APPLICABLE"
    else:
        assert expected["windows"]["EVENT"]["r_sector_adj"] is not None
    packet["sector_baskets"] = copy.deepcopy(registry["sector_baskets"])
    for row in packet["tokens"]:
        row.pop("sector", None)
    assert truth.relative_truth(packet, start, end) == expected


def test_missing_sector_and_basket_metadata_is_not_guessed_from_wld_symbol() -> None:
    token = next(t for t in registry["tokens"] if t["symbol"] == "WLD")
    packet = packet_for(token)
    for row in packet["tokens"]:
        row.pop("sector", None)
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    result = truth.relative_truth(packet, start, end)
    assert result["windows"]["EVENT"]["r_raw"] is not None
    assert result["windows"]["EVENT"]["r_sector_adj"] is None
    assert result["windows"]["EVENT"]["sector_reason"] == "NOT_ESTIMABLE"
