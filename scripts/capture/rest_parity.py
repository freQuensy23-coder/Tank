"""REST parity harness: recorded REST poll cycles through the Python reference.

Each fixture (capture/testdata/rest/*.json.gz, scripts/capture/record_rest.py) holds the
exact REST exchanges of MarketService.discover/poll_once and one trades page per
instrument. The responses are served back to tank.market (VenueAdapter, _request's
Hyperliquid cache, _fx_rate, poll_once, _store_kline, _ticker, _funding, save_mark,
normalize.oi/kline/trade) with the clock at the recording time, and to tank-capture
(`capture-parity rest FIXTURE`). Both must produce identical typed rows, fingerprints,
REST-normalized trades and gaps.

  --write          regenerate capture/testdata/parity/rest-<fixture>.json.gz (digests)
  --check          Python output still matches the committed digests (CI)
  --go BINARY      run `BINARY rest <fixture>` and diff full outputs
  --dump FIXTURE   print the full Python output of one fixture

Run: uv run python scripts/capture/rest_parity.py --check [--go capture/bin/capture-parity]
"""

import argparse
import asyncio
import copy
import gzip
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

from parity import (
    CODE_VERSION,
    CONFIG_VERSION,
    EXPECTED,
    ROOT,
    Clock,
    compare,
    digests,
    instruments_of,
    setup_config,
)

REST = ROOT / "capture" / "testdata" / "rest"
TIME_PARAMS = {"startTime", "endTime", "start", "end", "since", "after", "before"}


def selector(method, url, params, body):
    """The request a response answers: time-valued parameters vary with the clock."""
    parts = [method.upper(), str(httpx.URL(url).copy_with(query=None))]
    parts += [f"{k}={params[k]}" for k in sorted(params) if k not in TIME_PARAMS]
    if body is not None:
        parts += [str(body.get("type")), str(body.get("coin"))]
        if isinstance(body.get("req"), dict):
            parts.append(str(body["req"].get("coin")))
    return " ".join(parts)


class Replay:
    """httpx.AsyncClient.request serving the recorded responses in recorded order."""

    def __init__(self, exchanges):
        self.by_key, self.served = {}, {}
        for ex in exchanges:
            self.by_key.setdefault(selector(ex["method"], ex["url"], ex["params"], ex["json"]), []).append(ex)

    async def request(self, method, url, **kwargs):
        params = {k: str(v) for k, v in (kwargs.get("params") or {}).items()}
        key = selector(method, url, params, kwargs.get("json"))
        options = self.by_key.get(key)
        if not options:
            raise RuntimeError("no recorded response for " + key)
        n = self.served.get(key, 0)
        self.served[key] = n + 1
        ex = options[min(n, len(options) - 1)]
        return httpx.Response(ex["status"], content=ex["body"].encode(), request=httpx.Request(method, url))

    async def aclose(self):
        return None


async def run(path):
    from tank.market import normalize as norm
    from tank.market import service as service_module
    from tank.market.service import MarketService
    from tank.market_storage import encode, semantic_fingerprint
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.util import jsonable

    payload = json.loads(gzip.decompress(path.read_bytes()))
    meta = {"instruments": payload["instruments"], "recorded_at": payload["recorded_at"]}
    instruments = instruments_of(meta)
    clock = Clock(payload["recorded_at"])
    out = {"trades": [], "rows": [], "bars": [], "fingerprints": {}, "watermarks": {}, "errors": []}
    with (
        tempfile.TemporaryDirectory() as directory,
        mock.patch("tank.util.utcnow", clock.utcnow),
        mock.patch.object(service_module, "utcnow", clock.utcnow),
        mock.patch.dict(norm.QUOTE_USD_HISTORY, clear=True),
    ):
        settings = Settings(
            _env_file=None,
            config_dir=setup_config(directory, instruments),
            data_dir=Path(directory) / "data",
            replay=False,
            config_version=CONFIG_VERSION,
            code_version=CODE_VERSION,
        )
        service = MarketService(MemoryRepository(), settings, http=Replay(payload["exchanges"]))
        service._market_restored = True
        service.instruments = {i["instrument_id"]: i for i in instruments}
        original_put = service._put

        async def put(collection, key, row, *, immutable=False):
            if collection == "mkt.capture_gaps":
                # A gap's id is random; its kind and reason are the comparable part.
                out["errors"].append([row["instrument_id"], row["kind"], row["reason"]])
                return row
            before = service._pending_market.get(collection, {}).get(str(key))
            result = await original_put(collection, key, row, immutable=immutable)
            after = service._pending_market.get(collection, {}).get(str(key))
            if after is not None and after is not before:
                out["rows"].append(
                    {"collection": collection, "key": str(key), "row": jsonable(copy.deepcopy(after))}
                )
            return result

        service._put = put
        for iid, inst in service.instruments.items():
            await service.poll_once(iid, derivatives=True, klines=True, ticker=True)
            try:
                page = await service.adapter(inst).trades()
            except norm.SemanticError:
                continue  # Hyperliquid lists no REST trades
            rows = page[0] if isinstance(page, tuple) else page
            out["trades"] += [norm.trade(inst, raw, source="REST") for raw in rows]
        for item in out["rows"]:
            out["fingerprints"][item["collection"] + "|" + item["key"]] = semantic_fingerprint(
                encode(item["collection"], item["row"]), item["collection"]
            )
    return out


def expected_path(fixture):
    return EXPECTED / ("rest-" + fixture.name.removesuffix(".json.gz") + ".json.gz")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--go")
    parser.add_argument("--dump")
    args = parser.parse_args()
    failures = 0
    for fixture in sorted(REST.glob("*.json.gz")):
        if args.dump and not fixture.name.startswith(args.dump):
            continue
        out = asyncio.run(run(fixture))
        if args.dump:
            print(json.dumps(out, indent=1, sort_keys=True))
            continue
        summary = digests(out)
        failed = False
        if args.write:
            EXPECTED.mkdir(parents=True, exist_ok=True)
            text = json.dumps(summary, sort_keys=True, indent=1) + "\n"
            expected_path(fixture).write_bytes(gzip.compress(text.encode(), mtime=0))
        if args.check:
            committed = json.loads(gzip.decompress(expected_path(fixture).read_bytes()))
            if committed != json.loads(json.dumps(summary)):
                failed = True
                print("PYTHON DRIFT", fixture.name, file=sys.stderr)
        if args.go:
            go = json.loads(
                subprocess.run([args.go, "rest", str(fixture)], check=True, capture_output=True).stdout
            )
            mismatch = compare(out, go)
            if mismatch:
                failed = True
                print("GO MISMATCH", fixture.name, mismatch, file=sys.stderr)
        failures += failed
        print(fixture.name, {k: len(v) for k, v in out.items()}, "FAILED" if failed else "ok")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
