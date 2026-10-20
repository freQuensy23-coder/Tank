# Audit W10: DefiLlama /protocols and /overview/fees removed (2026-10-20)

- Change: commit 8f0224d on ws/tank-truth (src/tank/market/supply.py). Fundamentals use only the registered
  slugs (registry.yaml defillama_protocol_slug[s]); a token without one records
  NO_REGISTERED_DEFILLAMA_PROTOCOL_SLUG and makes no request. HLD §4.8 lists only /summary/fees/{protocol}.
- Component test: tests/test_market_service.py::test_fundamentals_use_registered_slugs_only_and_partial_holders_revenue
  (mock transport asserts every path is /summary/fees/...).
- Real run on openclaw-dp, 2026-10-20T17:1xZ, all 22 registry tokens, SupplyCapture.fundamentals, test DB tank_test_w10:
  real-run.jsonl (22 ok) and real-run-http.log: 55 requests, all https://api.llama.fi/summary/fees/{protocol};
  0 requests to /protocols or /overview/fees.
