# Replay evidence

## Mandatory acceptance

Run `uv run pytest -q tests/test_fixture_acceptance.py` for V0-C/V0-D. This
checks all 27 canonical input packets and separately requires completed,
integrity-bound output evaluated against pre-run sealed expectations. The older
`test_manifest_honest` checks inventory honesty only; its success does not mean a
fixture is executable or passed. Missing packets, empty market declarations,
unmasked names, absent external expectations and undeclared shifted minute cutoffs fail
acceptance. The new gate currently fails on the incomplete retained corpus;
that failure must not be converted to a skip or an expected failure.

A ready manifest entry names `acceptance_input`, `private_name_map` and
`sealed_outcomes` as `{path, sha256}` references. The map and expectations live
outside the pipeline input directory. Expectations bind the exact whole input
envelope hash and the unchanged `assertion_spec`, and contain `sealed_at` plus
executable predicates for the independent acceptance evaluator in `fixture_acceptance.py`. Predicates support literal equality, allowed/excluded sets, numeric bounds, collection length, `all`/`any`, and conditional checks. A `*` path checks every element and requires a nonempty collection; `satisfies` applies a nested predicate to each selected value; `some` requires at least one matching array element. Missing required fields fail. A completed entry
also names an `acceptance_run` receipt with `started_at` and a hash-bound
`frozen_output`. The evaluator verifies the output checksum, input generation,
sealing chronology and actual assertions; a `passed: true` label alone is not
evidence. Every sealed `manual_predicates` clause also requires a hash-bound
`manual_review` on that run. The review binds the input and frozen output,
preserves each requirement verbatim, and records a decision, reasoning and
retained evidence for each clause. Missing or failed manual checks fail acceptance.
Date-only cases retain their day precision; F24's later confirmation
does not replace its original first-phase cutoff. Whole-minute references must retain the original literal, `BUCKET(1m)` precision, and the explicitly declared exclusive boundary; they do not establish an exact original call instant.

`tests/test_fixture_acceptance_controls.py` tests this gate using explicitly
synthetic small inputs. Passing those controls is not a historical-fixture pass.

`manifest.json` transcribes all 27 pre-run assertion specifications from Technical Design §11.3. Twenty cases now reference prepared masked inputs, private maps and pre-run sealed outcomes. Preparation does not mean acceptance passed; actual run receipts and unmet requirements are recorded separately. The remaining seven full cases are incomplete; F11–16 have separately retained formula components. Captured REST/archive files under `fixture_check/` are authentic partial market inputs. F-16 follows the updated authoritative fixture: STRESS, one Trade Grade downgrade for non-STRUCTURAL events, and no BULLISH direction for a non-STRUCTURAL beneficiary. F-14 uses the clarified price-bar timestamps. Superseded assertions appear only in `assertion_audit_history.json`; current manifests follow the current Technical Design. Synthetic cases are labeled explicitly.

Offline replay JSON inputs contain `cutoff`, `config_version`, `code_version`, `registry`, `config`, `records`, `tool_results`. Each record is `{collection,key,available_at,record}`. Captured analyst calls are ordered in `config.replay_tool_calls`, each `{call_id,tool,arguments,context,available_at}`; arguments follow deployed tool schemas. Captured external tool HTTP results carry `method,url,status_code,result,available_at`. `replay.market_snapshots` records contain the complete normalized market snapshot with exact `token_id` and `cutoff`. These are passed through production Market Truth and decision code. Snapshot history must contain only observations before the cutoff; null/incomplete inputs retain their reasons.

Replay requires an empty isolated repository. Reproduction requires a second empty repository and re-executes tools; it never compares an output to itself. The CLI must run in its own process because its simulated clock and deterministic ID bindings temporarily affect module symbols. HTTP has no network fallback. A complete captured packet is required; missing captured tools/snapshots fail explicitly.

Sealed outcomes belong outside input packets, outside the agent's accessible replay environment. `evaluate_frozen` reads them only after output artifacts have been written. Input packets containing outcome fields are rejected. Mask identifying names before historical model behavior runs and retain the reversible map outside pipeline inputs. Model recall tests cannot establish historical investment performance.

Integration tests use synthetic inputs and mocked HTTP/SSE/Telegram. The opt-in PostgreSQL integration test runs with `TANK_E2E_DATABASE_URL` pointing at a disposable PostgreSQL16 database. It is not proof of live V0-G latency or any sustained V0-A/V0-H acceptance gate.

`replay_agent_session` is the separate fresh-session path: it requires an explicitly supplied offline control-plane transport, a real EventToolService with captured market readers, and a registered `replay_environment_id`. It creates a new replay-run identity, sends the source message through SessionService, waits for a protocol end-turn/IDLE, reruns domain verdict tools, and freezes newly computed outputs. A regression deliberately seeds a wrong stored START; the fresh session recomputes Market Truth and returns WAIT. Unit tests use a captured model transcript. Actual fresh native sessions have also run through the retained Colima/OpenMA controller; see `docs/verification/f08-cli-r35b-2026-10-19/result.json` and current `masked-*-r36-*` results for their specific outcomes and limitations. Remote session provenance differs across runs; exact immutable reproduction uses `replay_bundle` on the stored tool transcript.

`prepare_masked_packet` writes a derived masked packet without overwriting original captures. The explicit one-to-one reversible name map is written outside the replay input directory and must not be mounted into the replay environment. The loader rejects injected private maps. Masked historical LLM runs remain behavior checks, never investment-performance evidence.

Late market archive receipts require an explicit top-level `assembled_at` at or after every actual collection/receipt timestamp. These timestamps remain unchanged. Market observations and full bar bucket ends must be no later than the replay cutoff (normalized bars require `interval_s`; canonical `mkt.klines_1m` has a fixed 60-second bucket). Such outputs carry `evidence_mode=HISTORICAL_RECONSTRUCTION`, never latency evidence. Source, web, and agent evidence has no late-receipt exception. Missing assembly bounds, post-assembly receipts, future market points and partially future bars are rejected.
