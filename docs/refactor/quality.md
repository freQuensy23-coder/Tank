# Quality workstream

This is an incremental refactor report, not a declaration that the global refactor is complete. Production and deployment were not modified. All work used the authorized isolated server worktree.

## Pinned execution environment

Shared Python: `/srv/tank-refactor-env/bin/python` (3.13.13). Shared uv: `/srv/tank-refactor-tools/bin/uv` (0.11.7). Dependencies were installed from the unchanged frozen `uv.lock` using `uv sync --frozen --extra dev --no-install-project`. No editable installation binds the environment to this worktree. Set `PYTHONPATH="$PWD/src"` in the worktree being checked.

Shared Go: `/srv/tank-refactor-tools/go/bin/go` (1.25.14), official archive SHA256 `a21ae5633a269bcd7e90cf767e48225633795e99d831742cbf3397064fee7712`. Shared Node: `/srv/tank-refactor-tools/node-v24.22.0-linux-x64/bin/node` (24.22.0), official archive SHA256 `cc2f601cee82d9a5a233302fe307abc600727035fd7d2c3a3571755989e95008`. Prepend both bin directories to PATH; npm is in the same Node bin directory. Node gateway dependencies use the checked-in lock with `npm ci --prefix tests/node-gateway --ignore-scripts --no-audit --no-fund`.

Heavy jobs serialize through `flock /srv/tank-refactor-tools/heavy.lock`. No CPU affinity, worker limit, resource quota, production virtualenv change, process/container cleanup, or deployment was introduced.

## Implemented changes

- Commit `2c5bea6` added 1,584 proven void return annotations and 651 standard fixture parameter annotations across 138 existing test files. The initial progress counts included subsequently transferred files; these are the exact committed counts. Removing annotations and import nodes from before/after ASTs produces identical trees for every file, recorded in `annotation-verification.json`. The void-return selection excludes value returns, yield, and yield-from; nested functions/classes are analyzed as separate scopes.
- The fixture-name transform incorrectly classified HTTP callbacks named `request` as pytest fixtures. This was identified, reported, and assigned to the test owners for correction to `httpx.Request`. The complete suspect-site list is retained at `/srv/tank-refactor-evidence/fixture-shadowing.json`. Quality corrected its retained E2E callbacks. Other non-injected `tmp_path` and `monkeypatch` helpers carry actual propagated Path/MonkeyPatch objects. The broad annotation commit is not evidence of complete or correct test typing by itself.
- Capture service and health status use exact Go records. A behavior test checks current connected counts, hot-token sorting, independent returned slices, optional flush errors, and the existing serialized JSON fields.
- Capture HTTP requests now decode into a typed request. Missing method or source_id, non-string header values, non-string source IDs, and unknown request fields fail before invoking the fetcher. There is no implicit GET method or string coercion. Source ID is required and may explicitly be null, matching the Python contract.
- Capture fetch body transport uses `json.RawMessage`; absent bodies do not become a JSON-null GET payload. Success uses a typed result with status/content type, exact byte count, digest, collection timestamp, required nullable source ID, and exactly one text/base64 representation. Empty text bodies remain present. The Go allowlist now includes the existing Python-approved `openapi.okx.com`.
- CI watchdog Event/Run/Job/Page/Outcome records are Pydantic contracts. Missing successful completion evidence and invalid page counts fail explicitly. An empty nonterminal GitHub page fails rather than spinning inside pagination. The 240-second deadline and all required lane/image checks remain intact.
- The watchdog workflow now installs pinned Python/uv and frozen dependencies before invoking the typed script. Origin time still includes setup/queue delay. CI additionally runs strict mypy over CI helpers and the watchdog tests.
- CI integration hooks have exact pytest parameter types. Integration collection manifests and retained archive manifests validate against Pydantic models. PostgreSQL setup passes the original connection string explicitly instead of heterogeneous kwargs.
- The Go results gate uses typed test2json projections and execution counters. The old set comparison could accept two starts and one pass for the same case; the new gate rejects that incomplete execution history.
- Shared score fixtures construct all seven strict dimensions with unavailable values and explicit NOT_ASSESSED reasons. Their JSON conversion is only at the test-document boundary. No assessed business score is invented.

## Test audit and retained evidence

No behavior test was deleted. The CI shard test compared a list with `reversed(reversed(paths))`, which reconstructed the same discovery order. It now checks genuinely reversed discovery order and stability when another file is added. Existing shard coverage/disjointness assertions remain.

New regressions exercise real policy or effects: malformed GitHub pages and job evidence; incomplete repeated Go executions; actual retained archive staging, repeatability, refusal to overwrite differing bytes, and refusal to follow a destination symlink; actual Go fetch invocation suppression and storage encoder output; and live status snapshots. Raw JSON dictionaries in these tests intentionally exercise malformed or external protocol documents.

The assertion inventory detects literal assertions and direct self-comparisons without function calls. Neither occurs in the original baseline. This narrow scan cannot establish whether all other assertions prove useful behavior; semantic review remains with the file owners. It deliberately does not delete tests based on syntax heuristics.

## Boundary exceptions

| File/symbol | Retained dynamic form | Exact reason |
| --- | --- | --- |
| scripts/ci/ci_budget.py:Api / JsonPayload | JsonValue transport payload | GitHub's external response is parsed immediately into Event, Run, or JobPage. No untyped internal job dictionary remains. |
| scripts/ci/ci_budget.py:GitHubRecord | extra=ignore | The external GitHub API includes unrelated extensible metadata; the gate strictly validates the fields it uses. |
| scripts/ci/ci_budget.py:Event / Job | nullable action/pull_request/conclusion/completed_at | Workflow events can omit PR metadata; pending jobs have no terminal conclusion/time. Completed success requires completion evidence explicitly. |
| scripts/ci/check_go_results.py:TestEvent.test | nullable test identity | Package-level output/pass/skip events have no Test field. Run events require identity; test-level skips fail. |
| scripts/ci/check_integration_results.py:verify | dict[str,int] summary | The homogeneous three-key result is an output summary consumed by existing gate clients. Collection cases themselves are strict records. |
| scripts/ci/stage_retained_fixtures.py:stage | keyed entry/staged-byte maps and output summary | Files are indexed by external path; byte payloads and frozen manifest evidence remain unchanged. Entries are strict records. |
| scripts/ci/refactor_inventory.py | counters/import adjacency/symbol dictionaries | Keys are discovered file/module/symbol identities, not records with known field names. |
| capture/internal/capture/fetch.go:decodeFetchRequest | any input | The existing storage queue exposes raw JSON; strict decode ends that boundary immediately. |
| capture/internal/capture/fetch.go:FetchRequest.Body / rest.Client.Fetch | json.RawMessage | Arbitrary external exchange JSON payload is preserved; it is not an internal domain record. |
| capture/internal/capture/fetch.go:FetchResult.storageValue / fetchFailure | map[string]any | The existing canonical Python-compatible storage encoder accepts JSON maps, not Go structs. Typed internal records convert exactly once before that storage boundary; tests invoke the real encoder. |
| capture/internal/rest/client.go:Client.Fetch payload | map[string]any | Only an external JSON object is decoded for the existing exchange rate-budget classifier. The transport retains the original raw JSON bytes. |
| tests/score_fixtures.py:unassessed_score_dimensions | dict[str,JsonValue] | Serialization boundary for existing raw storage/LLM test documents; construction first validates strict ScoreDimensions. |
| tests/test_ci_go_results.py, test_ci_budget.py, test_refactor_inventory.py | malformed/external JSON maps and dynamically loaded tool modules | Tests must exercise invalid wire documents and standalone script entry modules. This is not an excuse for untyped production records. |

## Inventory and remaining work

`baseline-inventory.json` was produced from an untouched git archive of `1216b4b`. It includes every active Python/Go file selected by the source roots, per-area/module/symbol candidate counts, source import edges, and strongly connected import components. Third-party/upstream/vendor/history, retained fixture payload trees, and caches are excluded. Active Python fixture-staging code remains included.

| Baseline area | Untyped parameters | Untyped returns | Dictionary candidates | get/or candidates | Dynamic Go lines |
| --- | ---: | ---: | ---: | ---: | ---: |
| src | 2457 | 1130 | 2387 | 6214 | 0 |
| tests | 2957 | 2430 | 6785 | 1306 | 0 |
| scripts | 190 | 110 | 190 | 333 | 0 |
| deploy | 106 | 70 | 132 | 213 | 0 |
| capture | 0 | 0 | 0 | 0 | 1011 |

Baseline import SCCs: agents/sessions; clickhouse_market/market_storage/storage; market/service; sources/polling/probe. These include package reexport edges and require architectural interpretation, not automatic deletion. Foundation is addressing the genuine storage backedge. Regenerate the report from the integrated tree to measure all sibling work:

```sh
/srv/tank-refactor-env/bin/python scripts/ci/refactor_inventory.py --output docs/refactor/integrated-inventory.json
```

Add `--details` for exact source expressions/lines. The current quality-only branch report intentionally still contains untouched sibling production source and domain tests. Thousands of candidates remain: this work does not complete precise typing of all tests, acceptance/deployment scripts, or the remaining Go exchange adapters. No broad lint exceptions, arbitrary object annotations, Any placeholders, or source-only completion claims were added.

## Verification and limits

- Ruff check and formatting passed across tests/scripts/deploy in the quality worktree. The complete active comment policy passed.
- Strict mypy passed all ten CI helper files and the typed watchdog/Go-gate tests, using `--strict --explicit-package-bases`. This does not imply that all repository code passes strict mypy.
- The current focused CI/inventory tests pass; actual retained staging takes approximately 0.35 seconds in the focused run.
- Dedicated Go capture+REST run: 134 actual cases passed with zero skips, verified by the results checker from `/srv/tank-refactor-evidence/quality-fetch-final.jsonl`; exit file is zero. This is not PostgreSQL/ClickHouse integration qualification.
- The earlier `go test ./...` package exit was zero, but was not a zero-skip service integration proof and is not used as one.
- Root owns the actual disposable PostgreSQL/ClickHouse integration run, full collection/timing evidence, cross-domain validation, GitHub CI, and Codex review. Those remain required before declaring the global refactor complete.
