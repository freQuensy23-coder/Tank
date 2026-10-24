# Executable inventory boundaries

This ledger distinguishes code execution from permission to rewrite reference evidence. No directory called testdata, goldens, or fixtures is treated as proof that programs inside it are inactive.

## Current reached programs

| Exact program | Reachability and classification |
| --- | --- |
| scripts/capture/parity.py | Active CI and integration verifier: .github/workflows/capture.yml and tests/test_capture_parity.py run it against actual Go output. It remains in the Python inventory. |
| scripts/capture/gen_schema.py | Active current generator/checker: capture CI invokes --check and the schema-copy test calls render. It remains in the inventory. |
| capture/internal/budget/testdata/gen_vectors.py | Pinned historical request-budget oracle recipe, unchanged Git blob b6f81d1e94f1e2c2f46b45d330776cbf76616317 at creation/reference commit ef68a822ceafeca3e37b405535a9cb310e4fa35a; SHA256 5aa37e93275faad4f8c256002e1efce27e772aa07fa7a871d94c5c2c6de1dc0e. Current Go budget tests consume only testdata/vectors.json. Current helper imports are unavailable. AST observations are reported separately and exact source bytes are preserved. |
| capture/internal/rest/testdata/gen_vectors.py | Pinned historical VenueAdapter/MarketService._request oracle recipe, unchanged Git blob ff9b6c0962d9cf65e10cdc9447ed49621e529a5a at the same reference commit; SHA256 ee12c5940f249c5f39b0124015119b38868bd84c805dd6986c6f1c9e1ce92907. Current Go REST tests consume only testdata/vectors.json. The retired Python adapter must not be restored to make this recipe run at current HEAD. AST observations remain separate. |
| scripts/capture/gen_pyvectors.py | Supported current development generator advertised in its module usage; it uses current CPython/Tank primitives. It remains in the inventory. Frozen vector payloads were not regenerated. |
| scripts/capture/gen_goldens.py | Retained historical generator program. Its explicit documented runtime is immutable commit a510e03, before the retired Python capture was removed. It is inventoried as program text; it must not become a restored current exchange producer. Golden payloads remain unchanged. |
| tests/fixtures/supplementary-sources/stage.py | Active first-party CI staging program. test-suite.yml invokes it directly. It is included despite living under fixtures. |
| tests/fixtures/openma/runtime-codec.ts | Executed immutable reference excerpts from OpenMA85d5361, source SHA256 e2e01a8205d137a286c67e2d02f6440b0a3b8e7da7250354f92baea869e9adca. tests/test_agents_openma_codec.py:run_codec reads and executes it. The TypeScript inventory parses it and reports its findings separately under reference_programs; it is exempt from migration, not from observation. |
| tests/fixtures/openma/opaque-input-fixes.json | Required literal integration patch data applied by the same codec test. Its literal program replacements are retained verification input, not ordinary application source to rewrite. |
| scripts/test-node.mjs and migrated test .mjs entrypoints | Active launch paths. Their typed implementation is imported from .mts; wrappers contain no function declarations. Both extensions are inventoried, so the implementation cannot disappear behind a shim. |
| scripts/ci/refactor_inventory.mjs | Active function-free entrypoint into the strictly checked TypeScript inventory implementation. |
| deploy/openma_lifecycle/native_network_probe.py | Active constrained guest worker, now read verbatim by native-network-qualification.mjs and supplied to the qualified guest. It is counted directly by the Python inventory. See the narrow exception below. |

The top-level capture/testdata and scripts/capture/goldens directories contain no executable Python, Go, JS, MJS, TS, MTS, or shell files at this checkpoint. That statement does not cover nested testdata directories: the two capture/internal/.../testdata generator programs are explicitly classified above. Their compressed captures, primitive vectors, reference outputs and digests are immutable payloads. The inventory nevertheless scans future program files under these names; regression tests verify that an executable generator placed there is included.

The TypeScript inventory also records Python command expressions with dynamic arguments. Literal String.raw Python bodies are extracted with original path/line/symbol metadata and can be passed to the Python AST inventory with --javascript-report. These are candidates for inspection, not a keyword-based exemption. The network PROBE body was moved into a normal first-party Python file; remaining one-line nonce/observer/server commands stay explicit command-expression findings.

## Constrained native diagnostic worker exception

The exception is restricted to deploy/openma_lifecycle/native_network_probe.py. deploy/sandbox.Dockerfile declares FROM python:3.12-bookworm and its installed diagnostic/browser dependencies do not declare or qualify Pydantic. The parent explicitly approved preserving that guest environment rather than changing the runtime image solely for this worker.

The worker uses fully typed standard-library frozen records, explicit type/range checks, and validated exact JSON serialization. Internal measurements are named records; dictionaries occur at the final external JSON boundary. It emits the existing TANKPROBE prefix and field aliases. Failed operations remain explicit failure records. No timestamp, budget, numeric result, missing resolver or TLS version is guessed.

Seven offline worker regressions exercise actual owned-file effects, malformed DNS response bytes, nameserver parsing and rejection of invalid runtime values. Strict mypy and Ruff pass. Python3.12 grammar parsing passes; a real guest/native/network run was not performed. This exception does not apply to application/domain code or other deployment helpers, and does not qualify a changed runtime image.
