# Independent acceptance of the 20 October requirement update

Status: **r82 image and native protocol verified; actual r82 E2E running; no final acceptance claim**. The latest direct user request
is the authority when it changes an older PRD/HLD/TDD behavior. All 27 requested
changes are listed in `acceptance-plan.json`, with the original request digest.

The comparison baseline is the deployed r78 Tank image
`sha256:81e2d555ae3f8882551c97504a3b1c28c234b95a6bdce6f0309fa49ab6602819`,
not the repository HEAD. The working tree contains earlier implementation work.
`baseline-r78.json` verifies all 347 source, deployment and dependency files
against that image's existing manifest. Readable originals are in
`data/qa/requirements-integration-20261020/baseline-r78/`. The extraction used an
owned stopped container with no network or mounted volumes; it was removed.

## Current verified candidate

r82 is image `sha256:aeff9822a0a29414e5563f05c3f0e1b05fb222e30331cb0ff71f99c654a81402`,
code `43977ae5a00fed630eb80aa1a482312f9dcb6a544cb0c87be0837d9bb4af92b8`.
Its sole product change from r81 releases database ownership during agent
web_search/fetch_page external waits. Exact image/dependency verification,
actual-image canonical PostgreSQL blocking/error/retry proof, and60 independent
native protocol tests passed. See `candidate-r82-runtime-tuple.json` and
`current-acceptance-boundaries-r82.json`. No final live outcome is inferred.

Actual r81 E2E ended incomplete after observer PoolTimeout with18 events,
16 assessments, one market truth and zero decisions/cards. Its complete copied
original evidence was independently verified:61 table exports,4,737 raw/bundle
files,25 native sessions/1,525 events/204 executions;24 initial native canonical
source snapshots remain equal to final source rows. The single actual Ops
heartbeat message13 is separate from Alerts. Source→record completion was
76.605–585.080seconds; only3/18 events met90seconds in the cold25-item burst.
`r81-actual-independent-review.json` records these limits.

Primary r81 was actually deployed by the coordinator. Independent complete
archive readback verified2,212,864 SQL rows and19,500 raw-cache files/hardlinks,
private modes,30-day expiry, obsolete schema/path removal and unchanged frozen
history chains. Original paired historical ETH/BTC jobs then completed through
the normal primary queue on the first attempt in69.078/68.944seconds. Original
FAILED records remained unchanged. Proofs: `primary-r81-independent-audit.json`
and `recovery-r81/report.json`. These are specific primary proofs, not full
system acceptance. All owned temporary QA PostgreSQL containers were removed.

r81 is image `sha256:94a6ac799d1814dd810471c36b2ba4e869c48928b36dc6216bca39e137112e24`,
code `91b9a765de200fc0212fc386909f0638cb1b32be32890b52935cea2c967ad308`.
`candidate-r81-runtime-tuple.json` pins both config versions and the retained
qualified OpenMA image. `combined-status-r81.json` records the independent runs.

The complete r79 run was 1965 passed, 17 failed, 37 skipped. Its original
failures remain recorded: seven formal fixture evidence gaps, five failures from
a real omitted-cost producer bug, four PostgreSQL test-isolation failures and
one stale test Settings fixture. The producer and test fixtures were corrected
by their owners; r80 affected coverage passed 301 tests without skips. These
counts overlap and must not be summed into an invented full-suite result.

Actual r80 source/native E2E then found an undeclared lending field at the typed
SQL boundary. r81 preserves those diagnostics under the existing named
`identity_evidence` JSONB field. Independent verification passed all six supply
row kinds through the actual r81 image, MarketService pending batch, flush and
real PostgreSQL readback. The r81 market/supply regression passed 95 unique
cases, with four environment-specific skips. Two initial probes overlapped
fresh migrations and exhausted the default64 cluster lock budget; both negative
logs are retained. Sequential rechecks passed without code or PostgreSQL
setting changes.

The image contains exactly the frozen source/deploy file set and 46 production
packages. The clean r79 dependency rebuild retired the old modules and entire
venv; r80/r81 reuse that verified closure and replace complete source/deploy.
The original full-project `uv sync --check` requested only local editable
rebuild after Docker export and remains recorded as failed freshness evidence.
Independent exact frozen package-graph comparison, dependency check, source and
mode hashes, editable import path, pip check and synthetic Chromium all passed.

Our disposable PostgreSQL containers were removed after zero active clients;
logs, failed attempts and receipts remain. The workflow owner has the verified
r81 tuple and the coordinator's conditional authorization for actual non-X E2E.
No positive r81 real-source-to-Telegram claim or primary deployment is made here.

## Candidate and evidence boundaries

The coordinator must first announce a coherent frozen candidate and pin its
source/configuration/deployment/dependency fingerprint. A successful unit test
from an earlier image does not qualify changed code. Source files must have the
same digest before and after each independent run. A moving candidate is reported
as changed, not accepted.

The QA runner is
`data/qa/requirements-integration-20261020/run.py`. `--inventory` executes no tests
and prints a source fingerprint. Test execution requires that exact
`--frozen-digest`. It selects existing tests under `tests/`, so the established
credential-free/offline pytest boundary remains active. Reports are written to
new timestamped folders and failures are retained. `--postgres` requires a
coordinator-provided disposable test database; it must never point to production.
`--browser` enables synthetic browser tests that use temporary profiles. No real
X profile, authentication state, messages or session are touched by this runner.

The runner records regression results and skips; it does **not** label them full
acceptance. PostgreSQL skips must be replaced by an isolated PostgreSQL run for
queue, restart, concurrency, migration and immutability claims. A synthetic
Telegram transport proves payload/ledger logic, not actual delivery. A successful
image build proves neither native x86 KVM execution nor 72-hour availability.

## Cross-module acceptance chains

1. **Delayed LIVE update (1, 2, 10).** Publish APPROVED at 10:00, accept a fresh
   LIVE update at 10:05, freeze its text/evidence/decision IDs and persist exactly
   one due job for 10:15. Reopen the database and reconstruct services. Confirm
   no send at 10:14:59; at 10:15 send in the original thread without another item
   or agent invocation. Exercise concurrent publish and due workers, Telegram
   429 followed by success, restart after transport acknowledgement, and a later
   EXIT. Accepted snapshots remain immutable and delivery does not silently
   re-evaluate semantic policy. Separately attach a three-day-old STATE_UPDATE:
   stage changes in the database, but no card or delayed job is created and no
   timer can revive the stale claim.
2. **Source item through optional analysis (3, 4, 19, 27).** Collect a canonical
   source item once; repeat its external ID with changed metadata/body and restart
   bootstrap. Assert the original captured text, source/publication metadata and
   raw file are unchanged and no duplicate analysis job appears. Inspect evolving
   observation/routing/filter state separately: the current design keeps some
   processing bookkeeping in the same source row, so whole-row byte equality is
   not claimed. Let a valid native agent end
   an ITEM_ADDED turn without add_observation: the event, existing calls and
   session remain usable and a later item joins them. Only an actual OpenMA
   session failure causes FAILED. An outside-universe-only event is LOW and
   creates no publication. An HTTP 200 discovery page with zero articles is
   DEGRADED in durable monitor state; conditional 304 and an already-seen valid
   listing are distinguished from a broken zero-result parser.
3. **Assessment to market/action/card (5–12, 14).** Preserve the same frozen
   structured inputs across Market Truth, decision and composer. Check missing
   score reasons, primary/credible-secondary/loser identity filtering, all eight
   negative layer explanations, unconfirmed absorption wording and alternatives.
   G4 evidence must address required infrastructure rather than an unrelated
   quote. B3 ADD must not inherit START's extra guards. Removing only an optional
   instrument or self-test registry record cannot reduce confidence when spot,
   derivative and freshness observations are unchanged. Lending requests only
   /lendBorrow and maps measured fields without symbol guessing.
4. **Scheduler and storage (18, 20, 21, 23, 27).** On a MAJOR+ assessment, measure
   three pre-event windows at creation and let the scheduler create exactly five
   post-event jobs per analyzed token and benchmark. Concurrent ticks, crash
   recovery, missing-data retry and late-data revision retain frozen results
   without duplicate jobs or overwrite. Test both an empty database and a legacy
   upgrade containing archive/raw evidence, including failure halfway through
   export. Data must be preserved before obsolete schema/path removal. Verify
   actual PostgreSQL columns after migration, not only schema strings. Every
   runtime path/secret/volume must be documented in HLD and every retained class
   must have a defined retention rule.
5. **One deployment/provider path (13, 15–18, 22, 24–26).** Inspect the actual
   prepared OpenMA source and applied patch inventory, full Compose configuration,
   dependency lock and both native BoxLite artifact manifests. Every applied
   stage is justified in HLD; no historical/rejected lifecycle stage is selected
   accidentally. S19 and S29 acquire one browser owner and survive independent
   page failure without closing the shared browser or changing source X session
   state. A mocked Jev call and, later, an authorized actual call must use only
   OpenRouter /api/v1/systemone. Calibration uses the same production prompt and
   model binding, 200 independently labeled items and the recorded selection
   objective, with no production config mutation and no fabricated 90/90 claim.

## Full system evidence after the frozen candidate is deployed

The coordinator owns deployment, authenticated model/native sessions and any
actual Telegram sends. The independent review will check the resulting original
timestamps, jobs, OpenMA tool events, frozen evidence, transport receipt and row
digests. The positive path must enter through an ordinary enabled source and
reach Telegram with no manual pipeline stage. A STALE source item that correctly
produces no card is valid negative E2E evidence, not positive V0-G acceptance.
The source→event 90-second target and later market analysis timeout are measured
separately. Repeat the originally failing concurrent BTC/ETH historical market
jobs on the final image without altering old FAILED records.

Current earlier evidence is retained with its scope: r78 concurrent BTC/ETH
completed in about 70 seconds; r74 normal-source negative E2E created an event in
45.21 seconds and correctly suppressed STALE publication; the last positive live
source→Telegram proof is r56, on earlier code. Neither the r56 success nor the
readability preview in Ops message 9 establishes positive E2E of the new candidate.

Formal historical acceptance remains separate: 16/27 cases have accepted evidence
at differing versions; F11–F16 have market components rather than full ready
replay packets. Full V0-C/V0-D, 72-hour source operation, 14-day shadow operation
and native x86 runtime must be reported honestly until their evidence exists.
