# CI boundaries

The ci.yml orchestrator calls reusable test-suite.yml with two measured whole-file Python unit lanes and their exact-union/timing gate, three Python integration shards and their full-collection gate, and dependency-free Node local units/contracts. Python uses 3.13.13 and the frozen uv lock. Workers use xdist's normal auto CPU selection; the setup script calls the same pinned xdist hook to provision distinct databases. The setup script migrates one fresh UUID-named schema seed, validates its exact migration versions and schema fingerprint, closes its pool, and disables connections. Tests receive its name via --postgres-schema-template and retain fresh isolated clones; they never drop the external seed.

Integration starts its own PostgreSQL 16 container with tmpfs data and the verified lock/connection settings. It provides the intended read-only tank_agent role, Node 24, and exact retained captures. Cleanup collects logs and removes only the container created by that job. The integration gate records the selected collection before execution and requires the exact same case set to pass with zero skips, failures or errors. There is no allowed-skip list. Exact supplementary source captures and the locked actual pi-ai gateway package are prepared before collection.

The 71 prepared OpenMA Vitest cases and native/VM qualification are outside these local Node jobs. This workflow does not claim they ran.

stage_retained_fixtures.py verifies the full archive and each payload SHA before writing files. It rejects links, traversal, duplicate members and overwriting different inputs. It performs no network requests and does not recalculate results.json.

The upstream capture workflow remains authoritative for Go vet/race and recorded parity. The general integration job installs the module-declared Go toolchain, downloads pinned modules, and persists explicit GOMODCACHE/GOCACHE/GOPATH values before pytest changes HOME. Runtime Go child commands use the prepared caches with module/network and toolchain downloads disabled. The generated-schema comparison remains a unit; actual Go parity is an integration/process case. PostgreSQL migrations 046/047 require plain PostgreSQL 16;

The same orchestrator calls capture.yml and requires all source policy, Ruff, mypy, Go lint and test lanes before qualifying the five commit-tagged images. deploy.yml is manual only; see docs/TESTING.md for its production checkout and SHA guards.

Python integration and the Go race job each own a separate ClickHouse 25.8 service. prepare_clickhouse.py accepts only a loopback test URL, verifies the server release line and initializes all eight required market/trade tables. Full Python integration uses xdist loadfile to keep database-resetting ClickHouse cases together; check_go_results.py rejects every skipped/failed or incomplete Go test report.

MR wall-clock timing and integration shard completeness are described in [CI_BUDGET.md](../../docs/CI_BUDGET.md).

The Node local job now provisions Node24.22.0 plus uv0.11.7/Python3.13.13 and the frozen
project environment, installs the locked scripts/typescript tooling, and sets
TANK_FILE_IO_TEST_PYTHON to that environment's interpreter before running either lane.
For a fresh development checkout, follow the complete setup in docs/TESTING.md; a bare
node scripts/test-node.mjs command does not provision its prerequisites.


## Unit lanes

`unit_shards.json` records the actual complete local 59b963cf unit calibration: 4,020 cases in 239 files. Longest-processing-time whole-file placement balances summed reported case durations at 332.948/332.949 seconds, with 1,744/2,276 cases. These are calibration measurements, not predicted GitHub wall times. `-n auto`, `--dist loadfile`, the 10-second per-case bound, p90 below 1 second, zero skips and the 240-second total feedback budget remain unchanged. New files receive stable SHA-256 whole-file placement without an invented duration; declarations explicitly list unmeasured files and changed case counts. Recalibrate after observing new timing data rather than treating those files as zero-time measurements.

Each lane first uses the actual pytest console command with `-p scripts.ci.collect_units --collect-only -m 'not integration'`, then `-p scripts.ci.unit_shard --ci-unit-shard INDEX/2 -n auto --dist loadfile`. Both canonical plugins are loaded through the repository's declared pytest `pythonpath`; no cwd/PYTHONPATH shortcut is added. Workers must match the entire recorded collection before partitioning. The controller writes a declaration binding source SHA, plan digest and complete collection signature. Per-lane checking and the aggregate use `python -m scripts.ci.check_unit_results --bundle <result directories> --plan scripts/ci/unit_shards.json --source-sha <actual source SHA>`; per-lane calls additionally provide `--shard INDEX/2`. Every report must equal its declared partition. Both lane identities must appear once, their complete collections must agree, and their unique union and all durations are checked again. The budget monitor requires both lane jobs and the aggregate.

Failure node IDs/phases are flushed to the job log and `unit-failures.jsonl` immediately, before final JUnit exists. The pinned xdist crash marker `???` is retained as a separate record without falsely reporting its default zero as a measured duration. Test phases retain their actual reported durations. The raw mutable pytest report is an explicit foreign-framework boundary; no casts, ignores or cross-open filesystem caches are used.

To regenerate from a complete successful run, use the pinned environment and `uv run --frozen --extra dev python -m scripts.ci.build_unit_plan --report <one or both JUnit files> --collection <full collection JSON> --source-head <recorded commit> --output scripts/ci/unit_shards.json`. The generator accepts the retained 59b list format and the new source-bound collection format, verifies all case identities and timing limits, and records the input SHA-256 hashes. The historical list's source commit is an explicit required argument; no timestamp or filename is used to guess it. Input reports and retained evidence are never rewritten.
