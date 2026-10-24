# CI boundaries

The ci.yml orchestrator calls reusable test-suite.yml with Python units, three Python integration shards and their full-collection gate, and dependency-free Node local units/contracts. Python uses 3.13.13 and the frozen uv lock. Workers use xdist's normal auto CPU selection; the setup script calls the same pinned xdist hook to provision distinct databases. The setup script migrates one fresh UUID-named schema seed, validates its exact migration versions and schema fingerprint, closes its pool, and disables connections. Tests receive its name via --postgres-schema-template and retain fresh isolated clones; they never drop the external seed.

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
