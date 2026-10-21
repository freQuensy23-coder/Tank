# CI boundaries

The workflow has three independent jobs: Python units, Python integration, and dependency-free Node local units/contracts. Python uses 3.12 and the frozen uv lock. Workers use xdist's normal auto CPU selection; the setup script calls the same pinned xdist hook to provision distinct databases. The setup script migrates one fresh UUID-named schema seed, validates its exact migration versions and schema fingerprint, closes its pool, and disables connections. Tests receive its name via --postgres-schema-template and retain fresh isolated clones; they never drop the external seed.

Integration starts its own PostgreSQL 16 container with tmpfs data and the verified lock/connection settings. It provides the intended read-only tank_agent role, Chromium, Node 24, and exact retained captures. Cleanup collects logs and removes only the container created by that job. The integration gate records the selected collection before execution and requires the exact same case set to pass with zero skips, failures or errors. There is no allowed-skip list. Exact supplementary source captures and the locked actual pi-ai gateway package are prepared before collection.

The 71 prepared OpenMA Vitest cases and native/VM qualification are outside these local Node jobs. This workflow does not claim they ran.

stage_retained_fixtures.py verifies the full archive and each payload SHA before writing files. It rejects links, traversal, duplicate members and overwriting different inputs. It performs no network requests and does not recalculate results.json.
