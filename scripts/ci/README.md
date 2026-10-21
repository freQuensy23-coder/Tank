# CI boundaries

The workflow has three independent jobs: Python units, Python integration, and dependency-free Node local units/contracts. Python uses 3.12 and the frozen uv lock. Workers use xdist's normal auto CPU selection; the setup script calls the same pinned xdist hook to provision distinct databases.

Integration starts its own PostgreSQL 16 container with tmpfs data and the verified lock/connection settings. It provides the intended read-only tank_agent role, Chromium, Node 24, and exact retained captures. Cleanup collects logs and removes only the container created by that job. Existing prerequisite skips are listed with exact case and reason in integration-prerequisites.json. They remain unverified, not passes; failures and additional skips fail CI. Previously missing tank_agent is deliberately not an allowed skip.

The 71 prepared OpenMA Vitest cases and native/VM qualification are outside these local Node jobs. This workflow does not claim they ran.

stage_retained_fixtures.py verifies the full archive and each payload SHA before writing files. It rejects links, traversal, duplicate members and overwriting different inputs. It performs no network requests and does not recalculate results.json.
