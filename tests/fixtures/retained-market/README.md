# Retained market inputs

195 captured input files and the original results.json, preserved byte-for-byte from fixture_check in the isolated server audit. The deterministic archive has no links or executable payloads. manifest.json records every path, size and SHA256. CI stages these inputs using scripts/ci/stage_retained_fixtures.py before historical integration regressions; expected values are never regenerated.
