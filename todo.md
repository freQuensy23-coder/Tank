# TODO

- Calibrate the FILTER and ROUTE cutoffs (03 §3.1, 01 V0-B) on labeled real news collected by the running system; until then both run at the uncalibrated midpoint 0.5 (user decision, 2026-10-20).
- While under test, every job timeout runs at 15x the 03 §3.8 values (runtime.TIMEOUT_SCALE = 15, user decision 2026-10-20). Set it back to 1 when testing ends.
- OKX `latest-events` (S10) is off: OKX rejects this annType from the EU production IP (code 51000); find a way to read it or drop it from the HLD sources table.
