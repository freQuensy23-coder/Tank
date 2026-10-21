# BoxLite native runtime

The archives contain the exact Linux amd64 and arm64 BoxLite runtimes, including the Tank replay-DNS shim, original provenance and checksums. The amd64 bundle contains 18 files, including its original build evidence. The arm64 bundle contains the original 10 files from the qualified project export; no build evidence is synthesized. File bytes and executable modes are preserved; tar owner/time fields are normalized.

The Dockerfile selects the archive by `TARGETARCH`, checks its compressed SHA256, extracts directly into `/opt/tank-boxlite-runtime`, and checks the internal `SHA256SUMS`. The adjacent `SHA256SUMS` lists both compressed archives. `SOURCE.json` records the amd64 inventory; `SOURCE.arm64.json` records the arm64 inventory.

The upstream source is BoxLite 0.10.6 at commit 7873ae4995e171237a3342adbb84000c8ab9965b. The custom replay-DNS source, reviewed patch, build contracts and instructions are retained in `deploy/boxlite/v0.10.6/replay-dns` at the repository root. Original qualification metadata remains unchanged.

`../tank-native-qualification.json` preserves the original per-architecture component qualification records from `deploy/boxlite/v0.10.6/native-qualification.json` and `deploy/boxlite/v0.10.6/replay-dns-qualified-artifact.json`. The arm64 record covers native network/database behavior. These historical component receipts do not certify the current OpenMA image or full session acceptance.

The license texts in `licenses/` are copied verbatim from the pinned BoxLite source and its native dependency sources. Original per-file copyright/license notices and the complete upstream source remain available at those pinned source locations.
