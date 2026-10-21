# BoxLite native runtime

This archive contains the exact 18-file Linux amd64 runtime packaged in the existing Tank OpenMA image, including the Tank replay-DNS shim, original provenance, checksums and build evidence. File bytes and executable modes are preserved; tar owner/time fields are normalized.

Extract the archive directly into `/opt/tank-boxlite-runtime`, then check its internal `SHA256SUMS`. The adjacent `SHA256SUMS` validates the compressed archive itself. `SOURCE.json` records each original file and the upstream source/license locations.

The upstream source is BoxLite 0.10.6 at commit 7873ae4995e171237a3342adbb84000c8ab9965b. The custom replay-DNS source, reviewed patch, build contracts and instructions are retained in `deploy/boxlite/v0.10.6/replay-dns` at the repository root. Upstream submodule revisions and original toolchain/build receipts are included in the archive. The original qualification metadata is unchanged; this packaging operation does not add native hardware qualification.

The license texts in `licenses/` are copied verbatim from the pinned BoxLite source and its native dependency sources. Original per-file copyright/license notices and the complete upstream source remain available at those pinned source locations.
