# Retained supplementary source captures

These are the original 2026-10-19 public captures used by the existing Anchorage,
DTCC and Uniswap authored-body regressions. The unchanged raw index contains
1,085 records and retains SHA256
`9ca7e6b7a606b587eada5b2ad8faa9c861a114afa78ee0e4671d912bb59cffca`.

The archive contains that exact index plus 172 original compressed page files:
S28 indices 181 through 192 (12), every S08 record (142), and every S15 record
(18). The records and payloads are preserved byte for byte. No page was fetched
again, and no expected result, assertion, source timestamp or metadata was
regenerated. The S15 set deliberately includes the original malformed page.

Run from the repository:

```sh
python tests/fixtures/supplementary-sources/stage.py
```

The stdlib-only installer verifies archive and member hashes, safe regular paths,
completeness and destination links before staging to
`data/calibration/supplementary-sweep-20261019-run1/`. It accepts identical
existing files and refuses different bytes. Integration CI runs it before the
18 historical cases; no API credentials, environment variables or network are
required. Missing required captures fail those tests.

`manifest.json` records every exact file hash and size, each selected original
record index, public URL, decoded payload hash, archive construction and the
bounded credential-shape scan. The scan included the whole index and all 172
decoded pages, found no matches, and included no authorization/session files.

The archive is deterministic: sorted USTAR regular files, fixed zero ownership
and timestamps, no executable modes, and gzip with an empty name and zero
timestamp. Two builds were compared byte for byte.
