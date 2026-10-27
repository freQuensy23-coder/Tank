# Synthetic old-producer pending wire

`producer-d17.json` and `producer-d17-known.json` are unchanged outputs of actual `SourceService.poller.acquire` at commit `d17a24f80b98d0286861701f693a3b1b7dcb3e01`, executed locally on 2026-10-27 against an explicitly synthetic S01 RSS response. The first response omitted `pubDate`; the known-clock control reported its actual authored `pubDate`. The producer persisted its real generated item/batch identities, collection clock and missing-publication reasons before any detail request. This is an old-format producer regression witness, not a historical provider capture or production record.

The external original producer/reproducer scripts and outputs are retained in `source-legacy-pending-producer-v2.py/.log` and `source-legacy-detail-clock-repro.py/.log`. Tests load this record through the actual repository and current checkpoint decoder, without adding clock, body, ownership or publication facts to its original row.
