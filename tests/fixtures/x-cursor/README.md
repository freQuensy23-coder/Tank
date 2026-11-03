# Retained X cursor fixtures

`retained-accounts.json` contains twelve exact per-account projections of existing public-profile actor OUTPUT records collected read-only for the Tank Ops audit. Each entry retains the original run/build identity, account report, next cursor and that account's parsed public posts. The source was `x-apify-run-output-builds.jsonl`, obtained through existing-run Apify GETs; the immutable source build for these entries is `VVazk15ZfUceoqWms` (0.3.3). No actor was launched to create the fixtures.

The WuBlockchain, Hayden Adams and PancakeSwap sequences each show a missing previous maximum, a reported `historyGap: true`, and a later identical post set reported gap-free after the old implementation moved its cursor. These records prove loss of the original coverage boundary, not the cause of the missing post. The two Genius posts even carry the same text under different real IDs; they are not fabricated replacements.

The avax success/failure pair retains the actual last highwater and the following `PUBLIC_PROFILE_STALE_SAMPLE` report. The rejected own-post IDs and successful HTTP body were not retained; the empty failure post list is not evidence of an empty timeline. Deletion, pinning, truncation and stale cache remain indistinguishable from these records.

Tests inject exact retained parsed post lists at `fetch_profile`, then execute the real actor collection/composition and Tank consumer/polling logic. They do not reconstruct HTML or assert parser coverage. Reordering two genuine PancakeSwap samples is an explicit pending-batch regression, not a claim that those runs historically arrived in that order. The native stale case deliberately presents the older real sample against a newer real cursor; it is a refusal oracle, not the missing historical rejected response.

No complete historical timeline or recovery of the persistent avax/matchaxyz incidents is claimed. A missing-boundary poll may ingest visible new posts idempotently but may not advance its acknowledged cursor or certify history complete. Native HTTP/parser/deletion qualification requires the exact rejected response and its source boundaries, which are absent here.
