# Tank X reader

Versioned source of the public-profile Apify worker, imported from build `c99YRMlKXube5V0gV` (0.2.2). `main.py` reads INPUT and writes OUTPUT once. `direct_fetch.py` validates inputs and composes snapshots; `public_profile.py` reads public X profiles without an X session, cookies or a configured proxy.

INPUT accepts `handles`, optional `concurrency` (default 6, maximum 8), `cursors`, and `requestId`. OUTPUT schema version 1 contains `health`, `posts`, `accounts`, `nextCursors`, `failedAccounts`, and `coverageGaps`. Tank owns baseline seeding, durable cursor advancement and item deduplication.

Each profile read has at most three attempts: immediately, after 1 second, then after 5 seconds. Only HTTP 408/429/5xx and transient transport failures retry; success stops the sequence. Accounts share a run deadline below the 110-second synchronous API timeout, shortened by `ACTOR_TIMEOUT_AT` when present. Failed accounts retain their cursors.

Failed accounts include `diagnostics`: the GET method/URL and empty request body, attempt times/outcomes, response status, allowlisted headers, and response body. Wire and decoded captures are each bounded to 2,000,000 bytes; truncation and known omitted wire bytes are explicit. UTF-8 bodies remain text; binary bodies are base64. Gzip decoding is bounded. Credentials and cookie headers are excluded. Tank sends response bodies and metadata as documents linked to the Ops notice and records per-document delivery receipts.

The existing permanent CI runs `tests/test_x_reader.py`, `tests/test_x_diagnostics.py`, and `tests/test_x_reader_integration.py`, installing this directory's pinned requirements separately from the shipping Tank environment. The integration suite uses isolated real HTTP listeners for profile failures, actor snapshots and Telegram multipart uploads; it does not call paid Apify runs or production Telegram.

Publication and activation belong only to the existing main-only deploy workflow with green release CI. No manual actor publication or direct production changes are part of development.
