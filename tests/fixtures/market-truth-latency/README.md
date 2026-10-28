# Public ENA latency component inputs

These fixtures contain public exchange prices, ENA flow bars and ENA open interest. They contain no Telegram/session exports, prompts, credentials, private-account data or event narratives.

The three price captures re-read the original incident's ClickHouse projection SQL at its original semantic and availability cutoffs. Each contains 129,720 rows, including the live cursor's two-hour margin; normal snapshot preparation trims that margin to the 129,600-point, 90-day analysis window. Flow and OI captures preserve actual observations and their completeness, precision and admissibility fields. The manifest records counts, permitted fields and hashes.

The re-read is **not an exact retained incident snapshot**: ClickHouse FINAL versions may have changed. The integration case reconstructs price preparation, relative analysis and five-window OI analysis. It omits other flow/price venues, funding, books, liquidations and supply, and does not reproduce production contention or network timing.

`expected-components.json` is a selected subset of the original persisted derived result, retained independently of the reconstruction. Do not regenerate it from the code under test. Classify any mismatch before changing either input or expectations.

The existing automatic integration lane produces cold cProfile measurements and unprofiled warm timings as JUnit properties. Profiling overhead, runner differences and missing components prohibit treating these measurements as an exclusive historical attribution of the production request's wall time.
