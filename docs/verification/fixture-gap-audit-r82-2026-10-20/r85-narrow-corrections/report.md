# Narrow r85 corrections

Latest request line8 asks for an agent-authored absence explanation for each of eight layers. TDD §4.7 step10 (line1124) says: “No credible secondary: each layer reports NONE, UNKNOWN or 'not tradable' with a reason.”

set_beneficiaries previously used any credible edge, so a PRIMARY waived the secondary-absence note. The check now asks whether that layer contains a credible SECONDARY and requires a nonblank agent note otherwise. Agent prose is stored verbatim. PRIMARY evidence and existing FOUND status remain; an optional secondary-layer note stays empty instead of the invented 'Evidence-backed edges'.

TDD §7.4 (line1497) permits a standalone update when “verified state advanced”. alert_gate now requires the supplied state_verified=True for that trigger. It keeps an unverified LIVE stage intact and preserves action/class/negative/FATAL/expiry triggers.

Focused regression scenarios:9 passed. Related suite:229 passed,9 PostgreSQL-dependent skips with the PostgreSQL DSN unset; no PG/native run. See regression-final.log. One old unit expectation that explicitly allowed an unverified standalone ascent was corrected to the literal update rule; no financial fixture predicate changed. Ruff format passes four files. Full-file lint remains41 pre-existing findings, with zero added findings; see lint-comparison.json.

scoped.patch and after-owned/ contain only this agent's intended changes reconstructed against the captured before/ files. combined-observed.patch retains concurrent changes from the market agent that were visible during the broad test run. owned-changes.json distinguishes owned-only hashes from observed shared-file hashes. No production revert, image rebuild, deployment or real message was performed. The parent/market owner controls the final combined source pin.
