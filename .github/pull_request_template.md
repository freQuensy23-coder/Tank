<!--
Write the whole description in English. Keep every section short: facts, no narrative, no repetition.
Delete a section together with its heading when the rule inside it says it does not apply.
-->

## Summary

<!--
One or two plain-language lines: what this PR does and why.
Bug fix: at most three lines — what the bug was, what fixes it, and the evidence that this was the cause and that it is gone.
-->

## Product specification changes

<!--
Only when product behavior changes (01_Tank_V0_PRD.md). Delete this section for purely technical work
(a library or Python upgrade, a new index, a refactor): the product specification describes product
requirements and does not change then.
One line per change: section, then added / changed / removed, then what. Every behavior change in the
code appears here; nothing is left out.
-->

- PRD §… — added / changed (was … → now …) / removed: …

### Product diagram

<!--
Only when something already shown on diagrams/tank_v0_product_view.svg changed, or the user explicitly
asked to show something new on it. A detail the diagram never showed is not added on the author's own
initiative. Attach three images: before, after, and the diff with every change in red:
scripts/diagram_diff.sh <base-ref> diagrams/tank_v0_product_view.svg <out-dir>
Link before/after to the PNG at the base and head commits; commit the diff under docs/verification/<topic>/.
-->

| Before | After | Diff |
|---|---|---|
| ![before]() | ![after]() | ![diff]() |

## Technical changes

### Technical specifications

<!--
02_Tank_V0_HLD.md and 03_Tank_V0_Technical_Design.md. One line per change: section — was … → now … — why.
Delete when neither document changes.
-->

- HLD §… — was … → now … — because …

### Technical diagram

<!--
The same rule and the same three images for diagrams/tank_v0_technical_view.svg. Delete when it did not change.
-->

| Before | After | Diff |
|---|---|---|
| ![before]() | ![after]() | ![diff]() |

### Database

<!--
Only when the PostgreSQL or ClickHouse schema changes. One row per migration, then one line on the idea.
Regenerate the schema diagrams with `uv run python scripts/render_db_schema.py --png`; `--check` fails while
they are out of date. Attach before / after / diff images of each schema diagram that changed, the same way
as the diagram images above:
scripts/diagram_diff.sh <base-ref> diagrams/tank_postgres_schema.svg <out-dir>
scripts/diagram_diff.sh <base-ref> diagrams/tank_clickhouse_schema.svg <out-dir>
-->

| Store | Migration | Change |
|---|---|---|
| PostgreSQL | `src/tank/migrations/NNN_….sql` | … |
| ClickHouse | `tank.clickhouse_market` DDL | … |

Idea: …

| Schema | Before | After | Diff |
|---|---|---|---|
| PostgreSQL | ![before]() | ![after]() | ![diff]() |
| ClickHouse | ![before]() | ![after]() | ![diff]() |

### New or changed queries

<!--
Every new or changed SELECT / INSERT / UPDATE / DELETE in PostgreSQL or ClickHouse, including queries that an
agent skill now tells the agent to run. One row each. Delete when there are none.
-->

| Store | Table | What it does | Run by | How often |
|---|---|---|---|---|

### Database load

<!--
Required when the queries above add load. Use one of the three forms:
- Trivial: say why (for example, a 100-row registry table read once per item).
- Bounded: name the index, partition or limit that keeps it cheap.
- Heavy, frequent or unclear: measure it. Give the table sizes, the plan (EXPLAIN) and p50 / p95 before → after.
Then write exactly: I am confident this is safe for production.
A PR that adds database load without this sentence is not merged.
-->

## Tests

<!--
Required when this PR changes product behavior. A test checks a feature, not trivia: no tests that a config
parses or a constant exists. One row per new or changed test; delete a table without rows.
Speed: every unit case stays under 10 s and p90 under 1 s (docs/TESTING.md). When a new test may be slow (real
services, large fixtures, waits), measure it and give its time and the suite time before → after. A test that
slows the suite is fixed before the PR is opened, not reported.
-->

**Unit tests**

| Feature | Test | What it covers |
|---|---|---|

**Integration tests**

| Feature | Test | What it covers |
|---|---|---|

## Cut corners

<!--
What this PR deliberately left out, simplified or did not verify, and where. One line each, the biggest first.
-->

- …
