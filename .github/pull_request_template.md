<!--
Write the description in English. Keep every section short, without filler.
Delete a section, heading included, when its rule says it does not apply.
-->

## Summary

<!--
One or two plain-language lines: what was done.
Bug fix: at most three lines: what the bug was, what was done to fix it, and why we are sure this was the cause and it is gone.
-->

## Product specification changes

<!--
Only when the product specification (01_Tank_V0_PRD.md) changes. A technical change (a library version, a Python
upgrade, a database index) does not change it: delete this section then.
List every change, nothing left out: which specification is added, what is added, what is removed.
-->

- §… — added / removed / changed: …

### Product diagram

<!--
Only when something already shown on the product diagram changes, or the user explicitly asked to show something new
on it. The diagram stays high level: a detail it did not show before is not added unless the user asked.
Attach three images: the diagram before, the diagram after, and the diff with the changes highlighted in red.
-->

| Before | After | Diff |
|---|---|---|
| | | |

## Technical changes

### Technical specifications

<!--
Which technical specifications changed, one line each: was … → now … → why. As short as possible.
-->

- §… — was … → now … — why: …

### Database

<!--
Only when the database changes. What changed, as a migration; then one or two lines on the idea.
Attach three images of the database schema, for PostgreSQL and for ClickHouse: before, after, and the diff with the
changes highlighted in red.
-->

```sql
…
```

Idea: …

| Schema | Before | After | Diff |
|---|---|---|---|
| PostgreSQL | | | |
| ClickHouse | | | |

### New queries

<!--
New PostgreSQL and ClickHouse queries (SELECT, INSERT, UPDATE, DELETE), including queries that an agent skill now
tells the agent to run, even when the query is not written in the code.
-->

| Database | Query | Purpose |
|---|---|---|

### Database load

<!--
Only when the change adds database load.
- Obviously small (for example, a select from a 100-row token table): say that it is fine.
- Covered by indexes (for example, an hourly query over half a year of tables): name the indexes.
- Complex, frequent or unclear, and it has to be fast: measure it and give the parameters before → after.
Then write: I am confident this is safe for production.
A PR without this sentence is not merged.
-->

## Tests

<!--
Required when there is at least one product change. Tests check features, not trivia such as reading a config.
For every new test: the feature, the test, and the case it covers.
If a test may be slow, measure it and give its run time. A slow test is fixed before the PR, not merged as is.
-->

**Unit tests**

| Feature | Test | What it covers |
|---|---|---|

**Integration tests**

| Feature | Test | What it covers |
|---|---|---|

## Cut corners

<!--
The main corners cut in this PR: what was cut and where. One line each, sorted from the biggest to the smallest.
-->

- …
