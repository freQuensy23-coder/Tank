# CLI: reviewer and operations commands on real PostgreSQL (2026-10-20, tank-ops)

The commands of 01 §7.4, HLD §9, TDD §13 and docs/OPERATIONS.md were run with the real `tank` CLI (code of branch `ws/tank-ops`) against PostgreSQL 16 on openclaw-dp.

## Data

- Test database `tank_test_tank_ops_cli48` holds a real chain that `tests/test_e2e.py::test_full_sse_pipeline_postgres[True]` wrote on PostgreSQL: source item → Jev filter/route → session → tools → card v1 (FULL) → update v2 → Telegram ledger (fake transport). The plugin [keep_e2e_db.py](keep_e2e_db.py) starts that chain 48 h in the past and keeps a copy of its database.
- [seed_horizon_bars.py](seed_horizon_bars.py) adds synthetic typed 1m flow bars over the H24 horizon of the card, so the realized move can be computed.
- Production `tank` is read through a `default_transaction_read_only=on` session; nothing was written to it.

## Results

[run_cli.sh](run_cli.sh) → [run1/commands.tsv](run1/commands.tsv) (36 commands); [run_cli_extra.sh](run_cli_extra.sh) → [run2/commands.tsv](run2/commands.tsv) (7 commands). Every command's exit code is the expected one. Outputs are in `run*/out/` (files over 200 kB are compressed as `.json.xz`), and they contain no secrets.

| 01 §7.4 / HLD §9 | Command | Result |
|---|---|---|
| List cards and updates for a period | `review list --since --until`, `--event-id`, `--limit 1` + `--after <next_after>` | 2 versions; the next page has the second version; a bad cursor is refused |
| Open a card: versions, evidence, delivery, realized move | `review show ALERT` / `EVENT` | v1 and v2 each have valid frozen evidence (hash), a delivery ledger GENERATED→GENERATED→SENT, and outcome H24 **COMPLETED**: realized +8.80 %, MFE +10.78 %, MAE −2.08 %, BTC-adjusted +7.80 %, direction correct (expected values in seed_horizon_bars.py) |
| Label CATCH / LATE / FALSE ALARM / NOISE | `review label ALERT … --version N` | 4 append-only labels, shown by `review show`; an unknown label, version or subject is refused |
| Event without a card: CORRECT SILENCE / MISS with reason | `review label EVENT CORRECT_SILENCE`, `MISS --miss-reason SOURCE_NOT_COVERED` | written; MISS without a reason is refused |
| Weekly miss review (03 §11.5), S1–S9 | `review metrics --since <week> --until` | report computed (test DB and production) |
| Reaction record | `reaction EVENT`; `--recapture` guards | list returned; recapture without a checkpoint or reason is refused |
| Scout | `scout list`, `scout decide PRP APPROVE_FOR_SANDBOX` | decision stored; unknown proposal or action refused |
| Sources, coverage | `sources check`, `coverage` | ok (test DB and production) |
| Archive | `archive fetch BTCUSDT 2026-10-01` | data.binance.vision file verified: 1440 rows, complete, no gaps |
| Schema | `db migrate` | ok up to 036 |
| Replay, bundle reproduce | `replay F-01 --database-url`, `bundle reproduce SHA` | the same-database URL is refused; an isolated empty database is accepted; **no captured replay packet or replay bundle exists on the server yet**, so the positive path is open |
| Doctor | `doctor` (host, production config) | runs. Not ready, as expected outside the container: the host has no /run/secrets, the S29 list_id is null, and sources.yaml has no contact_email yet. The chats are read from delivery.yaml |
| Backup | `backup` without TANK_BACKUP_DIR refused; full backup → restore in [../backup-restore-2026-10-20](../backup-restore-2026-10-20/README.md) | ok |

## Fixed during the check

- `review list/show --after` was broken: the CLI passed a string where the repository needs a `[time, key]` pair, and `list` printed no cursor. Now `list` and `show` return `pagination.next_after`, and `--after` takes it (e58fca7).

## Open

- Positive `replay` / `bundle reproduce`: this needs a captured replay packet (tank-sessions) and, for fresh-session mode, the deployed OpenMA (tank-platform).
- `doctor` inside the deployed `tank` container: after deploy.
- Weekly miss audit: a major event that Tank never recorded has no event_id. `review label` accepts only an alert_id or event_id (TDD §2.8), so such a MISS cannot be recorded. Spec question raised with the lead.
