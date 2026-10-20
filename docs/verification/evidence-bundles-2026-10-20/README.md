# Evidence bundles: every card frozen with its evidence (2026-10-20, tank-ops)

01 §7.3 says: "Every card version can be reproduced from what Tank stored with it". 03 §11 asks for the same, and so do HLD §2 rule 8 and 01 §8 rule 15.

[verify_card_bundles.py](verify_card_bundles.py) is read-only. For every `out.alerts` version it checks:
1. the bundle row exists, and SHA-256 of the canonical payload equals the `evidence_bundle_id` of the card and the `sha256` of the row;
2. the bundle file `bundles/{sha}.json.zst` (with `--data-dir`) decompresses to the same hash;
3. the bundle holds what 01 §7.3 lists:
   - source items with raw SHA-256, and quotes that point to those items;
   - Market Truth per decision with spot and derivatives venue detail and its window;
   - exclusion fields;
   - EMS, outlooks, decisions with grades and rules;
   - config, code, agent, prompt, model and thresholds versions;
4. `compose_card` rebuilds the exact card text from the frozen inputs, using the verdict and WHY of the `publish_card` call stored in the bundle's `request_log`;
5. the delivery ledger has rows for the version.

## Run 1: full synthetic chain on PostgreSQL ([run1/report.json](run1/report.json))

The database is `tank_test_tank_ops_cli48`, the same one used in [../cli-review-2026-10-20](../cli-review-2026-10-20/README.md): the E2E chain source → Jev → session tools → `publish_card` FULL v1 and UPDATE v2 → Telegram ledger.

Result: **2/2 versions frozen with evidence**. Both hashes are valid, and both card texts are reproduced byte for byte. Each version has 3 ledger rows. The bundles honestly record one unresolved field (`skill_version`), because the synthetic session exposes no skill version.

The bundle files of this run were written to the pytest temporary directory, which was removed afterwards. File hashes were therefore not re-checked here. Publication itself checks them (`publish_card` refuses on mismatch), and the backup check covered the production bundle files ([../backup-restore-2026-10-20](../backup-restore-2026-10-20/README.md)).

## Still to do (status 2)

- Full fresh-session replay on a test DB (`tank replay` / `bundle reproduce`). No captured replay packet exists on the server yet (tank-sessions, tank-platform).
- The first live card after the stack starts: run `verify_card_bundles.py --data-dir /data` against production read-only.
