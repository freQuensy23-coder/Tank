#!/bin/bash
# Second part of the CLI check: positive scout decision, reaction recapture guard and
# a verified Binance archive fetch, on tank_test_tank_ops_cli48.
set -uo pipefail
OUT=${1:?output dir}
mkdir -p "$OUT/out"
export CLI_DB=tank_test_tank_ops_cli48
. /srv/tank-work/logs/tank-ops/clienv.sh
TEST_URL=$TANK_DATABASE_URL
EVENT=$(docker exec -u postgres tank-postgres-1 psql -d $CLI_DB -At -c "select event_id from out.alerts limit 1")
# A synthetic scout proposal (the scout writes only scout.expansion_proposals).
/srv/tank/.venv/bin/python - <<'PY'
import asyncio, os
from tank.storage import PostgresRepository
async def main():
    repo = await PostgresRepository(os.environ["TANK_DATABASE_URL"]).open()
    try:
        if not await repo.get("scout.expansion_proposals", "PRP-TANKOPS-CLI-1"):
            await repo.put("scout.expansion_proposals", "PRP-TANKOPS-CLI-1", {
                "prp_id": "PRP-TANKOPS-CLI-1", "hook": "NEW_MECHANISM", "concept_name": "synthetic CLI check",
                "definition": "evidence only", "why_existing_fails": "n/a", "mechanism": "n/a", "evidence": [],
                "overlap": "none", "recommendation": "TEST_MORE", "confidence": "LOW", "occurrences": 1,
                "adversarial_review": {}, "human_action": None})
    finally:
        await repo.close()
asyncio.run(main())
PY
echo -e "id\texpect\texit\tcommand" > "$OUT/commands.tsv"
run() {
  local id=$1 expect=$2; shift 2
  ( cd /tmp && /srv/tank/.venv/bin/python -m tank.cli "$@" ) > "$OUT/out/$id.json" 2> "$OUT/out/$id.err"
  echo -e "$id\t$expect\t$?\ttank $*" >> "$OUT/commands.tsv"
}
run X01-scout-list ok scout list
run X02-scout-decide ok scout decide PRP-TANKOPS-CLI-1 APPROVE_FOR_SANDBOX
run X03-scout-decide-bad-action fail scout decide PRP-TANKOPS-CLI-1 SHIP_IT
run X04-reaction-recapture-no-checkpoint fail reaction "$EVENT" --token-id T --recapture POST_1H --reason "operator recapture check"
run X05-reaction-recapture-no-reason fail reaction "$EVENT" --token-id T --recapture POST_1H
run X06-archive-fetch ok archive fetch BTCUSDT 2026-10-01
run X07-archive-fetch-bad-date fail archive fetch BTCUSDT 2026-13-01
echo done
