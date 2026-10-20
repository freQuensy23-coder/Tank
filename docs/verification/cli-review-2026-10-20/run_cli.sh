#!/bin/bash
# CLI verification (tank-ops, 2026-10-20): every reviewer and operations command
# of 01 §7.4, HLD §9 and docs/OPERATIONS.md, run with the real CLI against real
# PostgreSQL. Test database: tank_test_tank_ops_cli48, which holds the synthetic E2E
# chain (source -> Jev -> session -> card v1 + update v2 -> delivery) started 48 h
# in the past (keep_e2e_db.py), plus the typed bars of seed_horizon_bars.py over its
# H24 horizon. Production `tank` is read through a default_transaction_read_only session.
set -uo pipefail
OUT=${1:?output dir}
mkdir -p "$OUT/out"
export CLI_DB=tank_test_tank_ops_cli48
. /srv/tank-work/logs/tank-ops/clienv.sh
TEST_URL=$TANK_DATABASE_URL
REPLAY_URL="${TEST_URL%/*}/tank_test_tank_ops_replay"
PROD_URL="${TEST_URL%/*}/tank?options=-c%20default_transaction_read_only%3Don"
ALERT=$(docker exec -u postgres tank-postgres-1 psql -d tank_test_tank_ops_cli48 -At -c "select alert_id from out.alerts limit 1")
EVENT=$(docker exec -u postgres tank-postgres-1 psql -d tank_test_tank_ops_cli48 -At -c "select event_id from out.alerts limit 1")
BUNDLE=$(docker exec -u postgres tank-postgres-1 psql -d tank_test_tank_ops_cli48 -At -c "select document->>'evidence_bundle_id' from out.alerts where version=1")
echo -e "id\texpect\texit\tcommand" > "$OUT/commands.tsv"
n=0
run() {  # run <id> <expect: ok|fail> <db: test|prod> <args...>
  local id=$1 expect=$2 db=$3; shift 3
  n=$((n+1))
  local url=$TEST_URL; [[ $db == prod ]] && url=$PROD_URL
  ( cd /tmp && TANK_DATABASE_URL=$url /srv/tank/.venv/bin/python -m tank.cli "$@" ) > "$OUT/out/$id.json" 2> "$OUT/out/$id.err"
  local rc=$?
  echo -e "$id\t$expect\t$rc\ttank $*" | sed -e "s#$TEST_URL#<test-db>#g" -e "s#$REPLAY_URL#<replay-db>#g" >> "$OUT/commands.tsv"
}
P1=2026-10-18T00:00:00Z; P2=2026-10-21T00:00:00Z; W1=2026-10-14T00:00:00Z
# 01 §7.4 reviewer commands
run R01-list-period ok test review list --since $P1 --until $P2
run R02-list-event ok test review list --event-id "$EVENT"
run R03-list-page1 ok test review list --since $P1 --until $P2 --limit 1
NEXT=$(/srv/tank/.venv/bin/python -c "import json,sys;print(json.dumps(json.load(open(sys.argv[1]))['pagination']['next_after']))" "$OUT/out/R03-list-page1.json")
run R03b-list-page2 ok test review list --since $P1 --until $P2 --limit 1 --after "$NEXT"
run R03c-list-bad-cursor fail test review list --since $P1 --until $P2 --after cursor
run R04-show-card ok test review show "$ALERT"
run R05-show-event ok test review show "$EVENT"
run R06-label-catch-v1 ok test review label "$ALERT" CATCH --version 1 --reviewer tank-ops --notes "first card"
run R07-label-late-v2 ok test review label "$ALERT" LATE --version 2 --reviewer tank-ops
run R08-label-false-alarm ok test review label "$ALERT" FALSE_ALARM --version 2 --reviewer tank-ops
run R09-label-noise ok test review label "$ALERT" NOISE --version 1 --reviewer tank-ops
run R10-label-correct-silence ok test review label "$EVENT" CORRECT_SILENCE --reviewer tank-ops
run R11-label-miss ok test review label "$EVENT" MISS --miss-reason SOURCE_NOT_COVERED --reviewer tank-ops --notes "weekly miss audit"
run R12-label-miss-no-reason fail test review label "$EVENT" MISS --reviewer tank-ops
run R13-label-bad-label fail test review label "$ALERT" GOOD --reviewer tank-ops
run R14-label-unknown-subject fail test review label EVT-UNKNOWN NOISE --reviewer tank-ops
run R15-label-bad-version fail test review label "$ALERT" CATCH --version 9 --reviewer tank-ops
run R16-show-after-labels ok test review show "$ALERT"
run R17-weekly-metrics ok test review metrics --since $W1 --until $P2
run R18-reaction ok test reaction "$EVENT"
run R19-scout-list ok test scout list
run R20-scout-decide-unknown fail test scout decide PRP-UNKNOWN REJECT
# Operations commands (HLD §9, docs/OPERATIONS.md, 03 §13)
run O01-db-migrate ok test db migrate
run O02-sources-check ok test sources check
run O03-coverage ok test coverage
run O04-replay-same-db-refused fail test replay F-01 --database-url "$TEST_URL"
run O05-replay-isolated-no-packet fail test replay F-01 --database-url "$REPLAY_URL" --mode captured-tools
run O06-bundle-reproduce-no-packet fail test bundle reproduce "${BUNDLE#EVB-}" --database-url "$REPLAY_URL" --mode captured-tools
run O07-bundle-reproduce-bad-sha fail test bundle reproduce not-a-sha --database-url "$REPLAY_URL"
run O08-doctor fail prod doctor
run O09-backup-no-dir fail test backup
# Read-only production
run P01-prod-review-list ok prod review list --since 2026-10-01T00:00:00Z --until $P2
run P02-prod-metrics ok prod review metrics --since $W1 --until $P2
run P03-prod-scout-list ok prod scout list
run P04-prod-sources-check ok prod sources check
run P05-prod-coverage ok prod coverage
echo "ran $n commands"
