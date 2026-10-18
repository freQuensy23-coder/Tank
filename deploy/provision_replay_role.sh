#!/bin/sh
# Run through: docker compose ... exec -T -u postgres postgres sh < deploy/provision_replay_role.sh
set -eu
export TANK_REPLAY_AGENT_DB_PASSWORD="$(cat /run/secrets/tank_replay_agent_db_password)"
psql -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
\getenv replay_password TANK_REPLAY_AGENT_DB_PASSWORD
SELECT 'CREATE ROLE tank_replay_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tank_replay_agent') \gexec
SELECT format('ALTER ROLE tank_replay_agent PASSWORD %L', :'replay_password') \gexec
ALTER ROLE tank_replay_agent SET default_transaction_read_only = on;
REVOKE CONNECT ON DATABASE tank_replay FROM PUBLIC, tank_agent;
GRANT CONNECT ON DATABASE tank_replay TO tank, tank_replay_agent;
REVOKE CONNECT ON DATABASE tank FROM tank_replay_agent;
SQL
