#!/bin/sh
set -eu
export TANK_DB_PASSWORD="$(cat /run/secrets/tank_db_password)"
export OMA_DB_PASSWORD="$(cat /run/secrets/oma_db_password)"
export TANK_AGENT_DB_PASSWORD="$(cat /run/secrets/tank_agent_db_password)"
export TANK_REPLAY_AGENT_DB_PASSWORD="$(cat /run/secrets/tank_replay_agent_db_password)"
psql --username "$POSTGRES_USER" --dbname postgres --set ON_ERROR_STOP=1 <<'SQL'
\getenv tank_password TANK_DB_PASSWORD
\getenv oma_password OMA_DB_PASSWORD
\getenv agent_password TANK_AGENT_DB_PASSWORD
\getenv replay_password TANK_REPLAY_AGENT_DB_PASSWORD
CREATE ROLE tank LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'tank_password';
CREATE ROLE oma LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'oma_password';
CREATE ROLE tank_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'agent_password';
CREATE ROLE tank_replay_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'replay_password';
ALTER ROLE tank_replay_agent SET default_transaction_read_only = on;
ALTER ROLE tank_agent SET default_transaction_read_only = on;
CREATE DATABASE tank OWNER tank;
CREATE DATABASE oma OWNER oma;
CREATE DATABASE tank_replay OWNER tank;
REVOKE CONNECT ON DATABASE tank_replay FROM PUBLIC;
GRANT CONNECT ON DATABASE tank_replay TO tank, tank_replay_agent;
REVOKE CONNECT ON DATABASE oma FROM PUBLIC;
GRANT CONNECT ON DATABASE oma TO oma;
REVOKE CONNECT ON DATABASE tank FROM PUBLIC;
GRANT CONNECT ON DATABASE tank TO tank, tank_agent;
SQL
