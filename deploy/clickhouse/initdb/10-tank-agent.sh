#!/bin/bash
# First start only (empty data directory): the agents' read-only user. Agent sandboxes reach
# the services from the openma container's fixed address, with the password their
# TANK_DB_URL carries (secret tank_agent_db_password).
set -euo pipefail
agent=$(cat /run/secrets/tank_agent_db_password)
clickhouse-client --user tank --password "$(cat /run/secrets/tank_db_password)" --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS mkt;
CREATE SETTINGS PROFILE IF NOT EXISTS tank_agent_readonly SETTINGS readonly = 1, max_execution_time = 120, max_memory_usage = 1500000000;
CREATE USER IF NOT EXISTS tank_agent IDENTIFIED WITH sha256_password BY '${agent//\'/\\\'}' HOST IP '172.30.60.10' SETTINGS PROFILE 'tank_agent_readonly';
GRANT SELECT ON mkt.* TO tank_agent;
SQL
