#!/bin/bash
set -euo pipefail
agent=${TANK_AGENT_DB_PASSWORD:?}
clickhouse-client --user tank --password "${TANK_DB_PASSWORD:?}" --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS mkt;
CREATE SETTINGS PROFILE IF NOT EXISTS tank_agent_readonly SETTINGS readonly = 1, max_execution_time = 120, max_memory_usage = 1500000000;
CREATE USER IF NOT EXISTS tank_agent IDENTIFIED WITH sha256_password BY '${agent//\'/\\\'}' HOST IP '172.30.60.10' SETTINGS PROFILE 'tank_agent_readonly';
GRANT SELECT ON mkt.* TO tank_agent;
SQL
