\set ON_ERROR_STOP on
\getenv store_password GRAFANA_STORE_PASSWORD
\getenv reader_password GRAFANA_READER_PASSWORD
BEGIN;
CREATE ROLE tank_grafana_store LOGIN PASSWORD :'store_password';
CREATE ROLE tank_grafana_reader LOGIN CONNECTION LIMIT 4 PASSWORD :'reader_password';
GRANT CONNECT ON DATABASE tank TO tank_grafana_reader, tank_grafana_store;
REVOKE TEMPORARY ON DATABASE tank FROM PUBLIC;
GRANT TEMPORARY ON DATABASE tank TO tank, tank_agent, tank_grafana_store;
CREATE SCHEMA grafana AUTHORIZATION tank_grafana_store;
CREATE SCHEMA tank_metrics AUTHORIZATION tank;
ALTER ROLE tank_grafana_store IN DATABASE tank SET search_path = grafana;
ALTER ROLE tank_grafana_reader IN DATABASE tank SET search_path = tank_metrics;
ALTER ROLE tank_grafana_reader IN DATABASE tank SET default_transaction_read_only = on;
ALTER ROLE tank_grafana_reader IN DATABASE tank SET statement_timeout = '15s';
COMMIT;
