\set ON_ERROR_STOP on
\getenv store_password GRAFANA_STORE_PASSWORD
CREATE ROLE tank_grafana_store LOGIN PASSWORD :'store_password';
CREATE SCHEMA grafana AUTHORIZATION tank_grafana_store;
ALTER ROLE tank_grafana_store SET search_path TO grafana;
