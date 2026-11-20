\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE tank;
\ir queue-views.sql
\ir pipeline-views.sql
GRANT USAGE ON SCHEMA tank_metrics TO tank_grafana_reader;
GRANT SELECT ON tank_metrics.queue_now, tank_metrics.queue_terminal TO tank_grafana_reader;
COMMIT;
