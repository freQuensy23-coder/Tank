-- Run as local postgres administrator after migrations in tank AND tank_replay.
-- Separate roles prevent replay credentials from reading the production database.
DO $$ DECLARE agent_role text; db text := current_database(); ns text; BEGIN
  IF db NOT IN ('tank', 'tank_replay') THEN
    RAISE EXCEPTION 'Expected tank or tank_replay database';
  END IF;
  agent_role := CASE WHEN db = 'tank_replay' THEN 'tank_replay_agent' ELSE 'tank_agent' END;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = agent_role) THEN
    RAISE EXCEPTION 'Provision % with its distinct secret during database initialization', agent_role;
  END IF;
  EXECUTE format('ALTER ROLE %I SET default_transaction_read_only = on', agent_role);
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', db, agent_role);
  FOREACH ns IN ARRAY ARRAY['reg','ingest','evt','mkt','ana','out','ops','scout','qa'] LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', ns, agent_role);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', ns, agent_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE tank IN SCHEMA %I GRANT SELECT ON TABLES TO %I', ns, agent_role);
  END LOOP;
END $$;
-- Set connection limit and statement_timeout only after V0-G calibration.
