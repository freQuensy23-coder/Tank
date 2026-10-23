DO $$ DECLARE agent_role text; db text := current_database(); ns text; BEGIN
  agent_role := CASE WHEN db = 'tank_replay' THEN 'tank_replay_agent' ELSE 'tank_agent' END;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = agent_role) THEN
    RETURN;
  END IF;
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', db, agent_role);
  FOREACH ns IN ARRAY ARRAY['reg','ingest','evt','mkt','ana','out','ops','scout','qa'] LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = ns) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', ns, agent_role);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', ns, agent_role);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO %I', ns, agent_role);
    END IF;
  END LOOP;
END $$;
