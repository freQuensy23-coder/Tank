DO $$ DECLARE agent_role text; db text := current_database(); ns text; owner text; BEGIN
  agent_role := CASE WHEN db = 'tank_replay' THEN 'tank_replay_agent' ELSE 'tank_agent' END;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = agent_role) THEN
    RETURN;
  END IF;
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', db, agent_role);
  FOREACH ns IN ARRAY ARRAY['reg','ingest','evt','mkt','ana','out','ops','scout','qa'] LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = ns) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', ns, agent_role);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', ns, agent_role);
      FOR owner IN SELECT DISTINCT r FROM unnest(ARRAY[current_user::text, 'tank']) r WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) LOOP
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO %I', owner, ns, agent_role);
      END LOOP;
    END IF;
  END LOOP;
END $$;
