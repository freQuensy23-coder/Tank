ALTER TABLE ops.jobs ADD COLUMN created_at timestamptz;
ALTER TABLE ops.jobs ADD COLUMN finished_at timestamptz;
CREATE FUNCTION ops.stamp_job_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.created_at := coalesce(NEW.created_at, (NEW.document->>'created_at')::timestamptz, now());
 IF NEW.status IN ('DONE','FAILED','EXPIRED') THEN
  NEW.finished_at := coalesce(NEW.finished_at, (NEW.document->>'finished_at')::timestamptz,now());
 ELSE NEW.finished_at := NULL; END IF;
 NEW.document := NEW.document || jsonb_build_object('created_at',to_char(NEW.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'finished_at',CASE WHEN NEW.finished_at IS NULL THEN NULL ELSE to_char(NEW.finished_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END);
 RETURN NEW;
END $$;
CREATE TRIGGER job_lifecycle BEFORE INSERT OR UPDATE ON ops.jobs FOR EACH ROW EXECUTE FUNCTION ops.stamp_job_lifecycle();
-- Old rows have no trustworthy completion time; retain them for a full new retention period.
UPDATE ops.jobs SET created_at=coalesce((document->>'created_at')::timestamptz,now());
CREATE INDEX jobs_terminal_age ON ops.jobs(finished_at) WHERE status IN ('DONE','FAILED','EXPIRED');
