-- Repartition legacy DEFAULT capture atomically without editing its immutable evidence.
CREATE OR REPLACE FUNCTION ops.protect_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.immutable THEN
  IF TG_OP='DELETE' AND TG_TABLE_SCHEMA='mkt' AND
    current_setting('tank.partition_relocation',true)='on' THEN RETURN OLD; END IF;
  IF TG_OP='DELETE' AND TG_TABLE_SCHEMA='mkt' AND
    (TG_TABLE_NAME='trades' OR TG_TABLE_NAME LIKE 'trades_%') AND
    current_setting('tank.retention_purge',true)='on' THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'immutable record: %', OLD.storage_key;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
