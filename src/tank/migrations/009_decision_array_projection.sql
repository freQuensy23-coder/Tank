-- §14 trade_reasons is an array, including when the compact source DDL lists adjacent fields.
CREATE FUNCTION ops.project_text_array(raw text) RETURNS text[] LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF raw IS NULL THEN RETURN NULL; END IF;
 IF left(raw,1) = '[' THEN RETURN ARRAY(SELECT jsonb_array_elements_text(raw::jsonb)); END IF;
 RETURN ARRAY[raw];
END $$;
ALTER TABLE ana.decisions ALTER COLUMN trade_reasons TYPE text[] USING ops.project_text_array(trade_reasons);
DROP FUNCTION ops.project_text_array(text);
