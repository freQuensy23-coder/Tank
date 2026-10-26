ALTER TABLE "out".alerts DROP CONSTRAINT valid_kind;
ALTER TABLE "out".alerts ADD CONSTRAINT valid_kind
    CHECK (kind IN ('PARTIAL', 'FULL', 'UPDATE', 'PRELIMINARY'));
