ALTER TABLE evt.ems_assessments DROP CONSTRAINT IF EXISTS required_gates;
ALTER TABLE evt.ems_assessments DROP CONSTRAINT IF EXISTS required_criterion;
ALTER TABLE evt.ems_assessments DROP COLUMN IF EXISTS criterion;
