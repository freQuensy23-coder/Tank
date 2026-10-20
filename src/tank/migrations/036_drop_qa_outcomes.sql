-- W11: qa.outcomes is not a table of HLD §7.1. The outcome evaluator (TDD §11.5) is
-- descriptive: `tank review show` computes the realized move from frozen evidence and
-- stored market data on demand, so nothing needs to persist it.
DROP TABLE IF EXISTS qa.outcomes;
