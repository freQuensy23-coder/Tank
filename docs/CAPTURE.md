# V0-A capture before event calibration

Run `tank db migrate`, then `tank capture` to collect sources and BASE market data for the PRD §10 V0-A acceptance checkpoint. This explicit command enables `capture_only` for this process, synchronizes configured registry rows, and maintains database partitions independently of backups when the repository provides that operation. Stop with SIGINT or SIGTERM.

Capture retains source raw records and normalized items without scheduling FILTER_ITEM or ROUTE_ITEM. It does not start model workers, Jev classification, OpenMA sessions, the Codex gateway, event analysis, or Telegram delivery. Market capture ignores existing HOT event state. Normal source and market credentials/configuration remain necessary for actual live collection.

The command neither invents Jev cutoffs nor bypasses the calibrated cutoffs required by `tank serve`. V0-B remains a separate labeled-item calibration and event acceptance checkpoint. Captured source items do not automatically become analysis jobs when capture stops; prepare the reviewed V0-B calibration dataset explicitly. Running this command alone does not establish the required 72-hour V0-A acceptance evidence.
