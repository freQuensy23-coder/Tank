# Collection and startup

Run `tank calibrate` offline before the first Tank service startup, review its evidence and apply both measured Jev cutoffs. Then deploy ([OPERATIONS.md](OPERATIONS.md#deployment)).

Source and BASE market collection are part of the normal service. Historical acquisition libraries remain available to prepare calibration evidence; they do not expose a second service mode or bypass calibration. There is no public `tank capture` command. Market collection is the separate Go capture service `tank-capture` ([capture/README.md](../capture/README.md)).

The previous local capture-only experiment stopped gracefully at `2026-10-18T21:51:49.898596966Z`. Its data and dated verification reports are retained as historical evidence, with interruptions preserved. They are not proof of the required full service acceptance.

Backups recover PostgreSQL dumps and checksummed bundles through the documented restore procedure. See [OPERATIONS.md](OPERATIONS.md) for calibration, startup and recovery commands.
