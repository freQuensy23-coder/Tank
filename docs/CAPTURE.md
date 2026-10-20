# Collection and startup

Run `tank calibrate` offline before the first Tank service startup, review its evidence and apply both measured Jev cutoffs. Start the normal service with `tank serve` using the base Compose file. Production includes the configured X session through `compose.x.yaml`; local development may omit X when explicitly excluded.

Source and BASE market collection are part of the normal service. Historical acquisition libraries remain available to prepare calibration evidence; they do not expose a second service mode or bypass calibration. There is no public `tank capture` command or capture Compose workflow.

The previous local capture-only experiment stopped gracefully at `2026-10-18T21:51:49.898596966Z`. Its data and dated verification reports are retained as historical evidence, with interruptions preserved. They are not proof of the required full service acceptance.

Backups recover PostgreSQL dumps and checksummed bundles through the documented restore procedure. See [OPERATIONS.md](OPERATIONS.md) for calibration, startup and recovery commands.
