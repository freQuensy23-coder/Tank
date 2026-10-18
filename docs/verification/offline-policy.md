# Pytest offline policy

`tests/conftest.py` installs the boundary before test-module collection. It removes inherited service configuration and credential/proxy environment variables by name, without printing their values. Among Tank service variables, only explicit `TANK_TEST_DATABASE_URL`, `TANK_E2E_DATABASE_URL`, `TANK_SOURCE_TEST_DATABASE_URL`, and the synthetic Chromium opt-in flag survive. Tests can set fake environment values after the autouse fixture runs.

HOME, CODEX_HOME and XDG lookup roots are also temporary; only cached public Playwright executables retain their known cache path. Python file opens reject known operator credential paths (including auth.json, /run/secrets, /codex-auth, SSH/cloud credentials, .pgpass and the workspace .env), resolving symlinks before checking.

Every test disables Pydantic dotenv reads and gets empty temporary Docker-secret, Codex-auth, X-state and browser-profile defaults. Explicit synthetic constructor paths and fake environment overrides retain normal precedence. Production Settings code is unchanged. Gateway integration uses its own explicit fake JWT/auth file and internal key.

Python socket connect/connect_ex, UDP sendto/sendmsg, and DNS resolution reject external destinations. Loopback, AF_UNIX, and database hosts explicitly named in the test DSNs are allowed. External URLs served by HTTP MockTransport never create sockets and continue to work. Boundary tests intercept the native socket/resolver before any external probe.

This is an in-process Python pytest guard, not an operating-system firewall. Native libraries and subprocesses do not necessarily use Python sockets; PostgreSQL tests must use disposable explicit databases, and Chromium tests use synthetic state and local pages. For process-wide enforcement, also run pytest inside a network-isolated container with only the disposable PostgreSQL service accessible. Public live-smoke commands are separate from pytest and are not covered by this guard. No live acceptance claim follows from this test suite.

Run the default suite with `pytest`; optional database and synthetic Chromium tests require the explicit test variables above. Never supply production database URLs or operator credentials to those variables.

Pinned OpenMA source/SQL probes additionally accept explicit `OPENMA_SQL_TEST_SOURCE_DIR`, `OPENMA_SQL_TEST_CONTAINER`, and `OPENMA_SQL_TEST_DOCKER_HOST`. The last must be a local `unix://` endpoint for a disposable test Docker stack; the fixture sets it only with a temporary empty Docker config. Inherited operator Docker contexts and credentials remain disabled.
