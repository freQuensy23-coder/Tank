# Shared nginx edge fixture

Run from the repository root:

```sh
python3 tests/grafana/shared-nginx/edge-run.py
```

The runner builds the pinned nginx base image with Debian certbot and creates a uniquely named internal IPv4/IPv6 Docker network and disposable nginx, Pebble, PostgreSQL and Grafana containers. It publishes no host ports and removes only resources it created. Building the image needs access to Debian package repositories and, if not cached, the pinned image registries. Docker Compose with IPv6 network and reset-tag support is required. Passwords and the Grafana secret key are generated randomly for each run; Compose does not load the project's .env.

Inside the nginx container the release helper is copied to its own path and run against real nginx, certbot and Pebble, with the production absolute configuration paths. The fixture creates unrelated default and named sites and a self-signed default certificate. Grafana uses the pinned repository compose and shared nginx overlay, sharing nginx's network namespace so the shipping loopback upstream reaches it. PostgreSQL uses the HEL synthetic data and reader provisioning. Docker DNS maps the fixture hostname to nginx, and Pebble validates HTTP-01; no public ACME service is contacted. The systemctl shim signals nginx and reports cron active; it does not run systemd or the cron daemon. The certbot wrapper points at Pebble with test-only TLS verification disabled, injects one failure, and forces renewal without certbot's randomized cron delay. The installed cron command itself is executed, including its lock, nginx test and reload.

The fixture verifies rollback after certbot failure, unchanged default and named sites, unchanged IPv4/IPv6 sockets, issuance, redirects and the actual SNI certificate on both address families. An HTTPS client verifies the Pebble root and hostname, anonymous denial, login as a named non-admin Viewer, cookie security attributes, admin denial, both provisioned dashboards and every dashboard panel's database query against the HEL fixture time range. It checks the same session after an injected failed reload and rollback, and after forced renewal changes the served certificate serial. Removal preserves the unrelated sites. Certificate state is retained after removal by design. This is an HTTP/API test, not a headless-browser rendering test.
