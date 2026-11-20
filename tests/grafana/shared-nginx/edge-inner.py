import base64
import http.cookiejar
import json
import os
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

HOST = "grafana.127-0-0-1.sslip.io"
HELPER = "/fixture-helper/site.py"
STATE = Path("/var/lib/tank-grafana-nginx")
SITE = Path("/etc/nginx/conf.d/zzzz-tank-grafana.conf")
CRON = Path("/etc/cron.d/tank-grafana-certbot")
DEFAULT = Path("/etc/nginx/conf.d/default.conf")


def api(opener, path, data=None, headers=None, origin=None):
    req = urllib.request.Request(
        (origin or f"https://{HOST}") + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        response = opener.open(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    return response.status, response.headers, response.read()


def setup_viewer():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    token = base64.b64encode(f"admin:{os.environ['TEST_ADMIN_PASSWORD']}".encode()).decode()
    status, _, body = api(
        opener,
        "/api/admin/users",
        {
            "name": "Edge Viewer",
            "email": "viewer@example.invalid",
            "login": "edge-viewer",
            "password": os.environ["TEST_VIEWER_PASSWORD"],
        },
        {"Authorization": f"Basic {token}"},
        "http://127.0.0.1:3000",
    )
    assert status == 200, (status, body)


def verify_session(opener):
    status, _, body = api(opener, "/api/user")
    user = json.loads(body)
    assert status == 200 and user["login"] == "edge-viewer" and not user["isGrafanaAdmin"]
    status, _, body = api(opener, "/api/user/orgs")
    assert status == 200 and any(org["role"] == "Viewer" for org in json.loads(body))
    assert api(opener, "/api/admin/settings")[0] == 403


def viewer_session():
    root = run("curl", "--noproxy", "*", "-kfsS", "https://pebble:15000/roots/0").stdout
    Path("/tmp/pebble-root.pem").write_text(root)
    context = ssl.create_default_context(cafile="/tmp/pebble-root.pem")
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=context),
        urllib.request.HTTPCookieProcessor(cookies),
    )
    assert api(opener, "/api/user")[0] == 401
    status, headers, _ = api(
        opener, "/login", {"user": "edge-viewer", "password": os.environ["TEST_VIEWER_PASSWORD"]}
    )
    assert status == 200, status
    session = next(value for value in headers.get_all("Set-Cookie") if value.startswith("grafana_session="))
    assert all(flag in session.lower() for flag in ("secure", "httponly", "samesite=lax"))
    verify_session(opener)
    for uid in ("tank-queue", "tank-pipeline"):
        status, _, body = api(opener, "/api/dashboards/uid/" + uid)
        dashboard = json.loads(body)["dashboard"]
        assert status == 200 and dashboard["uid"] == uid
        queries = 0
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                query = {**target, "intervalMs": 60000, "maxDataPoints": 1000}
                status, _, body = api(
                    opener,
                    "/api/ds/query",
                    {"from": "1794787200000", "to": "1794873600000", "queries": [query]},
                )
                result = json.loads(body)["results"][query["refId"]]
                assert status == 200 and "frames" in result and not result.get("error"), (
                    uid,
                    panel.get("title"),
                    result,
                )
                queries += 1
        assert queries
        print(f"PASS {uid}: {queries} panel queries", flush=True)
    print(
        "PASS verified TLS, Viewer login, cookie, access control and both provisioned dashboards", flush=True
    )
    return opener


def run(*args, check=True, input=None):
    result = subprocess.run(args, check=False, text=True, input=input, capture_output=True)
    if check and result.returncode:
        debug = Path("/tmp/edge-certbot.log")
        detail = debug.read_text() if debug.exists() else ""
        raise RuntimeError(f"{args}: {result.returncode}\n{result.stdout}\n{result.stderr}\n{detail}")
    return result


def request(family, host, tls=False, path="/"):
    address = "127.0.0.1" if family == 4 else "[::1]"
    scheme = "https" if tls else "http"
    return run(
        "curl", "--noproxy", "*", "-sS", "-k", "-i", "-H", f"Host: {host}", f"{scheme}://{address}{path}"
    ).stdout


def sockets():
    lines = run("ss", "-ltn").stdout.splitlines()
    return sorted(line.split()[3] for line in lines[1:] if line.split()[3].endswith((":80", ":443")))


def serial():
    cert = STATE / f"certbot/config/live/{HOST}/cert.pem"
    return run("openssl", "x509", "-in", str(cert), "-noout", "-serial").stdout.strip()


def served_serial(family):
    address = "127.0.0.1:443" if family == 4 else "[::1]:443"
    handshake = run("openssl", "s_client", "-connect", address, "-servername", HOST, input="")
    return run("openssl", "x509", "-noout", "-serial", input=handshake.stdout).stdout.strip()


def wait_for_serial(expected):
    for _ in range(50):
        if all(served_serial(family) == expected for family in (4, 6)):
            return
        time.sleep(0.1)
    raise AssertionError(f"nginx did not serve {expected}")


def main():
    if not Path("/.dockerenv").is_file() or not Path(HELPER).is_file():
        raise SystemExit("Use edge-run.py with its disposable fixture container")
    Path("/etc/cron.d").mkdir(exist_ok=True)
    Path("/usr/local/bin/systemctl").write_text(
        "#!/bin/sh\n[ \"$1 $2 $3\" = 'is-active --quiet cron' ] && exit 0\n[ \"$1 $2\" = 'reload nginx' ] || exit 1\nif [ -e /tmp/edge-fail-reload ]; then rm /tmp/edge-fail-reload; touch /tmp/edge-reload-observed; exit 1; fi\nexec nginx -s reload\n"
    )
    Path("/usr/local/bin/systemctl").chmod(0o755)
    Path("/usr/local/bin/certbot").write_text(
        '#!/bin/sh\nif [ -e /tmp/edge-fail-certbot ]; then touch /tmp/edge-fail-observed; exit 1; fi\nif [ "$1" = renew ]; then set -- "$@" --force-renewal --no-random-sleep-on-renew; fi\n/usr/bin/certbot --server https://pebble:14000/dir --no-verify-ssl "$@" > /tmp/edge-certbot.log 2>&1\nstatus=$?\ncat /tmp/edge-certbot.log\nexit $status\n'
    )
    Path("/usr/local/bin/certbot").chmod(0o755)
    run(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "/tmp/default.key",
        "-out",
        "/tmp/default.crt",
        "-days",
        "2",
        "-subj",
        "/CN=default.invalid",
    )
    baseline = """server {
    listen 80;
    listen [::]:80;
    listen 443 ssl;
    listen [::]:443 ssl;
    server_name default.invalid;
    ssl_certificate /tmp/default.crt;
    ssl_certificate_key /tmp/default.key;
    return 200 'default-sentinel';
}
server {
    listen 0.0.0.0:80;
    listen [::]:80;
    listen 0.0.0.0:443 ssl;
    listen [::]:443 ssl;
    server_name other.invalid;
    ssl_certificate /tmp/default.crt;
    ssl_certificate_key /tmp/default.key;
    return 200 'named-sentinel';
}
"""
    DEFAULT.write_text(baseline)
    run("nginx", "-t")
    subprocess.Popen(["nginx", "-g", "daemon off;"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _attempt in range(40):
        if len(sockets()) == 4:
            break
        time.sleep(0.1)
    try:
        for _attempt in range(40):
            if run("curl", "--noproxy", "*", "-sk", "https://pebble:14000/dir").returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("Pebble did not become ready")
        before = sockets()
        assert len(before) == 4, before
        for family in (4, 6):
            assert "default-sentinel" in request(family, "unknown.invalid")
            assert "named-sentinel" in request(family, "other.invalid", True)
        Path("/tmp/edge-fail-certbot").touch()
        failed = run("python3", HELPER, "apply", HOST, "3000", check=False)
        assert failed.returncode != 0, "failure injection unexpectedly succeeded"
        assert Path("/tmp/edge-fail-observed").exists(), "certbot failure injection was not reached"
        assert not SITE.exists() and not CRON.exists() and STATE.exists()
        assert DEFAULT.read_text() == baseline and sockets() == before
        for family in (4, 6):
            assert "default-sentinel" in request(family, "unknown.invalid")
            assert "named-sentinel" in request(family, "other.invalid", True)
        print("PASS certbot failure rollback and existing dual-stack sites", flush=True)
        Path("/tmp/edge-fail-certbot").unlink()
        run("python3", HELPER, "apply", HOST, "3000")
        assert SITE.exists() and CRON.exists()
        assert DEFAULT.read_text() == baseline and sockets() == before
        first = serial()
        wait_for_serial(first)
        for family in (4, 6):
            assert "308" in request(family, HOST)
            assert served_serial(family) == first
            assert "302" in request(family, HOST, True)
            assert "default-sentinel" in request(family, "unknown.invalid", True)
            assert "named-sentinel" in request(family, "other.invalid", True)
        print(f"PASS HTTP-01 issuance and dual-stack HTTPS; {first}", flush=True)
        setup_viewer()
        viewer = viewer_session()
        Path("/tmp/edge-fail-reload").touch()
        assert run("python3", HELPER, "apply", HOST, "3001", check=False).returncode != 0
        assert Path("/tmp/edge-reload-observed").exists()
        verify_session(viewer)
        print("PASS Viewer session retained after failed apply and rollback", flush=True)
        run("sh", "-c", CRON.read_text().strip().split(maxsplit=6)[6])
        second = serial()
        assert first != second, (first, second)
        wait_for_serial(second)
        for family in (4, 6):
            assert served_serial(family) == second
            assert "302" in request(family, HOST, True)
        verify_session(viewer)
        print(f"PASS forced HTTP-01 renewal; {second}", flush=True)
        run("python3", HELPER, "remove")
        assert not SITE.exists() and not CRON.exists() and STATE.exists()
        assert DEFAULT.read_text() == baseline and sockets() == before
        for family in (4, 6):
            assert "default-sentinel" in request(family, "unknown.invalid", True)
            assert "named-sentinel" in request(family, "other.invalid", True)
        print("PASS removal preserves dual-stack default and named sites", flush=True)
    finally:
        pass


if __name__ == "__main__":
    main()
