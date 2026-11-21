import base64
import http.cookiejar
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HOST = "grafana.127-0-0-1.sslip.io"
HELPER = "/fixture-helper/setup.sh"
SITE = Path("/etc/nginx/sites-enabled/zzzz-tank-grafana")
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
        "http://127.0.0.2:983",
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
        raise RuntimeError(f"{args[0]} exited {result.returncode}")
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
    cert = Path(f"/etc/letsencrypt/live/{HOST}/cert.pem")
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
    SITE.parent.mkdir(exist_ok=True)
    Path("/var/www/html").mkdir(parents=True, exist_ok=True)
    Path("/etc/nginx/conf.d/fixture.conf").write_text("include /etc/nginx/sites-enabled/*;\n")
    systemctl = Path("/usr/local/bin/systemctl")
    systemctl.write_text(
        "#!/bin/sh\ncase \"$*\" in 'is-active --quiet certbot.timer'|'is-enabled --quiet certbot.timer') exit 0;; "
        "'reload nginx') exec nginx -s reload;; *) exit 1;; esac\n"
    )
    systemctl.chmod(0o755)
    Path("/etc/letsencrypt").mkdir(exist_ok=True)
    Path("/etc/letsencrypt/cli.ini").write_text(
        "server = https://pebble:14000/dir\nno-verify-ssl = true\nregister-unsafely-without-email = true\n"
    )
    hook = Path("/etc/letsencrypt/renewal-hooks/deploy/nginx")
    hook.parent.mkdir(parents=True, exist_ok=True)
    hook.write_text("#!/bin/sh\nnginx -t && systemctl reload nginx\n")
    hook.chmod(0o755)
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
    location /.well-known/acme-challenge/ { root /var/www/html; }
    location / { return 200 'default-sentinel'; }
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
    for _attempt in range(40):
        if run("curl", "--noproxy", "*", "-sk", "https://pebble:14000/dir", check=False).returncode == 0:
            break
        time.sleep(0.25)
    else:
        raise RuntimeError("Pebble did not become ready")
    before = sockets()
    assert len(before) == 4, before
    for family in (4, 6):
        assert "default-sentinel" in request(family, "unknown.invalid")
        assert "named-sentinel" in request(family, "other.invalid", True)
    collision = Path("/etc/nginx/conf.d/duplicate.conf")
    collision.write_text(f"server {{ listen 80; server_name {HOST}; }}\n")
    lineage = Path(f"/etc/letsencrypt/renewal/{HOST}.conf")
    assert run("bash", HELPER, HOST, check=False).returncode != 0
    assert not SITE.exists() and not lineage.exists()
    collision.unlink()
    lineage.parent.mkdir(exist_ok=True)
    lineage.write_text("unowned")
    assert run("bash", HELPER, HOST, check=False).returncode != 0
    assert not SITE.exists() and lineage.read_text() == "unowned"
    lineage.unlink()
    run("bash", HELPER, HOST)
    first = serial()
    wait_for_serial(first)
    login = request(4, HOST, True, "/login")
    assert "X-Tank-Route: grafana" in login
    assert "X-Tank-Route:" not in request(6, HOST, True, "/login")
    setup_viewer()
    viewer = viewer_session()
    run("bash", HELPER, HOST)
    assert serial() == first
    run("certbot", "renew", "--force-renewal", "--no-random-sleep-on-renew")
    second = serial()
    assert first != second
    wait_for_serial(second)
    verify_session(viewer)
    for family in (4, 6):
        assert "308" in request(family, HOST)
        assert "302" in request(family, HOST, True)
        assert "default-sentinel" in request(family, "unknown.invalid", True)
        assert "named-sentinel" in request(family, "other.invalid", True)
    assert DEFAULT.read_text() == baseline and sockets() == before
    print("PASS repeat setup, native renewal and served leaf replacement", flush=True)


def https_ready():
    script = Path("/deploy.sh").read_text()
    start = script.index("https_ready() {")
    end = script.index("\n}", start) + 2
    return (
        run(
            "bash",
            "-c",
            script[start:end] + "\nexternal_host=$1; export CURL_CA_BUNDLE=/tmp/pebble-root.pem; https_ready",
            "fixture",
            HOST,
            check=False,
        ).returncode
        == 0
    )


def check_transition(legacy):
    assert SITE.is_file()
    assert https_ready() != legacy
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    origin = "http://127.0.0.1:3000" if legacy else "http://127.0.0.2:983"
    assert api(opener, "/api/health", origin=origin)[0] == 200
    for family in (4, 6):
        assert ("502 Bad Gateway" if legacy else "401 Unauthorized") in request(
            family, HOST, True, "/api/user"
        )
        assert "default-sentinel" in request(family, "unknown.invalid", True)
        assert "named-sentinel" in request(family, "other.invalid", True)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        check_transition(sys.argv[1] == "legacy")
