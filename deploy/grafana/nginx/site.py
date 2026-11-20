import base64
import fcntl
import fnmatch
import glob
import hashlib
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CRON = Path("/etc/cron.d/tank-grafana-certbot")
STATE = Path("/var/lib/tank-grafana-nginx")
CERTBOT = STATE / "certbot"
WEBROOT = STATE / "webroot"
MANIFEST = STATE / "state.json"
JOURNAL = STATE / "transaction.json"
LOCK = Path("/run/lock/tank-grafana-nginx.lock")
SITES = (
    Path("/etc/nginx/sites-enabled/zzzz-tank-grafana.conf"),
    Path("/etc/nginx/conf.d/zzzz-tank-grafana.conf"),
)
TOKEN = re.compile(r"""\s+|\#[^\n]*|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s;{}#]+|[;{}]""")


class Refusal(Exception):
    pass


def run(args, *, check=True):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, text=True)
    if check and result.returncode:
        raise Refusal(f"{args[0]} failed; output withheld to protect host configuration")
    return result


def digest(data):
    return hashlib.sha256(data).hexdigest()


def atomic(path, data, mode=0o644):
    fd, name = tempfile.mkstemp(prefix=".tank-grafana-", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def save(state):
    atomic(MANIFEST, json.dumps(state, sort_keys=True).encode(), 0o600)


def present(path):
    return path.exists() or path.is_symlink()


def load():
    if STATE.is_symlink() or not STATE.is_dir() or MANIFEST.is_symlink() or not MANIFEST.is_file():
        raise Refusal("owned state is missing or unsafe")
    try:
        for path in (STATE, MANIFEST):
            info = path.stat()
            if info.st_uid != 0 or info.st_mode & 0o022:
                raise ValueError()
        state = json.loads(MANIFEST.read_text())
        if state["version"] != 2 or Path(state["site_path"]) not in SITES:
            raise ValueError()
        for path, key in ((Path(state["site_path"]), "site"), (CRON, "cron")):
            expected = state[key]
            if expected is None:
                if present(path):
                    raise ValueError()
            elif path.is_symlink() or not path.is_file() or digest(path.read_bytes()) != expected:
                raise ValueError()
        if any(path.is_symlink() for path in (CERTBOT, WEBROOT)):
            raise ValueError()
        return state
    except (KeyError, ValueError, OSError, TypeError, json.JSONDecodeError):
        raise Refusal("owned state does not match its manifest") from None


def hostname(value):
    match = re.fullmatch(r"grafana\.(\d{1,3})-(\d{1,3})-(\d{1,3})-(\d{1,3})\.sslip\.io", value)
    if not match:
        raise Refusal("invalid hostname")
    try:
        ip = ipaddress.IPv4Address(".".join(match.groups()))
    except ValueError:
        raise Refusal("invalid hostname address") from None
    if str(ip).replace(".", "-") != "-".join(match.groups()):
        raise Refusal("hostname address must be canonical")


def inspect(host, site_path=None):
    text = run(["nginx", "-T"]).stdout
    sections = re.split(r"(?m)^# configuration file (.+):\n", text)
    if len(sections) < 3:
        raise Refusal("cannot inspect nginx configuration")
    files = {}
    for index in range(1, len(sections), 2):
        name, content = sections[index : index + 2]
        if name in files and files[name] != content:
            raise Refusal("ambiguous nginx configuration source")
        files[name] = content
    root = sections[1]
    stack = []
    servers = []
    active = set()
    included = []
    site_patterns = []

    def parse(name):
        if name in active or name not in files:
            raise Refusal("cannot prove nginx include order")
        active.add(name)
        included.append(name)
        original_depth = len(stack)
        statement = []
        tokens = TOKEN.findall(files[name])
        for token in tokens:
            if token.isspace() or token.startswith("#"):
                continue
            if token == "{":
                if not statement:
                    raise Refusal("unsupported nginx configuration")
                block = {"name": statement[0], "directives": [], "source": name}
                if statement[0] == "server" and len(stack) == 1 and stack[0]["name"] == "http":
                    servers.append(block)
                stack.append(block)
                statement = []
            elif token == "}":
                if statement or len(stack) <= original_depth:
                    raise Refusal("unsupported nginx configuration")
                stack.pop()
            elif token == ";":
                if not statement:
                    raise Refusal("unsupported nginx configuration")
                if statement[0] == "include":
                    if len(statement) != 2 or "$" in statement[1] or "\\" in statement[1]:
                        raise Refusal("unsupported nginx include")
                    pattern = statement[1]
                    if not pattern.startswith("/"):
                        pattern = "/etc/nginx/" + pattern
                    if len(stack) == 1 and stack[0]["name"] == "http":
                        site_patterns.append(pattern)
                    for child in sorted(glob.glob(pattern)):
                        parse(child)
                elif stack and stack[-1] in servers:
                    stack[-1]["directives"].append(statement)
                statement = []
            else:
                statement.append(token.strip("\"'"))
        if statement or len(stack) != original_depth:
            raise Refusal("unsupported nginx configuration")
        active.remove(name)

    parse(root)
    if not servers:
        raise Refusal("no nginx http servers")
    owned = str(site_path) if site_path else None
    if owned and included.count(owned) != 1:
        raise Refusal("nginx does not include the owned site exactly once")
    sockets = {}
    owned_count = 0
    for server in servers:
        directives = server["directives"]
        names = [
            value for directive in directives if directive[0] == "server_name" for value in directive[1:]
        ]
        is_owned = server["source"] == owned
        if is_owned:
            owned_count += 1
            if names != [host]:
                raise Refusal("unexpected owned server_name")
        else:
            for name in names:
                name = name.lower()
                if name.startswith("~") or "$" in name or "\\" in name:
                    raise Refusal("cannot prove server_name does not collide")
                if (
                    name == host
                    or (name.startswith("*.") and host.endswith(name[1:]))
                    or (name.startswith(".") and (host == name[1:] or host.endswith(name)))
                    or (name.endswith(".*") and host.startswith(name[:-1]))
                ):
                    raise Refusal("colliding server_name")
        listens = [directive[1:] for directive in directives if directive[0] == "listen"]
        if not listens:
            listens = [["80"]]
        for listen in listens:
            if not listen:
                raise Refusal("unsupported nginx listen")
            address, *flags = listen
            address = {
                "80": "0.0.0.0:80",
                "*:80": "0.0.0.0:80",
                "443": "0.0.0.0:443",
                "*:443": "0.0.0.0:443",
            }.get(address, address)
            if address not in ("0.0.0.0:80", "0.0.0.0:443", "[::]:80", "[::]:443"):
                raise Refusal("unsupported nginx listen address")
            if any(flag not in ("default_server", "ssl", "ipv6only=on", "http2") for flag in flags):
                raise Refusal("unsupported nginx listen option")
            sockets.setdefault(address, []).append((is_owned, "default_server" in flags))
    required = {"0.0.0.0:80", "0.0.0.0:443", "[::]:80", "[::]:443"}
    if {
        address for address, entries in sockets.items() if any(not entry[0] for entry in entries)
    } != required:
        raise Refusal("existing nginx must own all four public listeners")
    if site_path and owned_count != (2 if b"ssl_certificate" in site_path.read_bytes() else 1):
        raise Refusal("missing or duplicated owned server")
    if owned:
        for entries in sockets.values():
            defaults = [entry for entry in entries if entry[1]]
            if len(defaults) > 1 or (
                any(not entry[0] for entry in entries) and (defaults[0] if defaults else entries[0])[0]
            ):
                raise Refusal("owned site would become a default server")
    return site_patterns


def site(host, port, tls):
    template = (Path(__file__).parent / ("https.conf" if tls else "http.conf")).read_text()
    return template.replace("@HOST@", host).replace("@PORT@", str(port)).encode()


def cron():
    return (Path(__file__).parent / "renew.cron").read_bytes()


def install_file(path, data, state, key):
    atomic(path, data)
    state[key] = digest(data)
    save(state)


def remove_file(path, state, key):
    if state[key] is not None:
        path.unlink()
        state[key] = None
        save(state)


def reload_nginx():
    run(["nginx", "-t"])
    run(["systemctl", "reload", "nginx"])


def begin(state, old_site, old_cron, candidates):
    journal = {
        "state": state.copy(),
        "site": base64.b64encode(old_site).decode() if old_site is not None else None,
        "cron": base64.b64encode(old_cron).decode() if old_cron is not None else None,
        "site_allowed": [digest(value) for value in candidates],
        "cron_allowed": [digest(cron())],
    }
    if old_site is not None:
        journal["site_allowed"].append(digest(old_site))
    if old_cron is not None:
        journal["cron_allowed"].append(digest(old_cron))
    atomic(JOURNAL, json.dumps(journal, sort_keys=True).encode(), 0o600)


def recover():
    if not present(JOURNAL):
        return
    if STATE.is_symlink() or JOURNAL.is_symlink() or not JOURNAL.is_file():
        raise Refusal("unsafe transaction journal")
    for path in (STATE, JOURNAL):
        info = path.stat()
        if info.st_uid != 0 or info.st_mode & 0o022:
            raise Refusal("unsafe transaction journal")
    try:
        journal = json.loads(JOURNAL.read_text())
        state = journal["state"]
        target = Path(state["site_path"])
        if state["version"] != 2 or target not in SITES:
            raise ValueError()
        for path, key in ((target, "site"), (CRON, "cron")):
            if present(path):
                if path.is_symlink() or not path.is_file():
                    raise ValueError()
                if digest(path.read_bytes()) not in journal[key + "_allowed"]:
                    raise ValueError()
        for path, key in ((target, "site"), (CRON, "cron")):
            encoded = journal[key]
            if encoded is None:
                if present(path):
                    path.unlink()
            else:
                data = base64.b64decode(encoded, validate=True)
                if digest(data) != state[key]:
                    raise ValueError()
                atomic(path, data)
        save(state)
        reload_nginx()
        JOURNAL.unlink()
    except (KeyError, TypeError, ValueError, OSError):
        raise Refusal("transaction recovery failed; manual recovery required") from None


def apply(host, port):
    hostname(host)
    if not re.fullmatch(r"[1-9][0-9]{0,4}", port) or not 1 <= int(port) <= 65535:
        raise Refusal("invalid loopback port")
    for command in ("nginx", "systemctl", "certbot", "flock", "openssl"):
        if shutil.which(command) is None:
            raise Refusal(f"{command} is required")
    if all(
        run(["systemctl", "is-active", "--quiet", service], check=False).returncode
        for service in ("cron", "crond")
    ):
        raise Refusal("an active cron service is required for certificate renewal")
    if present(STATE):
        state = load()
        if state["host"] != host and (state["site"] or state["cron"]):
            raise Refusal("owned certificate belongs to another hostname")
        target = Path(state["site_path"])
        inspect(host, target if state["site"] else None)
    else:
        if any(present(path) for path in (*SITES, CRON)):
            raise Refusal("target state already exists")
        patterns = inspect(host)
        target = next(
            (
                path
                for path in SITES
                if path.parent.is_dir()
                and any(fnmatch.fnmatchcase(str(path), pattern) for pattern in patterns)
            ),
            None,
        )
        if target is None:
            raise Refusal("no supported nginx site directory")
        STATE.mkdir(mode=0o755)
        state = {
            "version": 2,
            "host": host,
            "port": port,
            "site_path": str(target),
            "site": None,
            "cron": None,
        }
        save(state)
    old_site = target.read_bytes() if state["site"] else None
    old_cron = CRON.read_bytes() if state["cron"] else None
    begin(state, old_site, old_cron, [site(host, port, False), site(host, port, True)])
    try:
        WEBROOT.mkdir(mode=0o755, exist_ok=True)
        CERTBOT.mkdir(mode=0o700, exist_ok=True)
        live = CERTBOT / "config" / "live" / host
        cert = live / "fullchain.pem"
        key = live / "privkey.pem"
        fresh = not (cert.is_file() and key.is_file())
        if (
            fresh
            or run(
                ["openssl", "x509", "-in", str(cert), "-noout", "-checkend", "86400"], check=False
            ).returncode
        ):
            if fresh and present(live):
                raise Refusal("incomplete owned certificate lineage")
            install_file(target, site(host, port, False), state, "site")
            inspect(host, target)
            reload_nginx()
            run(
                [
                    "certbot",
                    "certonly",
                    "--non-interactive",
                    "--agree-tos",
                    "--keep-until-expiring",
                    "--cert-name",
                    host,
                    "--register-unsafely-without-email",
                    "--webroot",
                    "-w",
                    str(WEBROOT),
                    "-d",
                    host,
                    "--config-dir",
                    str(CERTBOT / "config"),
                    "--work-dir",
                    str(CERTBOT / "work"),
                    "--logs-dir",
                    str(CERTBOT / "logs"),
                ]
            )
            if not cert.is_file() or not key.is_file():
                raise Refusal("certbot did not create the expected lineage")
        install_file(target, site(host, port, True), state, "site")
        inspect(host, target)
        reload_nginx()
        install_file(CRON, cron(), state, "cron")
        state["host"] = host
        state["port"] = port
        save(state)
        JOURNAL.unlink()
    except Exception:
        try:
            recover()
        except Exception:
            raise Refusal("apply failed and rollback failed; manual recovery required") from None
        raise


def remove():
    if not present(STATE):
        if any(present(path) for path in (*SITES, CRON)):
            raise Refusal("unowned target state exists")
        return
    state = load()
    target = Path(state["site_path"])
    old_site = target.read_bytes() if state["site"] else None
    old_cron = CRON.read_bytes() if state["cron"] else None
    if old_site is None and old_cron is None:
        return
    begin(state, old_site, old_cron, [])
    try:
        remove_file(CRON, state, "cron")
        remove_file(target, state, "site")
        reload_nginx()
        JOURNAL.unlink()
    except Exception:
        try:
            recover()
        except Exception:
            raise Refusal("remove failed and rollback failed; manual recovery required") from None
        raise


def main():
    if os.geteuid() != 0:
        raise Refusal("must run as root")
    os.umask(0o022)
    fd = os.open(LOCK, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        recover()
        if len(sys.argv) == 4 and sys.argv[1] == "apply":
            apply(sys.argv[2], sys.argv[3])
        elif sys.argv[1:] == ["remove"]:
            remove()
        else:
            raise Refusal("usage: site.py apply HOST PORT | site.py remove")


if __name__ == "__main__":
    try:
        main()
    except (Refusal, OSError) as error:
        print(f"tank-grafana-nginx: {error}", file=sys.stderr)
        sys.exit(1)
