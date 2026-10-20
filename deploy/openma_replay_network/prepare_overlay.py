"""Guarded replay native-IP policy stage; DNS forwarding remains a separate gap."""

import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
TARGET = "packages/sandbox-adapter-litebox/src/litebox.ts"
INPUT_SHA256 = "4b304db1665f1f4151a25802ef0cfbe10d237d45b90f82a942abc452069b680c"


def apply(root):
    root = Path(root).resolve()
    path = root / TARGET
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError("Replay network target must be a scoped plain file")
    source = path.read_bytes()
    if hashlib.sha256(source).hexdigest() != INPUT_SHA256:
        raise ValueError("Replay network source guard mismatch")
    source = source.decode()
    anchor = 'import { randomBytes } from "node:crypto";'
    old = """    if (!env.TANK_SANDBOX_DATABASE_IPV4) throw new Error("Replay database address is not resolved");
    allowed.push(env.TANK_SANDBOX_DATABASE_IPV4);"""
    new = """    const address = env.TANK_SANDBOX_DATABASE_IPV4;
    const octets = address?.split(".").map(Number) ?? [];
    if (!address || isIP(address) !== 4 || !(octets[0] === 10
        || (octets[0] === 172 && octets[1]! >= 16 && octets[1]! <= 31)
        || (octets[0] === 192 && octets[1] === 168))) {
      throw new Error("Replay database must resolve to a private IPv4 service address");
    }
    // The configured host selects the database service; raw PostgreSQL uses
    // its already-resolved IP, not HTTP Host/TLS SNI authorization. A hostname
    // rule would put arbitrary 80/443 destinations into native inspection,
    // accepting the guest-side TCP handshake before deciding whether to dial.
    allowed.splice(0, allowed.length, address);"""
    if source.count(anchor) != 1 or source.count(old) != 1:
        raise ValueError("Replay network patch context mismatch")
    source = source.replace(anchor, anchor + '\nimport { isIP } from "node:net";', 1)
    source = source.replace(old, new, 1)
    manifest = BASE / "manifest.json"
    expected = json.loads(manifest.read_text())
    if hashlib.sha256(source.encode()).hexdigest() != expected["patched_sha256"]:
        raise ValueError("Replay network output integrity mismatch")
    path.write_text(source)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("prepared_source", type=Path)
    apply(parser.parse_args().prepared_source)
