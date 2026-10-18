#!/usr/bin/env python3
"""Generate restrictive PostgreSQL HBA rules from explicitly observed installation CIDRs."""

import argparse
import ipaddress
from pathlib import Path


def prepare(compose_cidr: str, sandbox_cidr: str | None, output: Path, *, bootstrap_only=False) -> str:
    if bootstrap_only and sandbox_cidr is not None:
        raise ValueError("Bootstrap must not invent a sandbox CIDR")
    if not bootstrap_only and sandbox_cidr is None:
        raise ValueError("An observed sandbox CIDR is required outside bootstrap")
    networks = []
    for label, value in [("Compose", compose_cidr)] + ([] if bootstrap_only else [("sandbox", sandbox_cidr)]):
        network = ipaddress.ip_network(value, strict=True)
        private_ranges = [
            ipaddress.ip_network(cidr) for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
        ]
        if network.version != 4 or not any(network.subnet_of(private) for private in private_ranges):
            raise ValueError(f"{label} CIDR must be an explicit private IPv4 subnet")
        networks.append(network)
    if networks[0].num_addresses < 8:
        raise ValueError("Compose subnet must fit three services plus gateway and reserved addresses")
    agent_rules = (
        "# Bootstrap only: all sandbox roles remain denied until VM source is observed.\n"
        if bootstrap_only
        else f"host tank tank_agent {networks[1]} scram-sha-256\nhost tank_replay tank_replay_agent {networks[1]} scram-sha-256\n"
    )
    text = f"""# Generated from explicit installation CIDRs; no trust or public network access.
local all postgres peer
local all all reject
host tank,tank_replay tank {networks[0]} scram-sha-256
host oma oma {networks[0]} scram-sha-256
{agent_rules}host all all 0.0.0.0/0 reject
host all all ::/0 reject
"""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text)
    output.chmod(0o644)
    return text


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose-cidr", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sandbox-cidr", help="Observed VM source CIDR; never guess")
    source.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Start service owners only; deny every sandbox connection until observed",
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    prepare(args.compose_cidr, args.sandbox_cidr, args.output, bootstrap_only=args.bootstrap_only)
