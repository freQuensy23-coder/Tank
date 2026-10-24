"""Guarded same-pinned native DNS policy; changes only an owned build workcopy."""

import hashlib
import json
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent
BRIDGE = "src/deps/libgvproxy-sys/gvproxy-bridge"
EXPECTED = {
    "main.go": "14b73c01d2ed7d8ee8d9bd610ffcb8b3f9e6d330c17e3c649cb00e81ab6308c7",
    "go.mod": "a5d0a73c4da38777c8bc84222933ebe50636758b9996504611b96ff70eecda0e",
    "go.sum": "84dddcab5d612e04cac72c140477a7eeeb8ec01088f237f1dd0de157d9217c47",
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply(source):
    source = Path(source).resolve()
    commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if commit != "7873ae4995e171237a3342adbb84000c8ab9965b":
        raise ValueError("Pinned BoxLite commit mismatch")
    subprocess.run(["git", "-C", str(source), "diff", "--exit-code"], check=True, capture_output=True)
    bridge = source / BRIDGE
    vendor = bridge / "tank-vendor/gvisor-tap-vsock"
    original = json.loads((BASE / "upstream/SHA256.json").read_text())
    for name in list(EXPECTED) + ["tank-vendor/gvisor-tap-vsock/" + name for name in original]:
        target = bridge / name
        if target.is_symlink() or not target.resolve().is_relative_to(source):
            raise ValueError("Native DNS target must be a scoped plain file")
    for name, expected in EXPECTED.items():
        if sha(bridge / name) != expected:
            raise ValueError("Pinned BoxLite source mismatch: " + name)
    for name, expected in original.items():
        if sha(vendor / name) != expected:
            raise ValueError("Pinned gvisor-tap-vsock source mismatch: " + name)
    for path in [
        bridge / "tank_dns_policy.go",
        bridge / "tank_dns_policy_test.go",
        vendor / "pkg/services/dns/tank_policy_test.go",
    ]:
        if path.exists() or path.is_symlink():
            raise ValueError("Unexpected existing DNS policy asset")

    def edit(path, old, new) -> None:
        text = path.read_text()
        if text.count(old) != 1:
            raise ValueError("Native DNS patch anchor changed: " + str(path))
        path.write_text(text.replace(old, new, 1))

    edit(
        vendor / "pkg/types/configuration.go",
        "\tDNS []Zone",
        "\tDNS []Zone\n\n\t// Disable forwarding beyond local zones. Defaults false for existing callers.\n\tDNSForwardingDisabled bool",
    )
    edit(
        vendor / "pkg/virtualnetwork/services.go",
        "dns.New(udpConn, tcpLn, configuration.DNS)",
        "dns.NewWithForwardingDisabled(udpConn, tcpLn, configuration.DNS, configuration.DNSForwardingDisabled)",
    )
    dns = vendor / "pkg/services/dns/dns.go"
    edit(dns, "type dnsHandler struct {\n", "type dnsHandler struct {\n\tforwardingDisabled bool\n")
    edit(
        dns,
        "func (h *dnsHandler) addAnswers(m *dns.Msg) {\n",
        """func (h *dnsHandler) addAnswers(m *dns.Msg) {
    // Preflight every question: a local first question cannot hide an
    // external later question. No host resolver is called for a denied query.
    if h.forwardingDisabled {
        for _, q := range m.Question {
            local := new(dns.Msg)
            if !h.addLocalAnswers(local, q) {
                m.Answer = nil
                m.Rcode = dns.RcodeRefused
                return
            }
        }
    }
""",
    )
    edit(
        dns,
        "\t\tresolver := net.Resolver{",
        """        // Retain this check even after preflight: local zones can change via
        // the control mux, but that cannot enable external forwarding.
        if h.forwardingDisabled {
            m.Answer = nil
            m.Rcode = dns.RcodeRefused
            return
        }
\t\tresolver := net.Resolver{""",
    )
    edit(
        dns,
        """func New(udpConn net.PacketConn, tcpLn net.Listener, zones []types.Zone) (*Server, error) {
\thandler := &dnsHandler{zones: zones}""",
        """func New(udpConn net.PacketConn, tcpLn net.Listener, zones []types.Zone) (*Server, error) {
    return NewWithForwardingDisabled(udpConn, tcpLn, zones, false)
}

// NewWithForwardingDisabled preserves local zones while refusing all forwarded
// questions. No synthetic IPs, sinkhole zones or secondary resolver are used.
func NewWithForwardingDisabled(udpConn net.PacketConn, tcpLn net.Listener, zones []types.Zone, disabled bool) (*Server, error) {
\thandler := &dnsHandler{zones: zones, forwardingDisabled: disabled}""",
    )
    edit(
        bridge / "main.go",
        "\t\tDNS:               buildDNSZones(config),",
        "\t\tDNS:               buildDNSZones(config),\n\t\tDNSForwardingDisabled: tankIPOnlyAllowlist(config.AllowNet),",
    )
    with (bridge / "go.mod").open("a") as output:
        output.write(
            "\n// Reviewed source overlay of the checksum-pinned v0.8.7 dependency.\nreplace github.com/containers/gvisor-tap-vsock => ./tank-vendor/gvisor-tap-vsock\n"
        )
    for name in ["tank_dns_policy.go", "tank_dns_policy_test.go"]:
        (bridge / name).write_bytes((BASE / name).read_bytes())
    (vendor / "pkg/services/dns/tank_policy_test.go").write_bytes((BASE / "tank_policy_test.go").read_bytes())
    patched = (
        [
            bridge / "main.go",
            bridge / "go.mod",
            bridge / "tank_dns_policy.go",
            bridge / "tank_dns_policy_test.go",
        ]
        + [vendor / name for name in original if name != "LICENSE"]
        + [vendor / "pkg/services/dns/tank_policy_test.go"]
    )
    subprocess.run(["gofmt", "-w", *[str(path) for path in patched if path.suffix == ".go"]], check=True)
    receipt = {
        "upstream_boxlite_commit": "7873ae4995e171237a3342adbb84000c8ab9965b",
        "dependency": "github.com/containers/gvisor-tap-vsock@v0.8.7",
        "dependency_sum": "h1:mFMMU5CIXO9sbtsgECc90loUHx15km3AN6Zuhg3X4qM=",
        "original_bridge_sha256": EXPECTED,
        "original_dependency_sha256": original,
        "patched_sha256": {str(path.relative_to(source)): sha(path) for path in patched},
        "license_sha256": sha(vendor / "LICENSE"),
        "sdk_abi_changed": False,
        "native_qualification": "NOT_RUN",
    }
    (source / "tank-replay-dns-patch.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    print(json.dumps(apply(parser.parse_args().source), indent=2))
