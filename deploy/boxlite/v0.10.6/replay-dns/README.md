# Native replay DNS source overlay

This recipe builds the native replay DNS overlay. The existing primary uses
exact shim `b645c32295a3450c58536a7da9238ee44560a21800e16859ea906aa3801382fa`
with its unchanged build provenance `ed4d17b305f0695e7bcd1e82c8def8b541af07f7c069aa44c910389ebfbf3245`.
The separate `../replay-dns-qualified-artifact.json` records its later native
network qualification. The historical d44 bundle and `../qualified-artifact.json`
remain unchanged as build baselines and are rejected for normal deployment. It starts at BoxLite commit
`7873ae4995e171237a3342adbb84000c8ab9965b` and retains all original SDK ABI,
guest, firmware, security settings and deadlines. The shim uses the same
Bookworm dynamic-link recipe as its parent. Only the native Go network backend
changes.

A nonempty allowlist containing only valid IP or CIDR rules has no hostname
destinations to resolve. For that policy, the gateway preserves builtin local
DNS zones and DHCP, but refuses every question requiring a host resolver.
Empty/unrestricted policies and hostname policies retain upstream behavior.
The paired OpenMA adapter stage uses one validated private PostgreSQL IP for
replay; unrestricted analysis retains its original native configuration.

`apply_patch.py` guards the exact original bridge files and the modified files
of `github.com/containers/gvisor-tap-vsock@v0.8.7`. The complete module is first
fetched through Go's committed checksum contract, copied to an owned local
module directory, and selected through an explicit `go.mod` replacement. The
module LICENSE is retained unchanged. No shared module cache is patched.
Both UDP and TCP DNS use the same refusal path; every question is preflighted,
so a local first question cannot hide a forwarded later question. A second
check immediately before the resolver also prevents a concurrent zone change
from enabling forwarding. No fabricated DNS answers or sinkhole zone is used.

The three build phases use `monitored_build.py` and an explicit immutable, architecture-matched pinned builder image. It mounts no KVM device, HOME or credentials. Only dependency retrieval
and compilation have network access. The test phase runs with `--network none`.
Every 3 seconds, its host process samples host free bytes and the actual Docker
backing filesystem through its own local cache volume. No daemon storage tree is mounted. An 8-GiB floor or phase deadline stops
only the immutable, labeled build-container ID. The monitor exits with that
phase; no service/project or unrelated container can be stopped by this helper.

The historical default builder and registered artifact are ARM64. For a new x86
candidate, build the parent `Dockerfile`'s pinned `builder` stage on Linux amd64,
record its immutable local image ID, and pass both `--builder-image sha256:...`
and `--architecture amd64` to every phase. The monitor verifies the actual image
platform, records it in each receipt and passes it to the compiler guard. It
still requires 8 GiB free before compiling. The compiler selects the matching
Rust target; no ARM output or Go toolchain is reused for amd64. The packager accepts the exact eight-file official archive directly (no unused base compilation), or a pinned base export, matching
native build/test receipts and the x86 ELF/system-loader contract. It produces
a source-contract-verified local candidate whose hardware status remains `NOT_RUN`, never an amd64 hardware acceptance. No private binary registration is needed for normal preparation. The fixed ARM
registration and its raw source/qualification evidence remain unchanged.

For a new owned workcopy of the exact source and its pinned submodules, with
an empty `output/` directory, run:

```sh
python3 deploy/boxlite/v0.10.6/replay-dns/monitored_build.py /tmp/OWNED_BUILD prepare --network default --seconds 600
python3 deploy/boxlite/v0.10.6/replay-dns/monitored_build.py /tmp/OWNED_BUILD test --network none --seconds 600
python3 deploy/boxlite/v0.10.6/replay-dns/monitored_build.py /tmp/OWNED_BUILD build --network default --seconds 1800
```

The actual dated build used a new dedicated source/cache/output rather than
Colima, whose disk headroom was inadequate. Dependency download/build traffic
is public package retrieval, not a guest isolation probe. The full bridge suite
passed offline. The upstream dependency's unrestricted external-DNS tests fail
with network disabled; the raw result is preserved. The named patch controls
pass offline without a resolver mock: type/name/class refusal combinations,
local records, mixed questions, opcode variants, UDP/TCP handlers, and native
policy-selection cases. Build evidence distinguishes the failed test harness
attempts from the successful focused gate.

`package_candidate.py` checks the actual ELF contract, seven unchanged official
runtime files, exact patch/toolchain/submodule evidence and successful build/
test receipts. It writes a **new** `UNQUALIFIED_REPLAY_DNS_CANDIDATE` bundle and
fresh provenance. No d44 hardware result is inherited. New local bundles include the selected successful phase receipts and scoped evidence. The normal verifier checks the fixed source/strict-DNS contract, actual ELF/toolchain and seven unchanged official files before preparation; no per-build developer allowlist is required. Hardware qualification remains `NOT_RUN`, and bit-identical rebuilds are not promised. The historical ARM registration remains unchanged for backward compatibility. Use `package_candidate.py --help` for explicit build/test
receipt selection when preserving earlier failed harness attempts.

The exact b645 artifact passed three sequential real factory scopes: unrestricted
before, private-IP-only replay, unrestricted after. Configured gateway DNS,
raw public DNS, TCP and validated HTTPS were checked in both unrestricted scopes;
replay refused forwarded DNS and blocked the external destinations while its
private PostgreSQL read succeeded and a write failed with SQLSTATE42501. Each
scope was exactly archived, the native catalog was empty, and HOME ownership was
returned. Evidence is in `docs/verification/native-replay-network-r18-2026-10-19`.
This qualifies the native network/database component; Managed session behavior,
full E2E acceptance and production service adoption are separate claims.

Raw candidate provenance is **not** relabelled: it still records the build-time
`UNQUALIFIED_REPLAY_DNS_CANDIDATE`/`NOT_RUN` state. For the historical artifact, `prepare_boxlite_runtime.py --verify` checks the fixed later external qualification registry and exact b645/ed4 bytes. New local bundles use the public pinned build contract instead. Its returned
`external_qualification` describes that limited later proof. The candidate-only
verification command continues to verify the old candidate registration without
claiming qualification. Normal canonical preparation uses the default verifier;
no diagnostic flag replacement or d44 fallback is permitted.

The complete clean-Linux path is [INSTALL_LINUX.md](../INSTALL_LINUX.md). The supervisor defaults to the local Docker context; pass `--docker-context` explicitly for another daemon. Native/hardware and full-system acceptance remain separate from verified source preparation.
