# Replay native destination policy

This guarded stage applies after the frozen base/retained/execution/skill/file-I/O
stages; it modifies only `packages/sandbox-adapter-litebox/src/litebox.ts`.
`apply(root)` in `prepare_overlay.py` requires the exact shipping input hash and
checks the output against `manifest.json`. There is no provider overlap.

The configured `postgres` host is a service selector. The controller already
resolves its dedicated replay database URL to a private IPv4 address. The native
allowlist now contains only that validated address. The hostname previously
also enabled upstream HTTP Host/TLS SNI inspection for arbitrary port-80/443
IPs; its guest-side TCP accept can precede the upstream authorization decision.
Unrestricted live analysis is unchanged. Empty replay networking remains
`mode: disabled`; arbitrary hosts and non-private routes are refused.

This stage alone does **not** disable upstream gateway DNS forwarding. It must
be paired with the DNS-capable b645 native artifact. Its external qualification
registry records the actual r18 factory isolation and unrestricted controls;
normal runtime verification now rejects the old d44 fallback. This is native
component qualification, not Managed session or full E2E acceptance.

Offline evidence is under `docs/verification/replay-network-2026-10-19`.
The 13 Vitest controls exercise the actual factory, and the separate allocation
capture exercises the actual Managed constructor, retained controller and
pinned provider with only the low-level SDK synthetic. No actual SDK runtime,
VM, model, database or external network request is used by those controls.
The original allocation capture and actual failed Managed proof remain intact.
