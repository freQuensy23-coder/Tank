# Isolated sandbox qualification

Use the existing `ci.yml` workflow on the exact candidate branch with `sandbox-qualification=true`. Ordinary PR jobs retain their four-minute budget. Qualification jobs have separate dispatch-only deadlines and never publish or deploy images.

```sh
gh workflow run ci.yml --repo freQuensy23-coder/Tank --ref <candidate-branch> -f sandbox-qualification=true
```

The jobs install the frozen Node24/pnpm11 workspace, typecheck every touched runtime package, run the named coordinator integration inventory independently against SQLite and fresh PostgreSQL16, and attempt native Managed API upgrades on a GitHub-hosted amd64 KVM executor. Every SQL lane requires exact equality to `sql-qualification-cases.mjs`, nonempty zero-skip JUnit and successful exit. Typecheck failures remain failures even when independent SQL execution succeeds.

KVM preflight executes the real API-version and VM-create ioctls. On an ephemeral GitHub-hosted dispatch runner only, EACCES permits a per-runner-UID ACL on `/dev/kvm`; it never changes world permissions, uses emulation, or changes the local development host. Native containers keep the normal non-root user, KVM device/group and shipping native bundle, without privileged mode or relaxed sandbox security.

Native scenarios use isolated internal Docker networks, generated test-only credentials, disposable PostgreSQL, private volumes, a deterministic Anthropic-wire test provider and real candidate OpenMA/BoxLite services. No production credentials/routes, personal browser, paid model, registry publication, deployment or operator repair SQL are used. Images A/B contain distinct real executable and Python-package markers. Source SHA, run ID, OCI hashes, service image ID, session/input/event/resource identities, guest file/browser receipts and label-checked cleanup are retained in named artifacts; generated credentials and VM disks are excluded.

The native inventory includes two A-to-B rounds, writable-memory restart, metadata-only continuation, protected-resource refusal, actual in-progress chunk-transfer process death, durable target-publication acknowledgement loss, network-partition owner expiry with competing service resumes, and archival at a blocked final publication. The PostgreSQL wire proxy forwards actual database bytes unchanged and only holds the selected external before-publication/after-commit acknowledgement boundary. Transfer interruption observes real nonempty host chunk staging; lease expiry uses the original real clock, not altered SQL deadlines. Every native filesystem includes a multi-chunk user-state payload with an independently checked digest.

`Sandbox upgrade acceptance` fails for failed/skipped prerequisite jobs, missing or stale artifacts, duplicate/missing/unexpected native cases and partial evidence. Recognized historical-image migration remains explicitly unproved until the exact historical OCI fixture is available and actually exercised; the driver cannot issue full acceptance while any obligation is missing. A successful prerequisite or ordinary PR run is not native qualification.
