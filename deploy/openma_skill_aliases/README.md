# Stable skill materialization stage

TDD §3.3 requires the data guide at `/home/user/.skills/tank-data/`.
The unchanged registered SKILL also refers to that path. This stage preserves
the existing versioned descriptor and materialization and writes the exact same
manifest-relative bytes under `/home/user/.skills/<compiled-name>/`.

This is a separate stage after the frozen SKILL resolver fix. It accepts only
`node-managed-session-inputs.ts` SHA256
`265f4c3ba423a31b5286f5a5035c09efd8520aff8b6429f2c539e53fe87b5156`:

```sh
python3 deploy/openma_skill_aliases/prepare_overlay.py /absolute/new-prepared-workcopy
```

`apply(root)` validates the input, frozen assets, absence of new modules and
expected output before writing. `manifest.json` lists the target and generated
modules for the composition owner. The normal public composer currently excludes
this stage; image/native qualification and reviewed adoption are separate.

All custom skills are resolved to concrete immutable versions before skill
copies. Metadata name must match the manifest and the upstream compiler's
lowercase slug restriction (maximum64 characters). Duplicate names across
attachments are rejected before skill writes. Archives retain their bytes and
paths; no instruction text, registered ID, version or descriptor is changed.
The helper retains compiler bounds of500 files,32MiB/file and100MiB contents,
with a128MiB compressed-input bound and4096-character guest path bound.

The fixed Python3.12 worker runs through existing bounded SandboxExecutor.exec
with30seconds per check/claim. Its metadata request is at most48KiB; larger
attachment sets use batches after a complete name preflight, without a total
attachment cap. A receipt is at most2048bytes and binds name, skill ID, concrete
version and original archive SHA. It rejects unknown directories, mismatched
bindings, symlink parents/receipts/alias trees and nonregular entries before
copying skill bytes. Existing alias tree validation is bounded to8192 entries.

The worker writes an owned O_EXCL temporary receipt, fsyncs complete JSON, and
atomically links it to its final name without overwriting an existing receipt.
Concurrent EEXIST requires exact binding validation; it then fsyncs the receipt
directory before creating the alias directory. Failed publication removes only
the current call's temporary file. A hard crash may leave an unreferenced
temporary receipt, which is not adopted or broadly cleaned. A completed claim
allows the same binding to retry all identical files after interrupted
preparation. A changed version/name/archive cannot reuse that directory.
The stage never deletes an alias directory or reads guest files through a host
bind. It uses the existing binary write API after claims succeed. The existing
command-error contract and the explicit worker completion marker are both
required before copying.

Offline verification distinguishes these scopes:

- Seven controls run the actual upstream resolver/materializer with the exact
  registered ZIP and fake SandboxExecutor I/O; they check both paths and bytes,
  collision refusal, concrete latest binding and worker failure handling.
- Thirteen filesystem/fault controls execute the real claim worker against a
  temporary filesystem, including concurrent receipt claims and interrupted
  atomic publication. The actual sandbox image's Python3.12.15 also passes
  claim/restart/mismatch controls in a network-disabled container, without KVM.
- The unmodified twelve upstream input tests give ten passes and two failures:
  one fake exec never acknowledges the new worker, and one downloaded ZIP stub
  lacks mandatory compiler frontmatter. The original log is preserved.
- A separate, labeled fixture-adapted copy supplies only the exact known worker
  acknowledgment and valid compiler frontmatter, preserving `SKILL_INPUT_OK`
  and every original behavior assertion. All twelve adapted tests pass. See
  `fixture-adaptation.json`; no production instruction bytes changed.

These are not native alias-path acceptance. Root's existing Managed qualifier
must still read both versioned and required stable paths unchanged. Mounted
file API I/O is a separate proposed stage and is not included here.
