The native harness now advertises the actual unique `SKILL.md` directory from the compiled skill ZIP. A registered archive containing `tank-data/SKILL.md` is materialized at `<version-root>/tank-data/SKILL.md`; its descriptor must advertise `<version-root>/tank-data/`. Previously the descriptor advertised `<version-root>/SKILL.md`, and the actual approved Managed diagnostic failed at that read before its guest command ran.

The resolver downloads and validates the archive in every exposure mode, including progressive mode without an inlined body. The descriptor and optional body come from the same unique manifest. Missing, ambiguous, unsafe, duplicate-normalized, malformed-ZIP and invalid-UTF8 manifests fail before exposure. Archive bytes, skill contents and materialization remain unchanged.

`prepare_overlay.py` exposes `apply(root)` and a CLI:

```sh
python3 deploy/openma_skills/prepare_overlay.py /absolute/path/to/prepared-openma
```

The input resolver must have SHA256 `93bb57eb4deb6681a81e00dc887f5ca4efdeb8214fd8a8a678f313c03939127b`; repeat or changed-source application fails. The output resolver is `265f4c3ba423a31b5286f5a5035c09efd8520aff8b6429f2c539e53fe87b5156`. The helper and regression test/fixture are copied alongside the existing source/tests. The internal base function invokes this guarded stage and includes the target plus new assets in its identity manifest. The canonical `prepare_openma_source.py` operator command now delegates to the [complete composer](../OPENMA_PREPARATION.md), which adds the unchanged retained/execution stages and refreshes final composite hashes and runtime identity.

`manifest.json` freezes the source/asset identities. `test-command.json` reproduces 15 regression tests against the actual upstream resolver and materializer in an existing network-disabled image. The exact public registered ZIP fixture, including its original ZIP bytes, matches the earlier stage-v6 registration hash. `test-result-combined.log` also includes 12 existing materializer tests. Python deployment tests (16), strict new-helper TypeScript compilation, Ruff and actual source guard checks pass. These are offline checks; the corrected image still needs native Managed qualification. The archived failed session is preserved under `docs/verification/managed-replay-offline-2026-10-19/actual-r12-v3`.

Mounted file transfer is a separate constraint: exact BoxLite0.10.6 source refuses copyIn/copyOut paths covered by guest container mounts. The existing `readFile` adapter uses copyOut, so built-in reads beneath `/replay/evidence` or `/mnt/session/outputs` need a separate guest-I/O contract qualification/fix. `/workspace` is ordinary rootfs in this deployment; that constraint does not explain the missing skill path above. No mounted-file workaround is part of this patch.
