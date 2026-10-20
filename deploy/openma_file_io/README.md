This separate post-retained overlay implements Tank's runtime file APIs through guest process I/O. BoxLite0.10.6's native copyIn/copyOut service deliberately refuses paths beneath container mounts, including `/replay/evidence`, `/mnt/session/outputs` and `/tmp`. The production LiteBox sandbox delegates its text/binary file APIs through `RetainedBoxOperations`, so this stage replaces that bridge's transfer implementation. It does not read host bind paths or change the upstream SDK, runtime security, skill bytes, browser transport or browser budgets.

`prepare_overlay.py` exposes `apply(root)` for a prepared retained source tree:

```sh
python3 deploy/openma_file_io/prepare_overlay.py /absolute/path/to/prepared-openma
```

It requires the exact **shipping prepared provider** input SHA256 in `manifest.json`, checks every frozen asset, refuses existing output modules and symlink targets, patches the prepared provider and emits two local modules plus `tank-file-io-overlay.json`. This is the post-lifecycle `ecbbe30d...` provider, whose type-only retained-session import has been normalized to `.js`; the raw lifecycle `0c9f5972...` asset is explicitly refused. The earlier mistaken raw-asset freeze and its test receipts remain in `history/raw-asset-freeze-2f590b714a7d/`; `rebase-proof/` records the exact reviewed difference and new actual-source tests. Frozen lifecycle/skills assets stay unchanged. The normal deployment composer must include this reviewed stage and its resulting hashes before deriving the final runtime identity. Applying it manually without refreshing composite provenance is not a deployment.

The file worker is passed directly to the VM's existing Python3.12 interpreter through argv, uses only its standard library and needs no materialized skill or guest worker file. Paths are separate arguments. The guest kernel enforces mount and file permissions. Reads and writes support regular files, binary data and normal guest symlink resolution; nonregular files are refused using nonblocking open plus descriptor validation.

Each direction allows at most one64KiB data chunk in flight. This application acknowledgement is necessary because the pinned SDK has unbounded native exec queues and its JavaScript stream bridge eagerly enqueues output. A read worker waits for an acknowledgement after the host has decoded and staged each bounded base64 chunk. A writer sends raw binary stdin bytes and waits for the guest's actual kernel-write acknowledgement before sending another chunk. Strict capped frames and final length/SHA256 receipts detect truncation, corruption and extra output. No full encoded file is buffered. The cap remains512MiB, matching the pinned native transfer cap; the cap does not promise completion of every large file within the time budget.

The existing retained exec deadline remains one absolute120s file-operation budget, including process startup, streams and acknowledgements. There are no retries. Timeout or transport/parser failure kills the exact owned execution and poisons that runtime until the lifecycle safely stops it and supplies a fresh resumed runtime. Confirmed guest errno failures preserve Node-style `.code`/`.errno` and safe messages without paths. Reads stage into an owned host temporary file and replace the caller's destination only after successful completion. Writes preserve ordinary kernel open/write permissions and the prior nontransactional transfer semantics: an error or cancellation may leave a partially written guest file. This stage does not claim atomic guest writes or fsync durability. No browser75/80/90 or guest-ready30 budget changes.

Run the offline contract with explicit pinned interpreters and a new output directory:

```sh
python3 deploy/openma_file_io/run-offline-tests.py \
  --node /absolute/path/to/node24.19.0 \
  --python /absolute/path/to/python3.12 \
  --prepared-source /absolute/path/to/actual-prepared-openma \
  --output /absolute/path/to/new-proof
python3 deploy/openma_file_io/docker-worker-contract.py
```

The first uses real isolated temporary subprocesses and no SDK/controller/API/network. It includes11 original/new file controls, two explicit acknowledgement controls and eight retained-provider regressions. The provider regression file substitutes real Python for the old file-map fake and maps the fake guest `/workspace` into the test's owned directory; its lifecycle/deadline assertions remain intact. Blocked transports are explicitly synthetic while the owned child and unrelated-process cleanup control are real. The second uses the retained exact Python3.12 sandbox image on DockerDesktop with a public read-only fixture, no KVM or secret mounts. It proves actual read success, write EROFS and unchanged fixture bytes. Neither is native Managed qualification.

Before adoption, a new coherent source/image identity needs one actual factory file-API check of rootfs read/write, mounted evidence read and write denial, mounted output text/binary read/write, missing-path errors, and skill materialization/bootstrap through Python. Any actual native probe needs root GO, sufficient headroom and exclusive controller/HOME coordination. No such qualification is claimed by this stage.
