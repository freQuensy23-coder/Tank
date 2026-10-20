# F-10: current two-session replay handoff

Do not start while the r82 ordinary-source E2E owns the shared QA controller, network alias `tank`, replay evidence mount and native catalogue. No native replay was launched by this audit.

The authentic prepared reference is [the r45 handoff](../../../data/replay/acceptance-F03-F10-context-r45-v2/F-10/handoff.json). Its input is ready under the current read-only acceptance gate. The later [r56 packet](../../../data/qa/replay-f10-r56/handoff.json) preserves both NEW_ITEM messages, original source/market observations, registry, cutoff and financial predicates. There is no runtime private-name-map dependency.

The last [r56 result](../replay-f10-r56-2026-10-20/result.json) is negative: the first native turn completed, two sessions were created, but the second native boot failed. It is not evidence that a second independent fact should be appended to the first bound event. The older r45 EVENT_ALREADY_BOUND/workflow failure is superseded as a runtime diagnosis, not erased as evidence.

Use the existing harness, with a new attempt directory and the exact current tuple:

- Tank image: `sha256:aeff9822a0a29414e5563f05c3f0e1b05fb222e30331cb0ff71f99c654a81402`.
- Code: `43977ae5a00fed630eb80aa1a482312f9dcb6a544cb0c87be0837d9bb4af92b8`.
- OpenMA image: `sha256:da8d1b4c986fb852d856453a62ad56fa0cd26df17139114b9c5adf9656f00f16`.
- Registered QA context9/data2 and `tank-replay` environment from `data/qa/normal-source-e2e-r82-20261020/config/openma.yaml`. Preserve the current controller/native settings rather than replaying the obsolete r56 OpenMA compose section.

The existing [prepare recipe](../../../data/qa/replay-f10-r56/prepare.py) needs only a successor output path, these image/code pins and the current QA OpenMA config source. Its equivalence guards must still prove that registry, tool captures, cutoff, assembly time, numeric config, source/market observations and message boundaries are unchanged. Freeze the successor masked registry/config version; bind the **unchanged** financial/manual predicates to that successor input hash before execution. Copy the four already captured raw dependencies from the source handoff after checking their existing hashes. Do not fetch substitute news or invent web-search responses.

After root releases the QA controller, use the existing stage/admission procedure and an empty isolated `tank_replay` database with its existing read-only agent grants. Keep all failed attempt directories/databases intact. The normal CLI executed by the existing [bounded driver](../../../data/qa/replay-f10-r56/scripts-r56b/driver.py) is:

```
tank replay F-10 --database-url <private QA tank_replay DSN>
```

The driver constructs that DSN in memory from Settings; do not put its value in logs or shell history. It starts the existing Codex gateway on `tank:8790`, runs the CLI for at most 600 seconds and reaps only its owned child process group. The executor command after staging a successor under `/etc/tank-extract-v4/replay-f10-r82` is:

```sh
/opt/homebrew/bin/colima -p tank ssh -- sudo docker compose \
  --env-file /etc/tank-extract-v4/f01-r12.env \
  -f /etc/tank-extract-v4/replay-f10-r82/compose.yaml \
  run --rm --no-deps --use-aliases \
  --name tank-r82-f10-driver \
  -v /etc/tank-extract-v4/replay-f10-r82/scripts:/diagnostic:ro \
  --env TANK_DIAGNOSTIC_F08_GO=approved-qa-only \
  tank /opt/tank/venv/bin/python /diagnostic/driver.py
```

That command is **not ready to run until the successor is staged and its admission receipt is PASS**. The r56 paths must not be silently reused; the driver output path must be new (`/data/replay/qa-r82-f10-driver`). Root/agent_workflow own this release and staging step.

Readback must freeze both session inputs/turns, all tool receipts, database rows and framework output before evaluating the presealed predicates. Specifically, evaluate the testing claim separately from the Janus validator claim: IN_DEVELOPMENT/BUILT for testing, no LIVE assertion about testing, and any MAJOR classification must be M2 with the original testing quote. Do not upgrade the separate validator fact into ICE/NYSE production adoption. Archive only the sessions created by this attempt, verify no replay connections remain, then rename the owned QA database with its actual terminal outcome. Technical CLI completion alone is not a formal pass or Telegram delivery.
