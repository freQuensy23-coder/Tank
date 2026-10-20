# Registered custom-tool kind

Pinned OpenMA `85d5361f535c7221de975838dc48444e468cc02e` classifies emitted tool calls using a built-in name set. A registered client custom tool named `web_search` consequently emits `agent.tool_use`, even when the built-in search is disabled. Pi correctly includes that pending callback in the declared group, which the continuation integrity check then rejects because its custom-use event is missing.

This stage marks explicit custom registrations and gives that marker precedence in the shared Pi/AI SDK event emitter. Existing built-ins, MCP tools and permission prompts keep their previous behavior. The continuation group integrity check is unchanged. No dependency, public API, prompt, tool name, network policy, model budget or native runtime changes.

The canonical `deploy/prepare_openma_source.py` composition applies this stage after the continuation stage. `apply(root)` verifies every source, asset and output hash before writing either target. Both targets retain their existing file-map owner, or become upstream patches when previously unmodified. The final composite identity binds this stage and its exact assets.

Offline tests exercise the actual builder, both harness paths, native SQLite coordinator/runner callback intake and continuation, enabled built-in search, an actual built-in permission prompt, and MCP/name-family classification. These tests use a faux model and no native SDK, VM, network or credentials. Actual deployment and model qualification require a separately reviewed successor tuple; old r28 attempts remain historical.
