# Native callback continuation

This stage repairs native session input consumption. Partial declared tool-result groups are intake-only, complete groups resume the model once, and new user inputs remain queued in FIFO order. Mixed public batches retain their original execution ID; a private journal receipt and fenced atomic requeue preserve unconsumed input across restart. Successful phase yields refund one claim exactly once while preserving generation and the original absolute deadline. Failed finalization never yields or refunds.

The model receives a causal projection; accepted events and immutable history are unchanged. Private fence/progress metadata stays outside public serialization. Ambiguous committed output fails the original execution precisely, projects truthful idle status and permits later work; it does not fabricate an end-turn or missing tool result.

The canonical `deploy/prepare_openma_source.py` entrypoint applies this stage after replay networking. `prepare_overlay.apply(root)` requires all fifteen exact input/output guards before writing. It refuses symlinks, altered assets, unexpected generated paths and repeat application. The node composition source uses the fixed reviewed pre-final native identity; the final composer derives and embeds a fresh complete runtime identity.

No dependency, table, public API, native runtime, SDK, provider, prompt, tool routing or timeout changes are introduced. Mock qualification is recorded under `docs/verification/native-continuation-diagnosis-2026-10-19/`. Controller, native and real-model qualification must be recorded separately before adoption.
