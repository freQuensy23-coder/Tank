# Tank OpenMA source

This is Tank's checked-in OpenMA fork, imported from
[openma-ai/open-managed-agents](https://github.com/openma-ai/open-managed-agents)
at commit `85d5361f535c7221de975838dc48444e468cc02e`.

All Tank runtime changes are present directly in this source tree. From the Tank
repository root, build it with `docker compose build openma`. No source preparation,
patch application, or generated Dockerfile is required.

The static Dockerfile preserves the frozen pnpm dependency graph and separate
dependency/source layers. The repository contains the exact Linux amd64 BoxLite
runtime in `tank-native/`; its archive and payload checksums are verified during
the image build. Native source, qualification provenance, and licenses are
recorded beside that archive.

`TANK_SOURCE.json` records the one-time import baseline. Original upstream
`README.md`, `LICENSE`, `NOTICE`, and source comments are retained. Future Tank
changes should be made directly to these committed files.
