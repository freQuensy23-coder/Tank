# Vendored OpenMA

The complete OpenMA application is checked in under `vendor/openma`, based on
`openma-ai/open-managed-agents` commit
`85d5361f535c7221de975838dc48444e468cc02e`. Tank's changes are ordinary application
source in that directory. The static `vendor/openma/Dockerfile` is the Compose
build recipe; there is no source preparation or patching step.

Build from the Tank checkout with the usual deployment environment:

```sh
docker compose --env-file /path/to/compose.env build
```

The environment file contains installation paths, network values and KVM group
identity. It does not select an OpenMA source checkout. The build uses the
checked-in Node24/pnpm11.0.8 workspace lockfile and the native bundle in
`vendor/openma/tank-native`. No credentials are build arguments or image inputs.

The runtime entrypoint is
`vendor/openma/tank-deploy/openma_entrypoint.mjs`. It keeps the database routing,
secret loading and native-platform validation used by the deployed application.
The retained-session, browser, replay evidence, execution-status, stable skill
paths, mounted-file I/O and private-IP replay behavior are implemented directly
in the vendored source. Existing controller and guest identity fields retain
their application protocol meaning.

The native bundle carries its provenance and checksums. Source/toolchain pins,
redistribution notices and rebuild contracts remain under
[deploy/boxlite](boxlite/v0.10.6/README.md). A native rebuild has its own artifact
identity and needs matching architecture/KVM acceptance. A successful image
build does not qualify a different host architecture or adopt existing retained
workspaces.

Config validation, agent/model/skill registration and sandbox OCI preparation
remain separate operations described in [Operations](../docs/OPERATIONS.md).
`prepare_openma.py` registers application resources through the running API; it
does not modify or generate the OpenMA application source.
