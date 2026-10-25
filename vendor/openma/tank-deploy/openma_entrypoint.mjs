export { main, replayDatabaseRoute, sandboxDatabaseRoute, verifyNativePlatform, verifyNativeQualification } from "./openma_entrypoint.mts";
import { runEntrypoint } from "./openma_entrypoint.mts";
await runEntrypoint(import.meta.url);
