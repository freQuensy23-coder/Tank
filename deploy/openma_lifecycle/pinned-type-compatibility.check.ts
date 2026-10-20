import * as realProvider from '/app/packages/managed-runtime-boxlite/src/index.ts';
import { SqlRuntimeResourceFencePort } from '/app/packages/runtime-resource-fence-sql/src/index.ts';
import type { PinnedBoxLiteModule } from '/audit/boxlite-provider.ts';
import type { Fences } from '/audit/retained-session.ts';
const provider: PinnedBoxLiteModule = realProvider;
const fences: Fences = new SqlRuntimeResourceFencePort(null as never);
void provider; void fences;
