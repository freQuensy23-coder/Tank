import type { Boxlite } from '/sdk-0106/dist/index.d.ts';
import type { BoxLiteClientPort } from '/app/packages/managed-runtime-boxlite/src/index.ts';
import { publicBoxLiteClient, type NativeBoxLiteClient } from '/audit/boxlite-provider.ts';
declare const official0106: Boxlite;
const publicSdk: NativeBoxLiteClient = official0106;
const pinnedProviderClient: BoxLiteClientPort = publicBoxLiteClient(publicSdk);
void pinnedProviderClient;
