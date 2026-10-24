import { createHash } from 'node:crypto';
import { trustedLegacyBuild } from './legacy-build-provenance.js';
import { stableDigest, type RuntimeIdentity } from './runtime-identity.js';
import type { Allocation } from './boxlite-provider.js';

export type LegacyFingerprintInputs = {
  scope: string[];
  resources: unknown;
  skills: unknown;
  environmentName: string;
  environment: unknown;
  allocation: Allocation;
  databasePolicy: unknown;
};
export type LegacyResolution = {
  identity: RuntimeIdentity;
  provenance: { build: string; canonicalAbi: string; image: string; digest: string };
};
export type LegacyReceipt = LegacyResolution['provenance'] & {
  fingerprint: string;
  publicationHash: string;
  runtimeId: string;
};
export function retainedCompatibility(inputs: LegacyFingerprintInputs): Record<string, string> {
  const { image, rootfsPath, name, volumes, network, security, ...runtimeResources } = inputs.allocation;
  const components = {
    runtimeAbi: trustedLegacyBuild.runtimeAbi, scope: inputs.scope,
    resources: inputs.resources, skills: inputs.skills, environmentName: inputs.environmentName,
    environment: inputs.environment, runtimeResources, mounts: volumes, network, security,
    databasePolicy: inputs.databasePolicy,
  };
  return Object.fromEntries(Object.entries(components).map(([key, value]) => [key, stableDigest(value)]));
}
export function resolveLegacyProvenance(inputs: LegacyFingerprintInputs, fingerprint: string): LegacyResolution | null {
  const build = trustedLegacyBuild;
  if ('sha256:' + createHash('sha256').update(build.rawIndex).digest('hex') !== build.indexRawSha256
      || stableDigest(build.rawIndex) !== build.indexLegacyDigest) throw Error('retained_legacy_trusted_provenance_invalid');
  const verified = [build.runtimeIdentity, build.runtimeAbi].some(runtimeIdentity =>
    stableDigest({ ...inputs, runtimeIdentity, rootfsIndex: stableDigest(build.rawIndex) }) === fingerprint);
  if (!verified) return null;
  return {
    identity: { version: 2, image: build.image, compatibility: retainedCompatibility(inputs) },
    provenance: { build: build.runtimeIdentity, canonicalAbi: build.runtimeAbi, image: build.image, digest: stableDigest(build) },
  };
}
