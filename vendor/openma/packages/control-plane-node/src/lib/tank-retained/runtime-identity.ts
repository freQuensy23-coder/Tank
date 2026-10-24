import { createHash } from 'node:crypto';
import { Buffer } from 'node:buffer';
import { readFile } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { join } from 'node:path';

export interface RuntimeIdentity {
  version: 2;
  image: string;
  compatibility: Record<string, string>;
}
export const stableDigest = (value: unknown): string => {
  const normalize = (item: unknown): unknown => Array.isArray(item) ? item.map(normalize)
    : item !== null && typeof item === 'object'
      ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)).map(([name, child]) => [name, normalize(child)])) : item;
  return 'sha256:' + createHash('sha256').update(JSON.stringify(normalize(value)) ?? 'undefined').digest('hex');
};
const validDigest = (value: unknown): value is string => typeof value === 'string' && /^sha256:[a-f0-9]{64}$/.test(value);
export function validateRuntimeIdentity(value: unknown): asserts value is RuntimeIdentity {
  const identity = value as RuntimeIdentity | null;
  if (!identity || identity.version !== 2 || !validDigest(identity.image) || !identity.compatibility
      || typeof identity.compatibility !== 'object' || Array.isArray(identity.compatibility)
      || !Object.keys(identity.compatibility).length
      || Object.entries(identity.compatibility).some(([name, hash]) => !/^[a-zA-Z][a-zA-Z0-9]*$/.test(name) || !validDigest(hash))) {
    throw Error('retained_identity_invalid');
  }
}
export function assertCompatible(previous: RuntimeIdentity, current: RuntimeIdentity): void {
  validateRuntimeIdentity(previous); validateRuntimeIdentity(current);
  const changed = [...new Set([...Object.keys(previous.compatibility), ...Object.keys(current.compatibility)])]
    .filter(name => previous.compatibility[name] !== current.compatibility[name]).sort();
  if (changed.length) throw Error('retained_configuration_changed:' + changed.join(','));
}
interface Descriptor {
  mediaType: string;
  digest: string;
  size: number;
  platform?: { architecture?: string; os?: string; variant?: string };
}
export async function executableImageIdentity(root: string, architecture = process.arch): Promise<string> {
  const arch = ({ x64: 'amd64', arm64: 'arm64', amd64: 'amd64' } as Record<string, string>)[architecture];
  if (!arch) throw Error('retained_image_architecture_unsupported');
  const blob = async (descriptor: Descriptor, collect = true): Promise<Buffer> => {
    if (!validDigest(descriptor.digest) || !Number.isSafeInteger(descriptor.size) || descriptor.size < 0) throw Error('retained_image_descriptor_invalid');
    const hash = createHash('sha256'), chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of createReadStream(join(root, 'blobs', 'sha256', descriptor.digest.slice(7)))) {
      size += chunk.length; hash.update(chunk);
      if (collect) chunks.push(chunk as Buffer);
    }
    if (size !== descriptor.size || 'sha256:' + hash.digest('hex') !== descriptor.digest) throw Error('retained_image_blob_invalid');
    return Buffer.concat(chunks);
  };
  const selected: Array<{ manifest: any; config: any }> = [];
  const visit = async (index: any, depth: number): Promise<void> => {
    if (depth > 4 || index.schemaVersion !== 2 || !Array.isArray(index.manifests)) throw Error('retained_image_index_invalid');
    for (const descriptor of index.manifests as Descriptor[]) {
      if (descriptor.platform && (descriptor.platform.os !== 'linux' || descriptor.platform.architecture !== arch
          || (descriptor.platform.variant && descriptor.platform.variant !== 'v8'))) continue;
      const document = JSON.parse(Buffer.from(await blob(descriptor)).toString('utf8'));
      if (document.manifests) { await visit(document, depth + 1); continue; }
      if (document.schemaVersion !== 2 || !document.config || !Array.isArray(document.layers)) throw Error('retained_image_manifest_invalid');
      const config = JSON.parse(Buffer.from(await blob(document.config)).toString('utf8'));
      if (config.architecture !== arch || config.os !== 'linux') continue;
      if (!config.rootfs || !Array.isArray(config.rootfs.diff_ids) || config.rootfs.diff_ids.length !== document.layers.length
          || config.rootfs.diff_ids.some((hash: unknown) => !validDigest(hash))) throw Error('retained_image_config_invalid');
      for (const layer of document.layers as Descriptor[]) await blob(layer, false);
      selected.push({ manifest: document, config });
    }
  };
  await visit(JSON.parse(await readFile(join(root, 'index.json'), 'utf8')), 0);
  if (selected.length !== 1) throw Error('retained_image_selection_ambiguous');
  const { manifest, config } = selected[0]!;
  const { created, history, ...executableConfig } = config;
  return stableDigest({ version: 1, config: executableConfig,
    layers: manifest.layers.map((layer: Descriptor) => ({ mediaType: layer.mediaType, digest: layer.digest, size: layer.size })) });
}
