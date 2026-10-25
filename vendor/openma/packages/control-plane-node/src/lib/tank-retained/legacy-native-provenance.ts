import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { stableDigest } from './runtime-identity.js';
import { trustedLegacyBuild } from './legacy-build-provenance.js';
import type { Allocation } from './boxlite-provider.js';

export type NativeLegacyConfiguration = { id: string; name: string; options: Record<string, any> };
export async function readNativeLegacyConfiguration(runtimeId: string): Promise<NativeLegacyConfiguration | null> {
  try {
    const { DatabaseSync } = await import('node:sqlite');
    const home = process.env.BOXLITE_HOME || join(homedir(), '.boxlite');
    const database = new DatabaseSync(join(home, 'db', 'boxlite.db'), { readOnly: true });
    try {
      const row = database.prepare('SELECT id, name, json FROM box_config WHERE id = ?').get(runtimeId) as { id: string; name: string; json: string } | undefined;
      if (!row) return null;
      const configuration = JSON.parse(row.json);
      if (configuration.id !== row.id || configuration.name !== row.name) throw Error('retained_legacy_native_record_identity_mismatch');
      return { id: row.id, name: row.name, options: configuration.options };
    } finally { database.close(); }
  } catch { throw Error('retained_legacy_provider_provenance_missing:native_configuration'); }
}
export async function validateLegacyNativeArtifact(): Promise<void> {
  for (const [name, expected] of [['boxlite-shim', trustedLegacyBuild.native.shim], ['provenance.json', trustedLegacyBuild.native.provenance]]) {
    let bytes: Buffer;
    try { bytes = await readFile(join('/opt/tank-boxlite-runtime', name!)); }
    catch { throw Error('retained_legacy_provider_provenance_missing:native_artifact'); }
    if (createHash('sha256').update(bytes).digest('hex') !== expected) throw Error('retained_legacy_provider_provenance_missing:native_artifact');
  }
}
export function assertLegacyNativeConfiguration(record: NativeLegacyConfiguration | null, runtimeId: string, name: string, allocation: Allocation): void {
  const fail = (component: string): never => { throw Error('retained_legacy_provider_provenance_missing:' + component); };
  if (!record || record.id !== runtimeId || record.name !== name || !record.options) fail('runtime_ownership');
  const options = record!.options;
  const equals = (actual: unknown, expected: unknown, component: string) => {
    if (stableDigest(actual) !== stableDigest(expected)) fail(component);
  };
  equals(options.cpus, allocation.cpus, 'cpus');
  equals(options.memory_mib, allocation.memoryMib, 'memory');
  if (!allocation.rootfsPath) fail('rootfs');
  equals(options.rootfs, { RootfsPath: allocation.rootfsPath }, 'rootfs');
  equals(options.volumes, (allocation.volumes ?? []).map(volume => ({ host_path: volume.hostPath, guest_path: volume.guestPath, read_only: volume.readOnly ?? false })), 'mounts');
  equals(options.network, allocation.network?.mode === 'disabled' ? 'Disabled' : { Enabled: { allow_net: allocation.network?.allowNet ?? [] } }, 'network');
  equals(options.env, [], 'environment');
  equals(options.secrets, [], 'secrets');
  equals(options.ports, [], 'ports');
  if ('mounts' in options) equals(options.mounts, [], 'mounts');
  if ('inbound_network' in options) equals(options.inbound_network, 'Disabled', 'network');
  equals(options.auto_remove, false, 'retention');
  equals(options.detach, true, 'retention');
  if (options.auto_delete != null) equals(options.auto_delete, 0, 'retention');
  equals(options.working_dir, '/workspace', 'working_directory');
  for (const field of ['entrypoint', 'cmd', 'user']) {
    if (options[field] != null) fail('process_configuration');
  }
  equals(options.advanced, {
    security: {
      jailer_enabled: allocation.security?.jailerEnabled ?? true, seccomp_enabled: allocation.security?.seccompEnabled ?? true,
      uid: 65534, gid: 65534, new_pid_ns: true, new_net_ns: false, chroot_base: '/srv/boxlite',
      chroot_enabled: true, close_fds: true, sanitize_env: true, env_allowlist: ['RUST_LOG'],
      resource_limits: { max_open_files: null, max_file_size: 21474836480, max_processes: null, max_memory: null, max_cpu_time: null },
      sandbox_profile: null, network_enabled: true,
    }, isolate_mounts: false, health_check: null, kernel: null, nested_virtualization: false, privileged: false,
  }, 'security');
}
