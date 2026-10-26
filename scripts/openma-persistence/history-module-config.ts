import { isAbsolute, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import upstream from '../../vendor/openma/packages/managed-agents-adapters-sql/vitest.config';
import { WorkspaceExports } from './workspace-exports.mts';

const repository = fileURLToPath(new URL('../../', import.meta.url));
const cache = process.env.TANK_OPENMA_HISTORY_CACHE_DIR;
if (cache === undefined || !isAbsolute(cache)) throw new Error('History module requires its owned cache directory');
const exports = new WorkspaceExports(repository);
interface WorkspacePlugin {
  name: string;
  enforce: 'pre';
  resolveId(source: string): string | null;
}
const plugin: WorkspacePlugin = {
  name: 'tank-history-public-workspace-exports',
  enforce: 'pre',
  resolveId(source: string): string | null { return exports.resolve(source); },
};
export default {
  ...upstream,
  root: resolve(repository, 'vendor/openma/packages/managed-agents-adapters-sql'),
  cacheDir: cache,
  plugins: [plugin],
};
