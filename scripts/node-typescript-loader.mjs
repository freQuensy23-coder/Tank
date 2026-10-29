import { existsSync } from 'node:fs';
import { registerHooks } from 'node:module';
import { extname } from 'node:path';
import { fileURLToPath } from 'node:url';

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL && specifier.startsWith('.')) {
      const candidates = specifier.endsWith('.js')
        ? [specifier.slice(0, -3) + '.ts']
        : extname(specifier) === '' ? [specifier + '.ts', specifier + '/index.ts'] : [];
      for (const candidate of candidates) {
        const source = new URL(candidate, context.parentURL);
        if (source.protocol === 'file:' && existsSync(fileURLToPath(source))) return nextResolve(source.href, context);
      }
    }
    return nextResolve(specifier, context);
  },
});
