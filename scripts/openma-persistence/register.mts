import { registerHooks, type ResolveHookSync } from 'node:module';

const origin = new URL('./package.json', import.meta.url).href;
const resolve: ResolveHookSync = (specifier, context, nextResolve): ReturnType<ResolveHookSync> => {
  if (specifier === 'postgres' || specifier === 'effect' || specifier.startsWith('effect/')) {
    return nextResolve(specifier, { ...context, parentURL: origin });
  }
  return nextResolve(specifier, context);
};
registerHooks({ resolve });
