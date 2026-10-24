import { existsSync } from "node:fs";
import { registerHooks, type ResolveHookContext, type ResolveHookSync } from "node:module";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const configured = process.env.TANK_AUDIT_OPENMA_ROOT;
const source = configured === undefined
  ? new URL("../../vendor/openma/", import.meta.url)
  : pathToFileURL(resolve(configured) + "/");

type NextResolve = Parameters<ResolveHookSync>[2];

function resolveOpenma(specifier: string, context: ResolveHookContext, nextResolve: NextResolve): ReturnType<ResolveHookSync> {
  if (
    context.parentURL !== undefined &&
    context.parentURL.startsWith(source.href) &&
    context.parentURL.endsWith(".ts") &&
    (specifier.startsWith("./") || specifier.startsWith("../")) &&
    specifier.endsWith(".js")
  ) {
    const typed = new URL(specifier.slice(0, -3) + ".ts", context.parentURL);
    if (typed.href.startsWith(source.href) && existsSync(fileURLToPath(typed))) return nextResolve(typed.href, context);
  }
  return nextResolve(specifier, context);
}

registerHooks({ resolve: resolveOpenma });
