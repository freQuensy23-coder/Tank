import { Hono } from "hono";
import { forwardHostedInferenceRequest } from "@open-managed-agents/inference-proxy";

export interface NodeInferenceUpstream {
  wireModel: string;
  apiKey: string;
  baseURL?: string;
  provider?: string;
  customHeaders?: Record<string, string>;
}

export interface NodeHttpInferenceProxyDependencies {
  resolveUpstream(input: {
    tenantId: string;
    sessionId: string;
  }): Promise<NodeInferenceUpstream | null>;
  fetcher?: typeof fetch;
}

export function buildNodeHttpInferenceProxyRoutes(
  dependencies: NodeHttpInferenceProxyDependencies,
) {
  const routes = new Hono<{ Variables: { tenant_id: string } }>();
  routes.all("/:sessionId/*", async (context) => {
    const tenantId = context.get("tenant_id");
    const sessionId = context.req.param("sessionId");
    const prefix = `/${sessionId}/`;
    const subPath = context.req.path.startsWith(prefix)
      ? context.req.path.slice(prefix.length)
      : "";
    const upstream = await dependencies.resolveUpstream({ tenantId, sessionId });
    if (upstream === null) {
      return context.json({ error: "forbidden" }, 403);
    }
    const method = context.req.method;
    const body = ["GET", "HEAD"].includes(method.toUpperCase())
      ? null
      : await context.req.raw.arrayBuffer();
    return forwardHostedInferenceRequest({
      upstream,
      method,
      subPath,
      inboundHeaders: context.req.raw.headers,
      body,
      fetcher: dependencies.fetcher,
    });
  });
  return routes;
}
