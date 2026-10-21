import { describe, expect, it, vi } from 'vitest';
import { Hono } from 'hono';
import { buildManagedSessionExecutionRoutes } from '../src/routes/session-executions';
import type { SessionsApplicationPort } from '../src/ports/sessions';

function fixture(found = true) {
  const retrieveSession = vi.fn(async () => ({ type: found ? 'found' : 'not_found' }));
  const list = vi.fn(async (_workspace: string, _session: string, ids: string[]) => ids.map(execution_id => ({ execution_id, state: 'UNKNOWN' as const, settled_outcome: null })));
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (c.req.header('authorization') !== 'Bearer test-only') return c.json({ error: 'unauthorized' }, 401);
    c.set('tenant_id' as never, 'workspace-request' as never);
    await next();
  });
  app.route('/', buildManagedSessionExecutionRoutes({ retrieveSession } as unknown as SessionsApplicationPort, {
    workspaceId: c => c.get('tenant_id'), store: { list },
  }));
  const headers = { authorization: 'Bearer test-only', 'anthropic-beta': 'managed-agents-2026-04-01' };
  return { app, list, retrieveSession, headers };
}
describe('managed session executions HTTP boundary', () => {
  it('requires enclosing authentication and the actual managed beta', async () => {
    const f = fixture();
    expect((await f.app.request('/s/executions?ids[]=e')).status).toBe(401);
    expect((await f.app.request('/s/executions?ids[]=e', { headers: { authorization: 'Bearer test-only' } })).status).toBe(400);
    expect(f.list).not.toHaveBeenCalled();
  });
  it('uses request tenant and checks session access before reading executions', async () => {
    const f = fixture();
    const response = await f.app.request('/s/executions?ids[]=e&ids[]=foreign', { headers: f.headers });
    expect(response.status).toBe(200);
    expect(f.retrieveSession).toHaveBeenCalledWith({ sessionId: 's' });
    expect(f.list).toHaveBeenCalledWith('workspace-request', 's', ['e', 'foreign']);
    expect(await response.json()).toEqual({ data: [
      { execution_id: 'e', state: 'UNKNOWN', settled_outcome: null },
      { execution_id: 'foreign', state: 'UNKNOWN', settled_outcome: null },
    ] });
    const denied = fixture(false);
    expect((await denied.app.request('/s/executions?ids[]=e', { headers: denied.headers })).status).toBe(404);
    expect(denied.list).not.toHaveBeenCalled();
  });
  it('rejects malformed, duplicate, oversized and unknown query fields', async () => {
    const f = fixture();
    for (const query of ['', '?ids=e', '?ids[]=', '?ids[]=e&ids[]=e', '?ids[]=e&owner=x', '?ids[]=' + 'x'.repeat(192), '?' + Array.from({ length: 65 }, (_, i) => 'ids[]=e' + i).join('&')]) {
      expect((await f.app.request('/s/executions' + query, { headers: f.headers })).status).toBe(400);
    }
    expect(f.retrieveSession).not.toHaveBeenCalled();
    expect(f.list).not.toHaveBeenCalled();
  });
  it('refuses malformed, reordered or internal-data-bearing projections', async () => {
    const f = fixture();
    for (const rows of [[], [{ execution_id: 'wrong', state: 'COMPLETED', settled_outcome: 'COMPLETED' }], [{ execution_id: 'e', state: 'COMPLETED', settled_outcome: 'COMPLETED', owner: 'private' }]]) {
      f.list.mockResolvedValueOnce(rows as never);
      expect((await f.app.request('/s/executions?ids[]=e', { headers: f.headers })).status).toBe(500);
    }
  });
});
