"""OpenMA managed-agents v1 adapter, verified at upstream 85d5361f535c.

Durable event-list reconciliation complements SSE: upstream's strict managed
stream ignores Last-Event-ID, while the native Node transport honors it.
"""
import asyncio
import json
from datetime import timedelta
from decimal import Decimal
import httpx
from httpx_sse import aconnect_sse
from tank.util import iso, utcnow, parse_time, sha256, canonical_json
from .pipeline import ConfigurationError, priority

class BudgetPaused(RuntimeError):
    def __init__(self, until):
        super().__init__('Daily deferrable LLM budget paused')
        self.run_after = until

class SessionService:
    def __init__(self, repo, settings, tool_dispatch, *, http=None):
        self.repo,self.settings,self.tool_dispatch = repo,settings,tool_dispatch
        self.http = http or httpx.AsyncClient(timeout=httpx.Timeout(30,read=None))
        self.owns_http = http is None
        self.tasks = {}
        self.event_locks = {}
        self.closed = False

    def config(self):
        config = self.settings.load_config('openma')
        if not config.get('agent_id') or not config.get('environment_id'):
            raise ConfigurationError('openma.yaml requires deployed agent_id and environment_id')
        return config

    def headers(self, idem=None):
        headers = {'anthropic-beta':'managed-agents-2026-04-01'}
        if self.settings.openma_api_key: headers['x-api-key'] = self.settings.openma_api_key
        if idem: headers['Idempotency-Key'] = idem
        return headers

    def url(self,path):
        return self.settings.openma_url.rstrip('/')+'/v1/'+path

    async def request(self,method,path,**kwargs):
        headers = self.headers(kwargs.pop('idem',None))
        response = await self.http.request(method,self.url(path),headers=headers,**kwargs)
        response.raise_for_status()
        return response.json()

    async def budget(self,item):
        today = utcnow().date()
        rows = await self.repo.list('ops.llm_usage',limit=1000000)
        daily = [r for r in rows if parse_time(r['created_at']).date() == today]
        unknown = any(r.get('cost_usd') is None for r in daily)
        spend = sum((Decimal(str(r['cost_usd'])) for r in daily if r.get('cost_usd') is not None),Decimal(0))
        cap = self.settings.daily_budget_usd
        day = str(today)
        until = iso(utcnow().replace(hour=0,minute=0,second=0,microsecond=0)+timedelta(days=1))
        async with self.repo.lock('budget:'+day):
            state = await self.repo.get('ops.budget_state',day) or {'day':day,'notices':[]}
            state.update(spend_usd=str(spend),unknown_cost=unknown,paused_until=until if spend >= cap or unknown else None)
            async with self.repo.transaction():
                for fraction in (80,100):
                    if spend >= cap*Decimal(fraction)/100 and fraction not in state['notices']:
                        state['notices'].append(fraction)
                        notice_id = f'llm-budget:{day}:{fraction}'
                        await self.repo.enqueue('DELIVER',{'notice_id':notice_id,'kind':'LLM_BUDGET','text':f'LLM budget {fraction}% reached: ${spend}; official and MAJOR+ work continues.','channel':'ops'},notice_id,priority=0)
                await self.repo.put('ops.budget_state',day,state)
        if state['paused_until'] and priority(item) == 2:
            raise BudgetPaused(until)
        return state

    async def resolve(self,payload):
        if payload.get('session_id'):
            session = await self.repo.get('ops.agent_sessions',payload['session_id'])
            if session is None: raise KeyError(payload['session_id'])
            return session
        event_id = payload.get('event_id')
        item_id = payload.get('item_id')
        rows = await self.repo.list('ops.agent_sessions',limit=100000)
        matching = [s for s in rows if (event_id and s.get('event_id') == event_id) or (not event_id and item_id and s.get('item_id') == item_id) or (payload.get('proposal_id') and s.get('proposal_id') == payload['proposal_id'])]
        if matching: return matching[0]
        config = self.config()
        scout = payload.get('kind') == 'SCOUT_REVIEW'
        agent = config.get('scout_agent_id') if scout else config['agent_id']
        if not agent: raise ConfigurationError('scout_agent_id required')
        identity = event_id or item_id or payload.get('proposal_id')
        if not identity: raise ValueError('Session needs event, item or proposal identity')
        # Session creation does not promise idempotency. Reconcile the durable
        # metadata marker before creating, including recovery after a lost reply.
        marker = sha256({'identity':identity,'scout':scout})
        remote = None
        page = None
        while True:
            params = {'limit':100}
            if page: params['page'] = page
            listing = await self.request('GET','sessions',params=params)
            for row in listing.get('data',[]):
                if row.get('metadata',{}).get('tank_identity') == marker: remote = row; break
            page = listing.get('next_page')
            if remote or not page: break
        if remote is None:
            remote = await self.request('POST','sessions',json={'agent':agent,'environment_id':config['environment_id'],'metadata':{'tank_identity':marker}},idem='create:'+marker)
        sid = remote['id']
        session = {'session_id':sid,'event_id':event_id,'item_id':item_id,'proposal_id':payload.get('proposal_id'),'status':'OPEN','last_event_seq':0,'last_item_at':None,'model_id':self.settings.openma_model,'prompt_version':config.get('prompt_version','unversioned'),'config_version':self.settings.config_version,'code_version':self.settings.code_version,'created_at':iso(),'messages':[]}
        await self.repo.put('ops.agent_sessions',sid,session)
        return session

    async def send_message(self,payload):
        item = await self.repo.get('ingest.source_items',payload['item_id']) if payload.get('item_id') else None
        event = await self.repo.get('evt.events',payload['event_id']) if payload.get('event_id') else None
        budget_item = dict(item or {})
        if payload.get('kind') == 'SCOUT_REVIEW': budget_item['deferrable'] = True
        if event: budget_item['event_class'] = event.get('event_class',event.get('class'))
        await self.budget(budget_item)
        async with self.repo.lock('session-mapping'):
            session = await self.resolve(payload)
        sid = session['session_id']
        key = sha256(payload)
        async with self.repo.lock('session:'+sid):
            session = await self.repo.get('ops.agent_sessions',sid)
            if key in session.get('messages',[]):
                self.start_stream(sid)
                return session
            content = {'message':payload,'item':item}
            if payload.get('proposal_id'):
                content['proposal'] = await self.repo.get('scout.expansion_proposals',payload['proposal_id'])
                content['open_proposals'] = await self.repo.list('scout.expansion_proposals',limit=10000)
            event_body = {'type':'user.message','content':[{'type':'text','text':canonical_json(content)}]}
            await self.request('POST',f'sessions/{sid}/events',json={'events':[event_body]},idem='message:'+key)
            session.setdefault('messages',[]).append(key)
            session['last_message'] = event_body
            session['last_payload'] = payload
            session['retry_count'] = 0
            session['status'] = 'OPEN'
            if item: session['last_item_at'] = iso()
            await self.repo.put('ops.agent_sessions',sid,session)
        self.start_stream(sid)
        return session

    def start_stream(self,sid):
        if not self.closed and (sid not in self.tasks or self.tasks[sid].done()):
            self.tasks[sid] = asyncio.create_task(self.follow(sid),name='openma:'+sid)

    async def tool(self,session,event):
        sid, call_id = session['session_id'],event['id']
        key = sid+':'+call_id
        async with self.repo.lock('tool:'+key):
            stored = await self.repo.get('ops.tool_calls',key)
            if stored and stored.get('status') == 'RETURNED': return
            if not stored:
                stored = {'call_id':call_id,'session_id':sid,'event_id':session.get('event_id'),'item_id':session.get('item_id'),'tool':event['name'],'arguments':event['input'],'status':'STARTED','started_at':iso()}
                await self.repo.put('ops.tool_calls',key,stored)
            if stored['status'] != 'DONE':
                context = {k:session.get(k) for k in ('session_id','event_id','item_id','proposal_id')}
                context['call_id'] = call_id
                context['role'] = 'scout' if session.get('proposal_id') else 'analyst'
                if session.get('proposal_id') and stored['tool'] not in ('web_search','search_tank_items','record_review'):
                    raise ValueError('Scout tool is outside reviewer permissions')
                # Tool mutations use their own call-id ledger/transactions, so a
                # crash between dispatch and saving DONE safely replays dispatch.
                result = await self.tool_dispatch(stored['tool'],stored['arguments'],context)
                stored.update(result=result,status='DONE',completed_at=iso())
                await self.repo.put('ops.tool_calls',key,stored)
        body = {'type':'user.custom_tool_result','custom_tool_use_id':call_id,'content':[{'type':'text','text':canonical_json(stored['result'])}]}
        await self.request('POST',f'sessions/{sid}/events',json={'events':[body]},idem='tool:'+sha256(key))
        async with self.repo.lock('tool:'+key):
            stored.update(status='RETURNED',returned_at=iso())
            await self.repo.put('ops.tool_calls',key,stored)

    async def usage(self,session,event):
        usage = event.get('model_usage')
        if not usage: return
        model = event.get('model') or session['model_id']
        prices = self.config().get('model_prices',{}).get(model,{})
        cost = Decimal(0)
        missing = []
        for field,rate in [('input_tokens','input_per_million'),('output_tokens','output_per_million'),('cache_read_input_tokens','cache_read_per_million'),('cache_creation_input_tokens','cache_creation_per_million')]:
            tokens = usage.get(field,0)
            if tokens and rate not in prices: missing.append(rate)
            elif tokens: cost += Decimal(str(tokens))*Decimal(str(prices[rate]))/1000000
        key = session['session_id']+':'+event['id']
        await self.repo.put('ops.llm_usage',key,{'usage_id':key,'session_id':session['session_id'],'event_id':session.get('event_id'),'call':'AGENT','model_id':model,'prompt_version':session['prompt_version'],'usage':usage,**usage,'cost_usd':None if missing else str(cost),'cost_reason':'Missing configured model prices: '+','.join(missing) if missing else None,'created_at':event.get('processed_at') or iso(),'config_version':self.settings.config_version,'code_version':self.settings.code_version},immutable=True)

    async def process_event(self,sid,event,seq=None):
        if event.get('type') in ('event_start','event_delta') or not event.get('id'): return
        key = sid+':'+event['id']
        async with self.event_locks.setdefault(sid,asyncio.Lock()):
            # Full durable payload is kept in the tool ledger; marker is stored
            # in session's log only after every side effect and response succeeds.
            session = await self.repo.get('ops.agent_sessions',sid)
            if event['id'] in session.get('processed_events',[]): return
            kind = event['type']
            if kind == 'agent.custom_tool_use':
                await self.tool(session,event)
                session = await self.repo.get('ops.agent_sessions',sid)
            elif kind == 'span.model_request_end': await self.usage(session,event)
            elif kind in ('session.error','agent.refusal'):
                retry = session.get('retry_count',0)
                if retry < 1 and session.get('last_message'):
                    await self.request('POST',f'sessions/{sid}/events',json={'events':[session['last_message']]},idem='retry:'+key)
                    session['retry_count'] = retry+1
                else:
                    session['status'] = 'FAILED'
                    if session.get('event_id'):
                        record = await self.repo.get('evt.events',session['event_id'])
                        if record:
                            record['status'] = 'FAILED'
                            await self.repo.put('evt.events',record['event_id'],record)
            elif kind == 'session.status_terminated': session['status'] = 'CLOSED'
            session.setdefault('processed_events',[]).append(event['id'])
            if seq is not None: session['last_event_seq'] = seq
            session['last_event_id'] = event['id']
            session['last_event_at'] = event.get('processed_at',iso())
            await self.repo.put('ops.agent_sessions',sid,session)

    async def catch_up(self,sid):
        # Scan from the beginning: provider page tokens may be ephemeral, while
        # dedup ids are durable. Never advance past an unfinished tool.
        page = None
        while True:
            params = {'limit':100,'order':'asc'}
            if page: params['page'] = page
            data = await self.request('GET',f'sessions/{sid}/events',params=params)
            for event in data.get('data',[]): await self.process_event(sid,event)
            page = data.get('next_page')
            if not page: break

    async def stream(self,sid):
        session = await self.repo.get('ops.agent_sessions',sid)
        headers = self.headers()
        headers['Last-Event-ID'] = str(session.get('last_event_seq',0))
        async with aconnect_sse(self.http,'GET',self.url(f'sessions/{sid}/events/stream'),headers=headers) as source:
            source.response.raise_for_status()
            async for message in source.aiter_sse():
                if self.closed: return
                if message.event == 'ping': continue
                data = json.loads(message.data)
                # Native Node streams wrap {seq,event}; strict managed API emits
                # flat durable event bodies. Numeric SSE ids are native cursors.
                event = data.get('event',data) if 'seq' in data else data
                seq = data.get('seq')
                if seq is None and message.id and message.id.isdigit(): seq = int(message.id)
                await self.process_event(sid,event,seq)

    async def follow(self,sid):
        delay = 1
        while not self.closed:
            stream_task = None
            try:
                # Subscribe before list reconciliation to close the history/live
                # gap even when the strict API ignores Last-Event-ID.
                stream_task = asyncio.create_task(self.stream(sid))
                await self.catch_up(sid)
                while not self.closed:
                    done,_ = await asyncio.wait([stream_task],timeout=10)
                    if done:
                        await stream_task
                        break
                    await self.catch_up(sid)
                    session = await self.repo.get('ops.agent_sessions',sid)
                    event = await self.repo.get('evt.events',session['event_id']) if session.get('event_id') else None
                    if event and event.get('status') in ('CLOSED','INVALIDATED','MERGED'):
                        await self.request('POST',f'sessions/{sid}/archive',json={})
                        session['status'] = 'CLOSED'
                        await self.repo.put('ops.agent_sessions',sid,session)
                        return
                delay = 1
            except asyncio.CancelledError: raise
            except (httpx.HTTPError,ValueError,KeyError,RuntimeError):
                # No provider errors, bodies or headers enter logs (may include
                # credentials). Durable state remains for reconciliation.
                delay = min(delay*2,30)
            finally:
                if stream_task and not stream_task.done():
                    stream_task.cancel()
                    await asyncio.gather(stream_task,return_exceptions=True)
            await asyncio.sleep(delay)

    async def scout_review(self,payload):
        proposal_id = payload.get('proposal_id') or payload.get('prp_id')
        if not proposal_id: raise ValueError('SCOUT_REVIEW needs proposal_id')
        proposal = await self.repo.get('scout.expansion_proposals',proposal_id)
        if proposal is None: raise KeyError(proposal_id)
        return await self.send_message({'kind':'SCOUT_REVIEW','proposal_id':proposal_id})

    async def run(self,stop):
        while not stop.is_set() and not self.closed:
            for session in await self.repo.list('ops.agent_sessions',limit=100000):
                if session.get('status') not in ('CLOSED','FAILED'):
                    self.start_stream(session['session_id'])
            try: await asyncio.wait_for(stop.wait(),timeout=5)
            except TimeoutError: pass

    async def close(self):
        self.closed = True
        for task in self.tasks.values(): task.cancel()
        await asyncio.gather(*self.tasks.values(),return_exceptions=True)
        if self.owns_http: await self.http.aclose()
