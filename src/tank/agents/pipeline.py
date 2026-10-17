"""Version-pinned Jev classification. Cutoffs must come from labeled calibration."""
from datetime import timedelta
from decimal import Decimal
import re
import httpx
from tank.util import iso, utcnow, parse_time, sha256

FILTER = 'Is this text about crypto - crypto assets, crypto exchanges, crypto regulation or enforcement, blockchain networks, or Bitcoin, Ethereum, Uniswap, PancakeSwap, Aerodrome, Curve, 1inch, 0x, CoW Protocol, Hyperliquid, Lighter, Aave, Morpho, Euler, Ondo, LayerZero, Quant, Avalanche, NEAR, Plasma, Ethena, World (Worldcoin)? Whether it reports anything new or important does not matter here. General finance, technology or politics that does not touch crypto does not count.'
ROUTE = 'Does the item report the same real-world event as the event described, or a new development of it - a confirmation, denial, stage change, new detail or new party? Another event about the same project does not count.'

class ConfigurationError(ValueError):
    pass


def priority(item):
    return 2 if item.get('deferrable', False) and item.get('event_class') not in ('MAJOR','STRUCTURAL_SHOCK') else 1

class Pipeline:
    def __init__(self, repo, settings, *, http=None):
        self.repo, self.settings = repo, settings
        self.http = http or httpx.AsyncClient(timeout=15)
        self.owns_http = http is None

    def cutoff(self, name):
        value = self.settings.load_config('thresholds').get(name)
        if value is None or isinstance(value, bool) or not 0 <= float(value) <= 1:
            raise ConfigurationError(f'{name} requires a calibrated probability')
        return float(value)

    @staticmethod
    def state(item):
        return {k: item.get(k) for k in ('source','author','text','quoted')}

    async def score(self, item, name, state):
        if not self.settings.typesafe_api_key:
            raise ConfigurationError('TANK_TYPESAFE_API_KEY is required')
        key = sha256({'item_id':item['item_id'],'question':name,'state':state,'model':'jev-1.13.0'})
        cached = await self.repo.get('ops.llm_usage', key)
        if cached:
            return cached['score']
        response = await self.http.post('https://api.typesafe.ai/v1/systemone', headers={'Authorization':f'Bearer {self.settings.typesafe_api_key}'}, json={'model':'jev-1.13.0','state':state,'questions':{name:{'type':'noul','instructions':FILTER if name == 'relevant' else ROUTE,'criteria':{'true':'About crypto' if name == 'relevant' else 'Same event or a development of it','false':'Not about crypto' if name == 'relevant' else 'A different event'}}}})
        response.raise_for_status()
        data = response.json()
        score = float(data['answers'][name]['noul'])
        if not 0 <= score <= 1 or data.get('model') != 'jev-1.13.0':
            raise ValueError('Jev returned invalid score or unexpected resolved model')
        usage = data.get('usage', {})
        tokens = usage.get('input_tokens')
        cost = str(Decimal(str(tokens)) * Decimal('0.042') / 1000000) if tokens is not None else None
        await self.repo.put('ops.llm_usage',key,{'usage_id':key,'item_id':item['item_id'],'call':'FILTER' if name == 'relevant' else 'ROUTE','model_id':data['model'],'prompt_version':'jev-v1','session_id':None,'score':score,'usage':usage,'input_tokens':tokens,'output_tokens':usage.get('output_tokens'),'cost_usd':cost,'cost_reason':None if cost is not None else 'provider omitted input tokens','created_at':iso(),'config_version':self.settings.config_version,'code_version':self.settings.code_version},immutable=True)
        return score

    async def filter_item(self, payload):
        cutoff = self.cutoff('filter_cutoff')
        async with self.repo.lock('filter:'+payload['item_id']):
            item = await self.repo.get('ingest.source_items',payload['item_id'])
            if item is None: raise KeyError(payload['item_id'])
            score = await self.score(item,'relevant',self.state(item))
            item.update(filter_score=score,status='RELEVANT' if score >= cutoff else 'SKIPPED')
            async with self.repo.transaction():
                await self.repo.put('ingest.source_items',item['item_id'],item)
                if score >= cutoff:
                    await self.repo.enqueue('ROUTE_ITEM',{'item_id':item['item_id']},item['item_id'],priority=priority(item))
            return item

    def identities(self, item):
        registry = self.settings.load_config('registry')
        text = ' '.join(str(item.get(k) or '') for k in ('text','quoted','author_organization','author'))
        result = set(item.get('entities',[])) | set(item.get('tokens',[]))
        for row in registry.get('entities',[]) + registry.get('tokens',[]):
            names = [row.get('name'),row.get('symbol'),*row.get('aliases',[])]
            if any(name and re.search(r'(?<!\w)'+re.escape(name)+r'(?!\w)',text,re.I) for name in names):
                result.add(row.get('entity_id') or row.get('token_id'))
        author = str(item.get('author','')).lstrip('@').lower()
        for account in registry.get('x_accounts',[]):
            if str(account['handle']).lstrip('@').lower() == author:
                result.add(account.get('entity_id'))
        return result - {None}

    async def route_item(self,payload):
        cutoff = self.cutoff('routing_cutoff')
        async with self.repo.lock('routing'):
            item = await self.repo.get('ingest.source_items',payload['item_id'])
            if item is None: raise KeyError(payload['item_id'])
            if item.get('routing_message'): return item
            identities = self.identities(item)
            sessions = await self.repo.list('ops.agent_sessions',limit=100000)
            recent = {s.get('event_id') for s in sessions if s.get('last_item_at') and parse_time(s['last_item_at']) >= utcnow()-timedelta(days=3)}
            best, best_score = None, -1
            scores = {}
            for event in await self.repo.list('evt.events',limit=100000):
                if event['event_id'] not in recent or event.get('status') in ('CLOSED','MERGED','INVALIDATED'): continue
                if not identities.intersection(set(event.get('entities',[]))|set(event.get('tokens',[]))): continue
                state = self.state(item)
                state['event'] = {k:event.get(k) for k in ('statement','entities','tokens','stage','known_to_us_at')}
                score = await self.score(item,'same_event',state)
                scores[event['event_id']] = score
                if score >= cutoff and score > best_score: best,best_score = event,score
            message = {'kind':'ITEM_ADDED' if best else 'NEW_ITEM','item_id':item['item_id']}
            if best: message['event_id'] = best['event_id']
            item.update(routing_message=message,routing_scores=scores,route_event_id=best['event_id'] if best else None,status='ROUTED')
            async with self.repo.transaction():
                await self.repo.put('ingest.source_items',item['item_id'],item)
                await self.repo.enqueue('AGENT_MESSAGE',message,item['item_id']+':'+message['kind'],priority=priority(item))
            return item

    async def close(self):
        if self.owns_http: await self.http.aclose()
