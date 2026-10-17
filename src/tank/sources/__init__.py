"""Durable raw-first polling for the 34 logical information sources."""
from __future__ import annotations
import asyncio
import gzip
import json
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from pathlib import Path
import httpx
from tank.util import iso, utcnow, parse_time, sha256, new_id, canonical_json
from . import parsers
from .browser import BrowserReader

class SourceService:
    def __init__(self,repo,settings,*,http=None):
        self.repo=repo; self.settings=settings
        self.http=http or httpx.AsyncClient(timeout=20,follow_redirects=True,headers={'User-Agent':settings.user_agent})
        self.owns_http=http is None; self.browser=BrowserReader(settings)
        cfg=settings.load_config('sources'); self.sources={s['source_id']:s for s in cfg['sources']}
        self.accounts={a['handle'].lower().lstrip('@'):a for a in settings.load_config('registry').get('x_accounts',[])}
        self.locks={}; self.requests={}

    def raw(self,payload):
        digest=sha256(payload); path=Path(self.settings.data_dir)/'raw'/'sources'/digest[:2]/(digest+'.gz')
        path.parent.mkdir(parents=True,exist_ok=True)
        if not path.exists():
            temporary=path.with_suffix('.tmp'); temporary.write_bytes(gzip.compress(payload,mtime=0)); temporary.replace(path)
        return str(path),digest

    async def fetch(self,feed,key):
        state=await self.repo.get('ops.source_checkpoints',key) or {}
        headers={}
        if state.get('etag'): headers['If-None-Match']=state['etag']
        if state.get('last_modified'): headers['If-Modified-Since']=state['last_modified']
        budget=feed.get('requests_per_minute',30)
        now=utcnow().timestamp(); history=[t for t in self.requests.get(key,[]) if now-t<60]
        if len(history)>=budget: raise RuntimeError('Source local request budget exhausted')
        error=None
        for attempt in range(4):
            try:
                history[:]=[t for t in history if utcnow().timestamp()-t<60]
                if len(history)>=budget: raise RuntimeError('Source local request budget exhausted')
                history.append(utcnow().timestamp()); self.requests[key]=history
                response=await self.http.request(feed.get('method','GET'),feed['url'],headers=headers,json=feed.get('body') if feed.get('method','GET').upper()=='POST' else None)
                if response.status_code==304: return None,state
                response.raise_for_status()
                payload=response.content; ref,digest=self.raw(payload)
                state.update(etag=response.headers.get('etag'),last_modified=response.headers.get('last-modified'),raw_ref=ref,raw_sha256=digest)
                return payload,state
            except (httpx.HTTPError,TimeoutError) as exc:
                error=exc
                if attempt<3:
                    delay=2**(attempt+1)
                    if isinstance(exc,httpx.HTTPStatusError):
                        retry=exc.response.headers.get('retry-after')
                        if retry:
                            try: delay=max(delay,float(retry))
                            except ValueError:
                                try: delay=max(delay,(parse_time(parsers.published(retry))-utcnow()).total_seconds())
                                except (ValueError,TypeError): pass
                    await asyncio.sleep(delay)
        raise error

    async def ingest(self,source,item,raw_ref,raw_hash):
        sid=source['source_id']; body=' '.join((item.get('title','')+' '+item.get('text','')+' '+(item.get('quoted') or '')).split())
        content_hash=sha256(body); external=str(item['external_id'])
        async with self.repo.lock('ingest-dedup'):
            async with self.repo.transaction():
                dedup=await self.repo.get('ops.source_dedup',sha256(sid+'|'+external)) or await self.repo.get('ops.source_dedup','content:'+content_hash)
                if dedup:
                    old=await self.repo.get('ingest.source_items',dedup['item_id'])
                    old['observation_count']=old.get('observation_count',1)+1; old['last_observed_at']=iso()
                    await self.repo.put('ingest.source_items',old['item_id'],old)
                    return old
                item_id=new_id('item'); role=self.accounts.get(item.get('author','').lower(),{}).get('role')
                filtered=sid in {'S30','S31','S32','S33','S34'} or (sid=='S25' and item.get('subtype')=='commits') or (sid=='S29' and role!='ORGANIZATION')
                priority=2 if sid in {'S30','S31','S32','S33','S34'} or (sid=='S25' and item.get('subtype')=='commits') or (sid=='S29' and role=='OTHER') else 1
                record=dict(item,item_id=item_id,source_id=sid,external_id=external,raw_ref=raw_ref,raw_sha256=raw_hash,content_hash=content_hash,observed_at=iso(),published_at_precision='SECOND' if item.get('published_at') else None,source_class=source.get('source_class',source.get('class','TECHNICAL_EVIDENCE' if sid=='S25' else 'SOCIAL_LEAD' if sid=='S29' else 'SECONDARY' if int(sid[1:])>=30 else 'PRIMARY')),relevance={'decision':'PENDING' if filtered else 'ALWAYS'},extraction_status='PENDING',observation_count=1,config_version=self.settings.config_version,code_version=self.settings.code_version)
                await self.repo.put('ingest.source_items',item_id,record)
                index={'item_id':item_id}
                await self.repo.put('ops.source_dedup',sha256(sid+'|'+external),index)
                await self.repo.put('ops.source_dedup','content:'+content_hash,index)
                await self.repo.enqueue('FILTER_ITEM' if filtered else 'ROUTE_ITEM',{'item_id':item_id},item_id,priority=priority)
                return record

    async def poll_once(self,source_id,*,due_only=False,escalated=False):
        source=self.sources[source_id]; lock=self.locks.setdefault(source_id,asyncio.Lock())
        async with lock:
            count=0; failures=[]
            for idx,original in enumerate(source.get('feeds',[])):
                feed=dict(original); key=f'{source_id}:{idx}'; state=await self.repo.get('ops.source_checkpoints',key) or {}
                if due_only and state.get('last_success_at') and (utcnow()-parse_time(state['last_success_at'])).total_seconds()<(15 if escalated else feed.get('interval_s',source.get('interval_s',60))): continue
                kind=str(feed.get('kind',feed.get('format',source.get('mode','rss')))).lower()
                try:
                    if source_id=='S29':
                        rows,_=await self.browser.posts(self.settings.x_list_id or source.get('list_id'),state.get('status_id'))
                        payload=canonical_json(rows).encode(); ref,digest=self.raw(payload); state.update(raw_ref=ref,raw_sha256=digest)
                    else:
                        if source_id=='S19': payload=await self.browser.html(feed['url']); ref,digest=self.raw(payload); state.update(raw_ref=ref,raw_sha256=digest)
                        else: payload,state=await self.fetch(feed,key)
                        if payload is None:
                            state.update(last_success_at=iso(),status='HEALTHY',error=None)
                            await self.repo.put('ops.source_checkpoints',key,state)
                            continue
                        if source_id=='S14':
                            snapshot=parsers.instruments(payload,feed)
                            if str(feed.get('venue','')).lower()=='bybit':
                                page_data=json.loads(payload); cursor=page_data.get('result',{}).get('nextPageCursor'); cursors=set()
                                while cursor:
                                    if cursor in cursors: raise ValueError('Repeated Bybit instrument cursor')
                                    cursors.add(cursor); parts=urlsplit(feed['url']); params=dict(parse_qsl(parts.query)); params['cursor']=cursor
                                    next_feed=dict(feed,url=urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(params),parts.fragment)))
                                    next_payload,next_state=await self.fetch(next_feed,key+':cursor:'+sha256(cursor))
                                    if next_payload is None: raise ValueError('Unexpected conditional paginated response')
                                    snapshot.update(parsers.instruments(next_payload,feed)); page_data=json.loads(next_payload); cursor=page_data.get('result',{}).get('nextPageCursor')
                            previous=state.get('instruments'); rows=[]
                            if previous is not None:
                                for symbol in sorted(set(snapshot)|set(previous)):
                                    if snapshot.get(symbol)!=previous.get(symbol):
                                        current=snapshot.get(symbol); old=previous.get(symbol)
                                        rows.append(dict(external_id=f"{feed.get('venue')}|{feed.get('market_type')}|{symbol}|{sha256(current)}",title=f'{symbol} instrument change',text=canonical_json({'before':old,'after':current}),url=feed['url'],published_at=None,change='REMOVED' if current is None else 'ADDED' if old is None else 'STATE_UPDATE'))
                            state['instruments']=snapshot
                        elif kind in {'rss','atom','feed'}: rows=parsers.feeds(payload,source_id,feed)
                        elif kind in {'html','rendered','rendered_html','html_diff'} or source_id=='S19': rows=parsers.article_links(payload,feed)
                        else:
                            rows=parsers.json_items(payload,source_id)
                            if source_id=='S24':
                                pending=state.get('pending_proposals',[])
                                if pending:
                                    query='query { proposals(first: 1000, where: {id_in: '+json.dumps(pending)+'}) { id title body state start end created scores scores_total space { id } } }'
                                    extra,extra_state=await self.fetch(dict(feed,body={'query':query}),key+':pending')
                                    if extra:
                                        existing={r['external_id'] for r in rows}
                                        for r in parsers.json_items(extra,source_id):
                                            if r['external_id'] not in existing:
                                                r['proposal_raw_ref']=extra_state['raw_ref']; r['proposal_raw_sha256']=extra_state['raw_sha256']; rows.append(r)
                                state['pending_proposals']=list({r['proposal_id'] for r in rows if r['state']!='closed'})
                    for item in rows:
                        if source_id=='S02' and not self.filing_matches(item): continue
                        if kind in {'html','rendered','rendered_html','html_diff'} or source_id in {'S01','S19'}:
                            seen=await self.repo.get('ops.source_dedup',sha256(source_id+'|'+str(item['external_id'])))
                            if not seen:
                                article,state_article=await self.fetch({'url':item['url']},key+':'+sha256(item['url']))
                                if article:
                                    detail=parsers.article(article); item.update({k:v for k,v in detail.items() if v}); item['discovery_raw_ref']=state['raw_ref']; item['discovery_raw_sha256']=state['raw_sha256']
                                    item['article_raw_ref']=state_article['raw_ref']; item['article_raw_sha256']=state_article['raw_sha256']
                        if source_id=='S09':
                            detail,dstate=await self.fetch({'url':'https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode='+item['external_id']},key+':detail:'+item['external_id'])
                            if detail:
                                obj=json.loads(detail)['data']; body=obj.get('body',''); item['text']=parsers.binance_body(body); item['detail_raw_ref']=dstate['raw_ref']; item['detail_raw_sha256']=dstate['raw_sha256']
                        await self.ingest(source,item,item.get('article_raw_ref') or item.get('detail_raw_ref') or item.get('proposal_raw_ref') or state['raw_ref'],item.get('article_raw_sha256') or item.get('detail_raw_sha256') or item.get('proposal_raw_sha256') or state['raw_sha256']); count+=1
                    if source_id=='S29' and rows: state['status_id']=rows[0]['external_id']
                    state.update(last_success_at=iso(),status='HEALTHY',error=None)
                    await self.repo.put('ops.source_checkpoints',key,state)
                except Exception as exc:
                    failures.append(str(exc)); state.update(status='DEGRADED',error=str(exc),last_failure_at=iso())
                    await self.repo.put('ops.source_checkpoints',key,state)
            return {'source_id':source_id,'items':count,'status':'DEGRADED' if failures else 'HEALTHY','errors':failures}

    def filing_matches(self,item):
        value=(item.get('title','')+' '+item.get('text','')).lower()
        registry=self.settings.load_config('registry'); names=[t.get('name',t.get('symbol','')) for t in registry.get('tokens',[])]+[e.get('name','') for e in registry.get('entities',[])]
        return any(n and n.lower() in value for n in names) or (any(t in value for t in ('trust','etf')) and any(t in value for t in ('crypto','bitcoin','ether','blockchain','digital asset')))

    async def check(self):
        results=[]
        for sid,source in self.sources.items():
            states=[await self.repo.get('ops.source_checkpoints',f'{sid}:{i}') for i,_ in enumerate(source.get('feeds',[]))]
            results.append({'source_id':sid,'configured':bool(source.get('feeds')),'states':states,'status':'DEGRADED' if any(s and s.get('status')=='DEGRADED' for s in states) else 'HEALTHY' if states and all(states) else 'UNVERIFIED'})
        for handle,account in self.accounts.items():
            try:
                response=await self.http.get('https://publish.twitter.com/oembed',params={'url':f'https://x.com/{handle}'})
                results.append({'handle':handle,'exists':response.status_code==200,'identity_status':'REQUIRES_OWNER_EVIDENCE','role':account.get('role')})
            except httpx.HTTPError as exc: results.append({'handle':handle,'exists':None,'error':str(exc)})
        return results

    async def run(self,stop):
        async def loop(source):
            while not stop.is_set():
                escalations=await self.repo.list('ops.source_escalations',filters={'source_id':source['source_id']},limit=1000)
                escalated=any(not e.get('expires_at') or parse_time(e['expires_at'])>utcnow() for e in escalations)
                await self.poll_once(source['source_id'],due_only=True,escalated=escalated)
                interval=15 if escalated else min([f.get('interval_s',source.get('interval_s',60)) for f in source.get('feeds',[])] or [60])
                try: await asyncio.wait_for(stop.wait(),timeout=interval)
                except TimeoutError: pass
        try: await asyncio.gather(*(loop(s) for s in self.sources.values()))
        finally:
            await self.browser.close()
            if self.owns_http: await self.http.aclose()
