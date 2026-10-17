"""Validated domain tools exposed to OpenMA. External material is evidence, never instructions."""
from __future__ import annotations
import copy
import json
from decimal import Decimal
import re
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse
import httpx
from tank.util import iso, parse_time, utcnow, new_id, sha256, canonical_json

STATES = ['RUMOR','PROPOSED','APPROVED','SIGNED','DEPLOYED','LIVE','MONEY_FLOWING']
INTEGRATION = ['COMPATIBLE','IN_DEVELOPMENT','BUILT','SUBMITTED','APPROVED','LIVE','MATERIAL_USAGE','ECONOMIC_FLOW']
LAYERS = ['ISSUER_PRODUCT','VENUE_DEX','SETTLEMENT_L1_L2','LIQUIDITY_EXECUTION','LENDING_COLLATERAL','ORACLE_DATA','INTEROPERABILITY','CUSTODY_COMPLIANCE']
IMPLEMENTATION = dict(zip(STATES,[0,4,6,6,7,8,10]))
def normalized(s): return ' '.join(str(s).split())
def freshness(age):
    for hours, grade in [(1,'FRESH'),(4,'NORMAL'),(8,'MEDIUM'),(24,'POOR'),(48,'OLD')]:
        if age <= hours * 3600: return grade
    return 'STALE'

class EventToolService:
    TOOLS = {'get_item','search_tank_items','list_open_events','get_event','get_registry','get_token_facts','web_search','fetch_page','record_event','add_observation','merge_into','set_assessment','set_beneficiaries','get_market_truth','get_decision','publish_card','schedule_milestone','propose_pattern'}
    def __init__(self, repo, settings, market_truth, decision_engine, *, http=None):
        self.repo,self.settings,self.truth,self.engine=repo,settings,market_truth,decision_engine
        self.http=http
    def provenance(self,ctx):
        return {k:v for k,v in dict(config_version=self.settings.config_version,code_version=self.settings.code_version,model_id=ctx.get('model_id',self.settings.openma_model),prompt_version=ctx.get('prompt_version','tank-event-analyst-v1'),session_id=ctx.get('session_id')).items()}
    async def dispatch(self, tool, arguments, context):
        if tool not in self.TOOLS: raise ValueError(f'Unknown Tank tool: {tool}')
        if not isinstance(arguments,dict): raise ValueError('Tool arguments must be an object')
        schema_path=Path(self.settings.config_dir)/'skills'/'tank-event-analyst'/f'{tool}.schema.json'
        if schema_path.exists(): validate_schema(arguments,json.loads(schema_path.read_text()))
        ctx=dict(context or {})
        event_id=arguments.get('event_id') or ctx.get('event_id')
        if tool in {'record_event','add_observation','merge_into','set_assessment','set_beneficiaries','publish_card','schedule_milestone','get_decision'}:
            async with self.repo.transaction():
                async with self.repo.lock('event:'+str(event_id or ctx.get('session_id') or arguments.get('item_id'))):
                    return await getattr(self,tool)(arguments,ctx)
        return await getattr(self,tool)(arguments,ctx)
    async def rows(self,table,event_id=None):
        return await self.repo.list(table,filters={'event_id':event_id} if event_id else None,limit=100000)
    async def require(self,table,key):
        row=await self.repo.get(table,key)
        if row is None: raise ValueError(f'Missing {table}: {key}')
        return row
    async def get_item(self,a,c): return await self.require('ingest.source_items',a['item_id'])
    async def search_tank_items(self,a,c):
        days=a.get('days',90)
        if not isinstance(days,(int,float)) or not 0 < days <= 90: raise ValueError('days must be >0 and <=90')
        cutoff=utcnow()-timedelta(days=days); sources=a.get('sources',[]); terms=a['query'].casefold().split()
        rows=await self.rows('ingest.source_items')
        return {'items':sorted([r for r in rows if parse_time(r['observed_at'])>=cutoff and (not sources or r['source_id'] in sources) and all(t in (r.get('title','')+' '+r.get('text','')).casefold() for t in terms)],key=lambda r:r['observed_at'],reverse=True)[:20]}
    async def list_open_events(self,a,c):
        q=a.get('entity_or_token','').casefold()
        return {'events':[r for r in await self.rows('evt.events') if r['lifecycle'] not in {'CLOSED','MERGED','INVALIDATED'} and (not q or q in canonical_json(r).casefold())]}
    async def get_event(self,a,c):
        eid=a['event_id']; event=await self.require('evt.events',eid)
        return {'event':event,**{table.split('.')[-1]:await self.rows(table,eid) for table in ['evt.event_observations','evt.claims','evt.state_changes','evt.ems_assessments','evt.beneficiary_maps','evt.beneficiary_edges']}}
    async def get_registry(self,a,c):
        q=a.get('entity_or_token','').casefold()
        return {name:[r for r in self.settings.load_config('registry').get(name,[]) if not q or q in canonical_json(r).casefold()] for name in ['entities','tokens','instruments','x_accounts']} | {'dependency_map':self.settings.load_config('dependency_map')}
    async def get_token_facts(self,a,c):
        token=a['token']; registry=await self.get_registry({'entity_or_token':token},c)
        return {'registry':registry,**{t.split('.')[-1]:await self.repo.list(t,filters={'token_id':token},limit=10000) for t in ['mkt.supply_obs','mkt.unlocks','evt.capture_chains']}}
    async def save_web(self,url,text,title,c,source_class='SECONDARY'):
        digest=sha256(text); iid='web-'+sha256({'url':url,'text':text,'session':c.get('session_id')})[:32]
        raw=Path(self.settings.data_dir)/'raw'/'web'/f'{digest}.txt'; raw.parent.mkdir(parents=True,exist_ok=True); raw.write_text(text)
        row=dict(item_id=iid,source_id='WEB',external_id=url,raw_ref=str(raw),raw_sha256=digest,content_hash=sha256(normalized(text)),url=url,title=title,text=text,observed_at=iso(),published_at=None,source_class=source_class,extraction_status='PENDING',**self.provenance(c))
        existing=await self.repo.get('ingest.source_items',iid)
        if existing: return existing
        return await self.repo.put('ingest.source_items',iid,row)
    async def web_search(self,a,c):
        if not self.settings.tavily_api_key: raise ValueError('TAVILY_API_KEY is required')
        async with httpx.AsyncClient(timeout=20) if self.http is None else _borrow(self.http) as client:
            r=await client.post('https://api.tavily.com/search',json={'api_key':self.settings.tavily_api_key,'query':a['query'],'search_depth':'basic','max_results':10}); r.raise_for_status()
            results=r.json().get('results',[])
        return {'items':[await self.save_web(x['url'],x.get('content',''),x.get('title',''),c) for x in results]}
    async def fetch_page(self,a,c):
        if urlparse(a['url']).scheme!='https': raise ValueError('Only HTTPS evidence fetches are allowed')
        async with httpx.AsyncClient(timeout=20,follow_redirects=False) if self.http is None else _borrow(self.http) as client:
            async with client.stream('GET',a['url'],headers={'User-Agent':self.settings.user_agent}) as r:
                r.raise_for_status(); chunks=[]; size=0
                async for chunk in r.aiter_bytes():
                    size+=len(chunk)
                    if size>2*1024*1024: raise ValueError('Page exceeds 2 MB')
                    chunks.append(chunk)
        from selectolax.parser import HTMLParser
        html=b''.join(chunks).decode('utf-8',errors='replace'); tree=HTMLParser(html)
        for node in tree.css('script,style,noscript'): node.decompose()
        return await self.save_web(a['url'],tree.text(separator=' ',strip=True),'Fetched page',c)
    def validate_time(self,x,item):
        public=x['public_at']; value=public['value'] if isinstance(public,dict) else public
        age=(parse_time(item['observed_at'])-parse_time(value)).total_seconds()
        if age<0: raise ValueError('V7 public_at is later than observed_at')
        if freshness(age)!=x['freshness']: raise ValueError('V7 freshness does not match public_at age')
        quote=public.get('quote') if isinstance(public,dict) else None
        if quote and normalized(quote) not in normalized(item['text']): raise ValueError('V7 public_at quote is not verbatim')
        return iso(value)
    async def validate_claims(self,claims,item,c,certainty,obs_id,eid):
        evidence_items=[item]+[r for r in await self.rows('ingest.source_items') if r.get('session_id')==c.get('session_id') and r.get('source_id')=='WEB' and c.get('session_id')]
        accepted=[]; dropped=[]
        for raw in claims:
            claim=copy.deepcopy(raw); quote=claim.pop('quote',None)
            evidence=claim.get('evidence') or ([{'item_id':item['item_id'],'quote':quote}] if quote else [])
            resolved=[]
            for e in evidence:
                matches=[r for r in evidence_items if (r['item_id']==e.get('item_id') or quote) and e.get('quote') and normalized(e['quote']) in normalized(r['text'])]
                if matches: resolved.append({'item_id':matches[0]['item_id'],'quote':e['quote']})
            if not evidence or len(resolved)!=len(evidence): dropped.append({'claim':raw,'reason':'V1 quote not found in permitted evidence'}); continue
            if claim.get('type') not in {'FACT','STATE','NUMBER','RELATION','TIMESTAMP','MECHANISM','MILESTONE'}: raise ValueError('Invalid claim type')
            verified=certainty=='HIGH' and all(next(r for r in evidence_items if r['item_id']==e['item_id']).get('source_class') in {'PRIMARY','TECHNICAL_EVIDENCE','MARKET_DATA'} for e in resolved)
            text=' '.join(e['quote'] for e in resolved).casefold(); state=claim.get('state_claim')
            if state in {'LIVE','MONEY_FLOWING','MATERIAL_USAGE','ECONOMIC_FLOW'} and not any(w in text for w in ['now live','available to','launched','trading opens','fees accrued','buyback executed']):
                state='BUILT' if state in INTEGRATION and claim.get('directionality')=='SOURCE_SIDE_SUPPORT' else 'DEPLOYED'
            if any(w in text for w in ['compatible','sdk','repository','github']):
                claim['directionality']='SOURCE_SIDE_SUPPORT'
                if state in INTEGRATION and INTEGRATION.index(state)>2: state='BUILT'
            claim.update(claim_id=new_id('clm'),obs_id=obs_id,event_id=eid,state_claim=state,evidence=resolved,verified=verified,accepted=True,**self.provenance(c))
            await self.repo.put('evt.claims',claim['claim_id'],claim); accepted.append(claim)
        return accepted,dropped
    async def record_event(self,a,c):
        x=a.get('extraction',a); required={'item_id','is_event','statement','certainty','certainty_reason','entities','tokens','event_time','public_at','freshness','claims','next_milestone','checked_sources'}
        if required-set(x): raise ValueError('Missing extraction fields: '+','.join(sorted(required-set(x))))
        item=await self.require('ingest.source_items',x['item_id']); public=self.validate_time(x,item)
        if x['certainty'] not in {'LOW','MEDIUM','HIGH'} or not x['certainty_reason']: raise ValueError('certainty and reason required')
        if not x['is_event']:
            item['extraction_status']='SKIPPED'; await self.repo.put('ingest.source_items',item['item_id'],item); return {'is_event':False}
        eid=new_id('evt'); obs=new_id('obs'); entities=[]; registry=self.settings.load_config('registry').get('entities',[])
        for e in x['entities']:
            found=next((r for r in registry if e.get('entity_id')==r.get('entity_id') or e['name'].casefold() in [str(v).casefold() for v in [r.get('name',''),*r.get('aliases',[])]]),None)
            entities.append({'entity_id':found['entity_id'] if found else 'UNRESOLVED:'+e['name'],'role':e['role']})
        claims,dropped=await self.validate_claims(x['claims'],item,c,x['certainty'],obs,eid)
        event_time=copy.deepcopy(x['event_time'])
        if event_time.get('value') and (not event_time.get('quote') or normalized(event_time['quote']) not in normalized(item['text'])): event_time['precision']='BUCKET_1D'
        event=dict(event_id=eid,statement=x['statement'],entities=entities,tokens_mentioned=x['tokens'],theme_tags=x.get('theme_tags',[]),event_time=event_time,public_at=public,public_at_precision=x['public_at'].get('precision'),known_to_us_at=item['observed_at'],freshness=x['freshness'],event_state='RUMOR',state_verified=False,certainty=x['certainty'],certainty_reason=x['certainty_reason'],timing_confidence='HIGH' if x['public_at']['precision']=='EXACT' and item['source_class']=='PRIMARY' else 'MEDIUM',best_source_class=item['source_class'],next_milestone=x['next_milestone'],lifecycle='DETECTED',**self.provenance(c))
        await self.apply_states(event,claims,c)
        await self.repo.put('evt.events',eid,event)
        await self.repo.put('evt.event_observations',obs,dict(obs_id=obs,event_id=eid,item_id=item['item_id'],link_type='ORIGIN',public_at=public,freshness=x['freshness'],match_method='AGENT',observed_at=item['observed_at']))
        item['extraction_status']='DONE'; await self.repo.put('ingest.source_items',item['item_id'],item)
        if c.get('session_id'):
            session=await self.repo.get('ops.agent_sessions',c['session_id'])
            if session: session['event_id']=eid; await self.repo.put('ops.agent_sessions',c['session_id'],session)
        return {'event_id':eid,'accepted_claims':claims,'dropped_claims':dropped}
    async def apply_states(self,event,claims,c):
        old=event['event_state']; states=[r['state_claim'] for r in claims if r.get('state_claim') in STATES]
        event['event_state']=max([old,*states],key=STATES.index)
        event['state_verified']=event.get('state_verified',False) if old==event['event_state'] else any(r['verified'] and r['state_claim']==event['event_state'] for r in claims)
        if any(r['verified'] and r.get('state_claim')==event['event_state'] for r in claims): event['state_verified']=True
        if old!=event['event_state']:
            sid=new_id('state'); await self.repo.put('evt.state_changes',sid,dict(state_change_id=sid,event_id=event['event_id'],ladder='EVENT',from_state=old,to_state=event['event_state'],evidence=[r['claim_id'] for r in claims if r.get('state_claim')==event['event_state']],changed_at=iso()),immutable=True)
        for r in claims:
            marker=r.get('state_claim')
            if marker in {'CANCELLED','REVERSED','REJECTED','PAUSED'}:
                event['negative_marker']=marker
                await self.contradiction(event['event_id'],'UNRESOLVED' if marker=='PAUSED' else 'FATAL',marker,[r['claim_id']],c)
    async def contradiction(self,eid,kind,description,evidence,c):
        key=new_id('ctr'); row=dict(contradiction_id=key,event_id=eid,type=kind,description=description,evidence=evidence,detected_at=iso(),**self.provenance(c)); await self.repo.put('ana.contradictions',key,row); return row
    async def add_observation(self,a,c):
        event=await self.require('evt.events',a['event_id']); item=await self.require('ingest.source_items',a['item_id']); link=a.get('link',a.get('link_type'))
        if link not in {'CORROBORATION','STATE_UPDATE','CONTRADICTION','DENIAL'}: raise ValueError('Invalid observation link')
        if 'public_at' not in a or 'freshness' not in a: raise ValueError('V7 observation public_at and freshness required')
        public=self.validate_time(a,item); obs=new_id('obs'); certainty=a.get('certainty',event['certainty'])
        claims,dropped=await self.validate_claims(a['claims'],item,c,certainty,obs,event['event_id']); before=event['event_state']; await self.apply_states(event,claims,c)
        contradictions=[]
        if link=='DENIAL':
            if not any(r['verified'] for r in claims): raise ValueError('DENIAL requires accepted primary evidence')
            event['lifecycle']='INVALIDATED'; contradictions=[await self.contradiction(event['event_id'],'FATAL','Official denial',[r['claim_id'] for r in claims],c)]
        elif link=='CONTRADICTION' and claims: contradictions=[await self.contradiction(event['event_id'],'UNRESOLVED','Contradicting observation',[r['claim_id'] for r in claims],c)]
        elif link=='STATE_UPDATE' and event['event_state']==before: link='CORROBORATION'
        if claims and link!='CORROBORATION' and event['lifecycle']!='INVALIDATED': event['lifecycle']='SCORED'
        await self.repo.put('evt.events',event['event_id'],event)
        await self.repo.put('evt.event_observations',obs,dict(obs_id=obs,event_id=event['event_id'],item_id=item['item_id'],link_type=link,public_at=public,freshness=a['freshness'],match_method='AGENT',observed_at=item['observed_at']))
        return {'event_state':event['event_state'],'accepted_claims':claims,'dropped_claims':dropped,'contradictions':contradictions,'link_type':link}
    async def merge_into(self,a,c):
        source=await self.require('evt.events',c['event_id']); target=await self.require('evt.events',a['event_id'])
        if source['event_id']==target['event_id']: return target
        if parse_time(target['known_to_us_at'])>parse_time(source['known_to_us_at']): raise ValueError('Merge target must be the earlier event')
        async with self.repo.lock('event:'+target['event_id']):
            target['known_to_us_at']=min(target['known_to_us_at'],source['known_to_us_at'],key=parse_time)
            for table,key in [('evt.event_observations','obs_id'),('evt.claims','claim_id')]:
                for row in await self.rows(table,source['event_id']):
                    row['event_id']=target['event_id']; await self.repo.put(table,row[key],row)
                    if key=='obs_id': await self.repo.enqueue('AGENT_MESSAGE',{'kind':'ITEM_ADDED','event_id':target['event_id'],'item_id':row['item_id'],'session_id':target.get('session_id')},'merge:'+row['obs_id'])
            source.update(lifecycle='MERGED',merged_into=target['event_id']); await self.repo.put('evt.events',source['event_id'],source); await self.repo.put('evt.events',target['event_id'],target)
        return target
    async def set_assessment(self,a,c):
        eid=a['event_id']; event=await self.require('evt.events',eid); x=copy.deepcopy(a['assessment']); claims=await self.rows('evt.claims',eid); accepted={r['claim_id']:r for r in claims if r['accepted']}
        criterion=x['criterion']; quote=x.get('criterion_quote',''); klass=x['class']; caps=[]
        if klass not in {'LOW','MATERIAL','MAJOR','STRUCTURAL_SHOCK'}: raise ValueError('Invalid EMS class')
        if criterion not in {'G1','G2','G3','G4','G5','G6','M1','M2','M3','M4','M5','M6','M7','MATERIAL','LOW'}: raise ValueError('Invalid class criterion')
        evidence_ok=bool(quote) and any(normalized(quote) in normalized(e['quote']) for r in accepted.values() for e in r['evidence'])
        if klass=='STRUCTURAL_SHOCK' and (event['certainty']!='HIGH' or not event['state_verified'] or STATES.index(event['event_state'])<2 or not criterion.startswith('G') or not evidence_ok): klass='MAJOR'; caps.append('STRUCTURAL_GATE')
        if klass=='MAJOR' and (event['certainty']=='LOW' or not evidence_ok or not criterion.startswith(('M','G'))): klass='MATERIAL'; caps.append('MAJOR_EVIDENCE')
        if criterion=='M1' and not any(v in quote.casefold() for v in ['binance','coinbase','okx','bybit','kraken','robinhood']): klass='MATERIAL'; caps.append('M1_VENUE')
        if event['event_state']=='PROPOSED' and criterion not in {'M4','M5'} and klass in {'MAJOR','STRUCTURAL_SHOCK'}: klass='MATERIAL'; caps.append('PROPOSED')
        universe={r['token_id'] for r in self.settings.load_config('registry')['tokens']}
        concerned=set(event['tokens_mentioned']) | {r.get('token_id') for r in await self.rows('evt.beneficiary_edges',eid)}
        if not concerned & universe: klass='LOW'; caps.append('NO_UNIVERSE_TOKEN')
        if event['certainty']=='LOW' and klass in {'MAJOR','STRUCTURAL_SHOCK'}: klass='MATERIAL'; caps.append('LOW_CERTAINTY')
        dims=x.get('dims',{})
        for name in ['economic_directness','magnitude','permanence','distribution_expansion','mechanical_flows','surprise','implementation']:
            d=dims.get(name)
            if not isinstance(d,dict): raise ValueError('Every dimension requires value, rationale and evidence')
            if d.get('value') is not None and (not isinstance(d['value'],(int,float)) or not 0<=d['value']<=10): raise ValueError('Dimension outside 0..10')
            if not d.get('rationale') or any(ref not in accepted for ref in d.get('evidence',[])): raise ValueError('Dimension evidence/rationale invalid')
            if d.get('value') is None and not d.get('reason'): raise ValueError('Missing score requires reason')
        dims['implementation']['value']=IMPLEMENTATION[event['event_state']]
        if dims['magnitude'].get('value') is not None and not re.search(r'\d',dims['magnitude']['rationale']): dims['magnitude'].update(value=None,reason='NOT_ESTIMABLE')
        horizon=x.get('impact_horizon',{})
        if horizon.get('impact_start')=='NOW' and STATES.index(event['event_state'])<STATES.index('LIVE'): raise ValueError('NOW requires LIVE or evidenced immediate market access')
        if horizon.get('ramp_shape')=='CONDITIONAL' and not horizon.get('dependencies'): raise ValueError('Conditional ramp requires dependencies')
        for chain in x.pop('capture_chains',[]):
            score=chain.get('capture_score'); mechanism=chain.get('token_mechanism',{}); applied=[]
            primary=any(accepted.get(ref,{}).get('verified') for ref in mechanism.get('evidence',[]))
            if score is not None:
                if not chain.get('eligible_activity') or not chain.get('protocol_capture'): score=min(score,3); applied.append('C1')
                if not primary: score=min(score,4); applied.append('C2')
                if not mechanism.get('active'): score=min(score,5); applied.append('C3')
                fees=chain.get('effective_fees'); fees_value=fees.get('value',fees.get('number')) if isinstance(fees,dict) else fees
                if fees_value is None: score=min(score,7); applied.append('C4')
            chain.update(event_id=eid,version=len(await self.rows('evt.capture_chains',eid))+1,capture_score=score,capture_class='UNKNOWN' if score is None else 'DIRECT' if score>=7 and primary and mechanism.get('active') else 'INDIRECT' if score>=4 else 'NARRATIVE_ONLY',conditional=not mechanism.get('active'),caps_applied=applied,**self.provenance(c))
            await self.repo.put('evt.capture_chains',f"{eid}:{chain['token_id']}:{chain['version']}",chain)
        ems=new_id('ems'); x.update(ems_id=ems,event_id=eid,version=len(await self.rows('evt.ems_assessments',eid))+1,cutoff=iso(),dims=dims,**{'class':klass},class_reasons=x.get('class_reasons',[])+caps,**self.provenance(c)); await self.repo.put('evt.ems_assessments',ems,x)
        event.update(current_ems_id=ems,lifecycle='ANALYZING' if klass in {'MAJOR','STRUCTURAL_SHOCK'} else 'TRACKED'); await self.repo.put('evt.events',eid,event)
        if klass in {'MAJOR','STRUCTURAL_SHOCK'}:
            anchor=event['event_time'].get('value') if event['event_time'].get('precision')=='EXACT' and event['state_verified'] else event['public_at']
            for token in sorted((concerned & universe)|{'BTC','ETH'}):
                for label,minutes in [('PRE_24H',-1440),('PRE_4H',-240),('PRE_1H',-60),('POST_15M',15),('POST_1H',60),('POST_4H',240),('POST_24H',1440),('POST_72H',4320)]:
                    due=parse_time(anchor)+timedelta(minutes=minutes)
                    await self.repo.enqueue('REACTION_CAPTURE',{'event_id':eid,'token_id':token,'label':label,'anchor':anchor,'retro':due<utcnow()},f'{eid}:{token}:{label}',run_after=iso(max(due,utcnow())))
            source_ids={r['source_id'] for r in await self.rows('ingest.source_items') if any(o['item_id']==r['item_id'] for o in await self.rows('evt.event_observations',eid)) and r['source_class']=='PRIMARY'}
            for sid in source_ids:
                source=await self.repo.get('reg.sources',sid)
                if source: source.update(escalated_until=iso(utcnow()+timedelta(hours=2)),escalated_interval_s=15); await self.repo.put('reg.sources',sid,source)
        return x
    async def set_beneficiaries(self,a,c):
        eid=a['event_id']; event=await self.require('evt.events',eid); version=len(await self.rows('evt.beneficiary_maps',eid))+1; claims={r['claim_id']:r for r in await self.rows('evt.claims',eid)}; registry=self.settings.load_config('registry'); tokens={t['token_id'] for t in registry['tokens']}; chains=await self.rows('evt.capture_chains',eid); edges=[]
        for raw in a['edges']:
            e=copy.deepcopy(raw)
            if e['layer'] not in LAYERS or e['role'] not in {'PRIMARY','SECONDARY','COMPETITOR','LOSER'}: raise ValueError('Invalid layer or beneficiary role')
            if not e.get('evidence') or any(ref not in claims or not claims[ref]['accepted'] for ref in e['evidence']): raise ValueError('Beneficiary requires accepted evidence')
            if not e.get('causal_chain'): raise ValueError('Beneficiary requires causal/share-loss mechanism')
            chain=next((r for r in reversed(chains) if r['token_id']==e.get('token_id')),{}); score=chain.get('capture_score')
            state=e.get('integration_state','COMPATIBLE')
            support=[claims[r] for r in e['evidence']]; supported=[r.get('state_claim') for r in support if r.get('state_claim') in INTEGRATION]
            state=max(supported,key=INTEGRATION.index) if supported else 'COMPATIBLE'
            if all(r.get('directionality')=='SOURCE_SIDE_SUPPORT' for r in support) and INTEGRATION.index(state)>2: state='BUILT'
            e['tradable']=e.get('token_id') in tokens and any(i['token_id']==e.get('token_id') for i in registry['instruments'])
            mature=STATES.index(event['event_state'])>=STATES.index('DEPLOYED') if e['role']=='PRIMARY' else INTEGRATION.index(state)>=2
            e.update(edge_id=new_id('edge'),event_id=eid,map_version=version,integration_state=None if e['role']=='PRIMARY' else state,capture_ref=[eid,e.get('token_id'),chain.get('version')],capture_score=score,capture_class=chain.get('capture_class','UNKNOWN'),capture=chain.get('capture_class','UNKNOWN'),mechanism_evidence=chain.get('token_mechanism',{}).get('evidence',[]),credible=e['role'] in {'PRIMARY','SECONDARY'} and score is not None and score>=4 and e['exposure_score']>=6 and mature and e['edge_certainty'] in {'MEDIUM','HIGH'} and e['tradable'],**self.provenance(c))
            await self.repo.put('evt.beneficiary_edges',e['edge_id'],e); edges.append(e)
        primary=[e for e in edges if e['role']=='PRIMARY' and e['tradable']]; secondary=sorted([e for e in edges if e['role']=='SECONDARY' and e['credible']],key=lambda e:e['exposure_score']*e['capture_score'],reverse=True)[:4]; losers=sorted([e for e in edges if e['role']=='LOSER' and e['tradable']],key=lambda e:e['exposure_score'],reverse=True)[:2]
        notes=a.get('layer_reasons',{}); layers={}
        for layer in LAYERS:
            found=[e['edge_id'] for e in edges if e['layer']==layer]
            if not found and not notes.get(layer): raise ValueError(f'Layer {layer} requires NONE/UNKNOWN reason')
            layers[layer]={'status':'FOUND' if found else 'UNKNOWN','edges':found,'note':notes.get(layer,'Evidence-backed edges')}
        result=dict(event_id=eid,map_version=version,cutoff=iso(),layers=layers,analysis_set=list(dict.fromkeys(e['token_id'] for e in primary+secondary+losers)),**self.provenance(c)); await self.repo.put('evt.beneficiary_maps',f'{eid}:{version}',result); return result
    async def get_market_truth(self,a,c):
        eid=a['event_id']; event=await self.require('evt.events',eid); assessment=await self.require('evt.ems_assessments',event['current_ems_id'])
        if assessment['class'] not in {'MAJOR','STRUCTURAL_SHOCK'}: raise ValueError('Market escalation requires MAJOR+')
        maps=await self.rows('evt.beneficiary_maps',eid)
        allowed=set(maps[-1]['analysis_set'] if maps else event['tokens_mentioned'])
        if not set(a['tokens'])<=allowed: raise ValueError('Tokens outside analysis set')
        await self.repo.enqueue('ESCALATE_TOKENS',{'event_id':eid,'tokens':a['tokens']},eid+':'+','.join(sorted(a['tokens'])))
        truths=[]
        for token in a['tokens']:
            cutoff=iso(); await self.repo.enqueue('RUN_MARKET_TRUTH',{'event_id':eid,'token_id':token,'cutoff':cutoff},f'{eid}:{token}:{cutoff}')
            truths.append(await self.truth.assess(eid,token,cutoff))
        return {'market_truth':truths}
    async def get_decision(self,a,c):
        eid=a['event_id']; token=a.get('token',a.get('token_id')); event=await self.require('evt.events',eid); assessment=await self.require('evt.ems_assessments',event['current_ems_id']); edges=[r for r in await self.rows('evt.beneficiary_edges',eid) if r.get('token_id')==token]; truths=[r for r in await self.rows('ana.market_truth',eid) if r['token_id']==token]
        if not edges or not truths: raise ValueError('Decision requires beneficiary and market truth')
        calls=[r for r in await self.rows('ana.calls',eid) if r['token_id']==token and r['status']=='OPEN']; conditions=[r for r in await self.rows('ana.watch_conditions',eid) if r['token_id']==token]
        latest_truth=max(truths,key=lambda r:r['cutoff'])
        result=self.engine.evaluate(event,assessment,edges[-1],latest_truth,open_call=calls[-1] if calls else None,conditions=conditions); result.update(dec_id=result.get('dec_id',new_id('dec')),event_id=eid,token_id=token,cutoff=result.get('cutoff',iso()),**self.provenance(c)); 
        relative=latest_truth.get('relative',{}); anchor=(relative.get('anchor') or {}).get('ref_price'); entry=relative.get('entry_ref_price',anchor)
        if entry is not None and anchor is not None: result.update(entry_ref_price=str(entry),invalidation_price=str(min(Decimal(str(anchor)),Decimal(str(entry)))))
        await self.repo.put('ana.decisions',result['dec_id'],result)
        if isinstance(result.get('outlook'),dict):
            outlook=dict(result['outlook'],out_id=new_id('out'),event_id=eid,token_id=token,cutoff=result['cutoff'],**self.provenance(c)); await self.repo.put('ana.outlooks',outlook['out_id'],outlook)
        for condition in result.get('conditions',[]):
            if isinstance(condition,dict):
                condition=dict(condition,cond_id=condition.get('cond_id',new_id('cond')),event_id=eid,token_id=token,created_by=result['dec_id']); await self.repo.put('ana.watch_conditions',condition['cond_id'],condition)
        return result
    async def publish_card(self,a,c):
        from tank.analysis.cards import compose_card, lint_card, alert_gate
        eid=a['event_id']; event=await self.require('evt.events',eid); assessment=await self.require('evt.ems_assessments',event['current_ems_id']); alerts=sorted(await self.rows('out.alerts',eid),key=lambda r:r['version']); previous=alerts[-1] if alerts else None
        decisions=await self.rows('ana.decisions',eid); latest={}
        for d in sorted(decisions,key=lambda r:r['cutoff']): latest[d['token_id']]=d
        decisions=list(latest.values())
        event['contradictions']=await self.rows('ana.contradictions',eid)
        gate_previous=None
        if previous:
            gate_previous=dict(previous,decisions=[await self.require('ana.decisions',ref) for ref in previous['decisions']],published_at=previous['generated_at'],ems_class=previous['card_struct']['assessment']['class'],event_state=previous['card_struct']['event']['event_state'])
        ok,reason=alert_gate(event,assessment,previous=gate_previous,decisions=decisions)
        if not ok: return {'published':False,'reason':reason}
        kind=a['kind']
        if kind not in {'FULL','PARTIAL','UPDATE'}: raise ValueError('Invalid alert kind')
        if kind=='UPDATE' and not previous: raise ValueError('UPDATE requires a published original')
        if kind=='PARTIAL' and assessment['class']!='STRUCTURAL_SHOCK': raise ValueError('PARTIAL requires STRUCTURAL_SHOCK')
        beneficiaries=await self.rows('evt.beneficiary_edges',eid); truths=await self.rows('ana.market_truth',eid); ctx={'event':event,'assessment':assessment,'beneficiaries':beneficiaries,'decisions':decisions,'market_truths':truths}
        verdict=a.get('verdict',''); why=a.get('why',[])
        if kind!='PARTIAL' and len(why) not in {2,3}: raise ValueError('FULL/UPDATE require 2–3 WHY bullets')
        text=compose_card(event,assessment,beneficiaries,decisions,truths,kind=kind,verdict=verdict,why=why); violations=lint_card(text,ctx)
        if violations: return {'published':False,'reason':'LINTER','violations':violations}
        cutoff=iso(); observations=await self.rows('evt.event_observations',eid); claims=await self.rows('evt.claims',eid); ids={o['item_id'] for o in observations}|{e['item_id'] for r in claims for e in r['evidence']}
        bundle={'cutoff':cutoff,'event':event,'source_items':[await self.require('ingest.source_items',i) for i in sorted(ids)],'observations':observations,'claims':claims,'state_changes':await self.rows('evt.state_changes',eid),'ems':assessment,'capture_chains':await self.rows('evt.capture_chains',eid),'beneficiary_map':await self.rows('evt.beneficiary_maps',eid),'market_truth':truths,'outlooks':await self.rows('ana.outlooks',eid),'decisions':decisions,'contradictions':await self.rows('ana.contradictions',eid),'data_confidence':[m['data_confidence'] for m in truths],'unresolved_fields':[],'config_hash':sha256({name:self.settings.load_config(name) for name in ['registry','thresholds','semantics','dependency_map']}),'code_version':self.settings.code_version,'model_ids':[self.settings.openma_model],'agent_version':'tank-event-analyst-v1','skill_version':'v1','session_id':c.get('session_id'),'tool_call_ids':[c['call_id']] if c.get('call_id') else [],'request_log':[]}
        # Canonical ordering makes the exact frozen payload independently hashable.
        for k,v in bundle.items():
            if isinstance(v,list) and v and isinstance(v[0],dict): v.sort(key=canonical_json)
        digest=sha256(bundle); await self.repo.put('out.evidence_bundles',digest,{'bundle_id':'EVB-'+digest,'sha256':digest,'payload':bundle},immutable=True)
        aid=previous['alert_id'] if previous else new_id('alert'); version=previous['version']+1 if previous else 1
        alert=dict(alert_id=aid,version=version,kind=kind,event_id=eid,generated_at=cutoff,card_text=text,card_struct=ctx,decisions=[d['dec_id'] for d in decisions],evidence_bundle_id=digest,linter={'passed':True,'violations':[],'regenerations':0},supersedes_version=version-1 if previous else None,**self.provenance(c)); await self.repo.put('out.alerts',f'{aid}:{version}',alert,immutable=True)
        delivery=new_id('delivery'); await self.repo.put('out.deliveries',delivery,{'delivery_id':delivery,'alert_id':aid,'version':version,'channel':'telegram:alerts','status':'GENERATED','attempt':0,'at':cutoff},immutable=True)
        await self.repo.enqueue('DELIVER',{'alert_id':aid,'version':version,'channel':'telegram:alerts'},f'{aid}:{version}:telegram:alerts',priority=0,max_attempts=5)
        return {'published':True,'alert_id':aid,'version':version,'evidence_bundle_id':digest}
    async def schedule_milestone(self,a,c):
        event=await self.require('evt.events',a['event_id']); due=iso(a['due_at']); source=a['source']; source_id=source.get('source_id') if isinstance(source,dict) else source
        if not await self.repo.get('reg.sources',source_id): raise ValueError('Milestone source must be registered')
        event['next_milestone']={'kind':a['milestone'],'due_at':due,'source_id':source_id}; await self.repo.put('evt.events',event['event_id'],event)
        job=await self.repo.enqueue('MILESTONE_CHECK',{'event_id':event['event_id'],'milestone':a['milestone'],'due_at':due,'source_id':source_id},f"{event['event_id']}:{a['milestone']}:{due}",run_after=due)
        source=await self.repo.get('reg.sources',source_id); source.update(escalated_until=iso(parse_time(due)+timedelta(hours=1)),escalated_from=iso(parse_time(due)-timedelta(hours=1)),escalated_interval_s=15); await self.repo.put('reg.sources',source_id,source); return {'job_id':job['job_id']}
    async def propose_pattern(self,a,c):
        if a['hook'] not in {'BENEFICIARY_LAYER','MARKET_TRUTH_STATE','SOURCE_OPPORTUNITY','ALTERNATIVE_EXPLANATION'}: raise ValueError('Unknown scout hook')
        if not a.get('evidence'): raise ValueError('Scout evidence required')
        subject=a['subject']; subject={'concept_name':subject} if isinstance(subject,str) else subject
        existing=next((p for p in await self.rows('scout.expansion_proposals') if p['concept_name'].casefold()==subject['concept_name'].casefold() and not p.get('human_action')),None)
        row=existing or dict(prp_id=new_id('prp'),hook=a['hook'],**subject,occurrences=0,evidence=[],recommendation='SUBCATEGORY',confidence='LOW',**self.provenance(c)); row['occurrences']+=1; row['evidence']+=a['evidence']; await self.repo.put('scout.expansion_proposals',row['prp_id'],row); await self.repo.enqueue('SCOUT_REVIEW',{'prp_id':row['prp_id']},f"{row['prp_id']}:{row['occurrences']}",priority=4); return row

class _borrow:
    def __init__(self,client): self.client=client
    async def __aenter__(self): return self.client
    async def __aexit__(self,*args): return False


def validate_schema(value, schema, path='$'):
    """Validate the JSON-schema subset used by the checked-in tool declarations."""
    kinds={'object':dict,'array':list,'string':str,'boolean':bool,'integer':int,'number':(int,float),'null':type(None)}
    declared=schema.get('type'); declared=[declared] if isinstance(declared,str) else declared
    if declared and not any(isinstance(value,kinds[t]) and not (t in {'integer','number'} and isinstance(value,bool)) for t in declared): raise ValueError(f'{path}: invalid type')
    if 'enum' in schema and value not in schema['enum']: raise ValueError(f'{path}: invalid enum')
    if isinstance(value,dict):
        for key in schema.get('required',[]):
            if key not in value: raise ValueError(f'{path}: missing {key}')
        props=schema.get('properties',{})
        if schema.get('additionalProperties') is False and set(value)-set(props): raise ValueError(f'{path}: unknown fields {sorted(set(value)-set(props))}')
        for key,item in value.items():
            if key in props: validate_schema(item,props[key],path+'.'+key)
    if isinstance(value,list):
        for n,item in enumerate(value): validate_schema(item,schema.get('items',{}),f'{path}[{n}]')
    for key,op in [('minimum',lambda a,b:a<b),('maximum',lambda a,b:a>b)]:
        if key in schema and op(value,schema[key]): raise ValueError(f'{path}: violates {key}')
