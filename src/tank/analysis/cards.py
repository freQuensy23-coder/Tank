"""Template-first cards and publication policy; diagnostic wording is preserved."""
import re
from datetime import timedelta
from tank.util import utcnow, parse_time


def alert_gate(event, assessment, *, previous=None, decisions=None, now=None):
    decisions = decisions or []
    cls=assessment.get('class',assessment.get('ems_class'))
    if previous:
        old={d.get('token_id'):d.get('action') for d in previous.get('decisions',[])}
        changed=any(d.get('action') != old.get(d.get('token_id')) and d.get('action') != 'HOLD' for d in decisions)
        fatal=any(c.get('type') == 'FATAL' and not c.get('resolved_at') for c in event.get('contradictions',[])+assessment.get('contradictions',[]))
        changed=changed or fatal or cls != previous.get('ems_class',previous.get('class',cls)) or event.get('event_state',event.get('state')) != previous.get('event_state',event.get('event_state',event.get('state'))) or event.get('milestone_resolved',False) or event.get('call_expired',False)
        if not changed: return False,'NO_MATERIAL_CHANGE'
        immediate=fatal or any(d.get('action') in {'START','EXIT'} for d in decisions)
        stamp=previous.get('published_at',previous.get('created_at'))
        if not immediate and stamp and (parse_time(now) if now else utcnow())-parse_time(stamp)<timedelta(minutes=15): return False,'UPDATE_RATE_LIMIT'
        return True,'UPDATE_SAFETY_OVERRIDE' if event.get('freshness') == 'STALE' or event.get('lifecycle') == 'INVALIDATED' else 'UPDATE'
    if cls not in {'MAJOR','STRUCTURAL_SHOCK'}: return False,'BELOW_MAJOR'
    if assessment.get('certainty',event.get('certainty','LOW')) not in {'MEDIUM','HIGH'}: return False,'LOW_CERTAINTY'
    if event.get('freshness') == 'STALE': return False,'STALE'
    if event.get('lifecycle') == 'INVALIDATED': return False,'INVALIDATED'
    return True,'INITIAL'


def _na(value, reason='NOT_AVAILABLE'):
    return str(value) if value is not None else 'n/a ('+reason+')'


def compose_card(event, assessment, beneficiaries, decisions, market_truths, *, kind='FULL', verdict='', why=None):
    if isinstance(market_truths,dict): market_truths=list(market_truths.values())
    dm={d.get('token_id'):d for d in decisions}; mm={m.get('token_id'):m for m in market_truths}
    claims=event.get('claims',assessment.get('claims',[]))
    verified=[]; unverified=[]
    for c in claims:
        if c.get('verified') or c.get('status') == 'VERIFIED': verified.append(str(c.get('text',c.get('claim','confirmed claim')))+' ['+str(c.get('source',c.get('source_id','source unavailable')))+']')
        else: unverified.append(str(c.get('missing',c.get('text','verification missing'))))
    lines=[f"{assessment.get('class',assessment.get('ems_class','UNKNOWN'))} ({assessment.get('criterion',assessment.get('matched_criterion','n/a'))}) — {verdict}",f"Stage: {_na(event.get('event_state',event.get('state')))} · Certainty: {_na(assessment.get('certainty',event.get('certainty')))} · Fresh: {_na(event.get('freshness'))}", 'VERIFIED: '+('; '.join(verified) or 'n/a (no verified claims)')+' · NOT VERIFIED: '+('; '.join(unverified) or '—'),'WHY IT MATTERS']
    lines += ['• '+str(w) for w in (why or ['Economic effect: not estimable'])[:3]]
    lines.append('WHO: '+ ' · '.join(str(b.get('token_id'))+' '+str(b.get('role')) for b in beneficiaries[:3]))
    for b in beneficiaries[:3]:
        token=b.get('token_id'); d=dm.get(token,{}); m=mm.get(token,{}); o=d.get('outlook',{}); r=m.get('relative') or {}; dc=m.get('data_confidence') or {}
        facts=[]
        for f in m.get('decisive_facts',[])[:2]:
            if isinstance(f,dict): facts.append(f"{_na(f.get('value'),f.get('reason','NOT_AVAILABLE'))} {_na(f.get('unit'))} / {_na(f.get('window'))} {f.get('metric','')}")
            else: facts.append(str(f))
        alt=(m.get('divergence') or {}).get('unresolved',[])+m.get('alternatives',[])
        lines += [f"── {token} ({b.get('role','UNKNOWN')}) ──",f"MARKET TRUTH: {m.get('move_class','UNKNOWN')} — "+('; '.join(facts) or 'n/a (market facts unavailable)')+f"; priced: {r.get('repricing_band','UNKNOWN')}", 'Unresolved alternatives: '+(', '.join(alt) or '—'),f"OUTLOOK: {o.get('direction','UNKNOWN')} · {o.get('potential','UNKNOWN')} · best { {'H1':'1h','H4':'4h','H12':'12h','H24':'24h','D3':'3d','D7':'7d'}.get(o.get('best_horizon'), 'n/a')} ({o.get('horizon_reason','unknown')})"]
        lines += [name+': '+str(text) for name,text in o.get('scenarios',{}).items()]
        lines += [f"WHAT TO DO: {d.get('action','WAIT')}"+(' (tactical)' if d.get('tactical') else '')+f" — Thesis {d.get('thesis_grade','n/a')} · Trade {d.get('trade_grade','n/a')}"]
        for role,symbol in [('UPGRADE','↑'),('DOWNGRADE','↓')]:
            condition=next((c for c in d.get('conditions',[]) if c.get('role')==role),{})
            lines.append(symbol+' '+condition.get('human_text','n/a (condition unavailable)').replace('two consecutive hourly windows','two hourly windows').replace('Spot BUY with most volume buying','Majority-volume spot BUY').replace('Spot SELL with most volume selling','Majority-volume spot SELL'))
        lines.append(f"CONFIDENCE: {o.get('confidence','LOW')} — main unknown: "+str((dc.get('limiting') or {}).get('reason','n/a (limiting input unavailable)')))
    text='\n'.join(lines)
    if len(text)>3500: raise ValueError('CARD_LENGTH: card exceeds 3500 characters; shorten verdict, facts or WHY')
    return text


def lint_card(text, context):
    errors=[]
    def fail(rule,sentence): errors.append({'rule':rule,'sentence':sentence})
    if len(text)>3500: fail('LENGTH', 'Card exceeds 3500 characters')
    if text.count('──')>6: fail('TOKENS','More than three token blocks')
    event=context.get('event',{}); bs=context.get('beneficiaries',[])
    mts=context.get('market_truths',[])
    if isinstance(mts,dict): mts=list(mts.values())
    # Each block is checked against its own token; header prose must be valid for all shown tokens.
    current=mts
    for line in text.splitlines():
        if line.startswith('──'):
            current=[m for m in mts if str(m.get('token_id')) in line] or mts
        lower=line.lower()
        diagnostic=line.startswith('Unresolved alternatives:')
        for m in current or [{}]:
            spot=(m.get('spot') or {}).get(m.get('primary_window','W4H'),{})
            broad=spot.get('classification')=='BROAD' and (spot.get('coverage') or {}).get('n_complete',0)>=2
            if re.search(r'\b(market-wide|broad|buying|selling)\b',lower) and not broad and not line.startswith(('↑','↓')):
                venues=[v.get('venue','').lower() for v in spot.get('venues',[])]
                if not any(v and v in lower for v in venues): fail('L1',line)
            deriv=m.get('derivatives') or {}; deriv=deriv if isinstance(deriv,dict) else {}
            if re.search(r'\b(leverage|new positions?|oi build|open positions)\b',lower) and (deriv.get('synchronized') or {}).get('oi_class')!='REAL_NATIVE': fail('L2',line)
            if re.search(r'\b(accumulation|accumulating)\b',lower) and m.get('move_class')!='SPOT_LED_ACCUMULATION' and not diagnostic and not line.startswith('↑'): fail('L11',line)
            div=m.get('divergence') or {}
            trigger=div.get('trigger',False) or (spot.get('direction')=='SELL' and div.get('state','UNKNOWN')!='UNKNOWN')
            if trigger and not diagnostic:
                if re.search(r'\b(weak|weakness|bearish)\b',lower): fail('L12',line)
                if re.search(r'\b(absorption|absorbed)\b',lower) and div.get('state') not in {'ABSORPTION_CONFIRMED','CONFIRMED'} and 'PASSIVE_SPOT_ABSORPTION' not in line: fail('L12',line)
            if re.search(r'\b(trend|increasing|decreasing)\b',lower) and not all(spot.get('cohort_consistent',{}).values()): fail('L6',line)
            if 'market-wide' in lower and 'liquidation' in lower and any(v.get('liq_coverage')!='COMPLETE' for v in deriv.get('per_venue',[])): fail('L5',line)
        state=event.get('event_state',event.get('state','UNKNOWN')); verified=event.get('state_verified',event.get('verified',False))
        if not lower.startswith('not verified:') and 'unverified' not in lower and re.search(r'\b(live|launched|adopted|integrated|approved)\b',lower):
            required_live=bool(re.search(r'\b(live|launched|adopted|integrated)\b',lower))
            if not verified or state not in ({'LIVE','MONEY_FLOWING'} if required_live else {'APPROVED','SIGNED','DEPLOYED','LIVE','MONEY_FLOWING'}): fail('L3',line)
        if re.search(r'\b(revenue to holders|buyback|burn|value accrual)\b',lower) and not any(b.get('capture_class',b.get('capture')) in {'DIRECT','INDIRECT'} and b.get('mechanism_evidence') for b in bs): fail('L4',line)
        if re.search(r'(volume|tvl).*(capture|accrual)',lower): fail('L4',line)
        if re.search(r'\b(chance|probability|odds)\b|%\s*likely',lower): fail('L7',line)
        if re.match(r'(base|upside|failure):',lower) and re.search(r'\d+\s*[-–]\s*\d+',line): fail('L7',line)
        if re.search(r'\b(new|fresh|just|breaking)\b',lower) and event.get('freshness') not in {'FRESH','NORMAL'} and not line.startswith('Stage:'): fail('L10',line)
        if re.search(r'n/a.*\b0\b',lower): fail('L9',line)
        if re.search(r'(?<![\w])\d+(?:\.\d+)?',line) and not line.startswith(('Stage:','Scores','──','↑','↓')):
            if not re.search(r'\b(?:\d+\s*[hmds]|EVENT|W1H|W4H|W24H|hour|event)\b',line,re.I) or not re.search(r'%|\b(?:USD|bps|tokens?|WLD|BTC|ETH)\b|\$',line): fail('L8',line)
    return list({(e['rule'],e['sentence']):e for e in errors}.values())
