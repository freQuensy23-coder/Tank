"""Deterministic, cutoff-bounded market evidence. Missing evidence never votes true."""
from __future__ import annotations
from collections import defaultdict
from datetime import timedelta
from math import sqrt, log
from statistics import mean, stdev, median
from tank.util import parse_time, iso, new_id

WINDOWS = {'W15M':900,'W1H':3600,'W4H':14400,'W24H':86400}
RISK_ORDER = ['LIQUIDITY_VACUUM','SHORT_SQUEEZE','PERP_LED_CHASE','DISTRIBUTION','SELL_SIDE_ABSORPTION','ORGANIC_MIXED','SPOT_LED_ACCUMULATION','REAL_MARKET_FRAGMENTATION','PRICE_MARKUP_FAKE_OI']

def number(v):
    if isinstance(v,dict): v=v.get('value')
    return None if v is None else float(v)

def ratio(a,b): return a/b if a is not None and b else None

def missing(reason='INCOMPLETE',unit=None):
    return {'value':None,'unit':unit,'window':None,'observed_at':None,'precision':None,'coverage':None,'reason':reason}

def percentile(values,p):
    s=sorted(v for v in values if v is not None)
    if not s:return None
    k=(len(s)-1)*p; i=int(k)
    return s[i]+(s[min(i+1,len(s)-1)]-s[i])*(k-i)

def normal(values):
    return [percentile(values,.025),percentile(values,.975)] if values else None

def beyond(v,r): return None if v is None or not r or None in r else v<r[0] or v>r[1]
def within(v,r):
    b=beyond(v,r)
    return None if b is None else not b

def decomposition(n0,n1,p0,p1):
    position=(n1-n0)*p0; markup=n0*(p1-p0); interaction=(n1-n0)*(p1-p0)
    return dict(position=position,markup=markup,interaction=interaction,pos_share=ratio(abs(position),abs(position)+abs(markup)+abs(interaction)))

def oi_class(dn,du,dec,nrange,urange):
    nb=beyond(dn,nrange); ub=beyond(du,urange)
    if nb is None or ub is None:return None
    return 'REAL_NATIVE' if nb and (dec['pos_share'] or 0)>.5 else 'PRICE_MARKUP' if not nb and ub else 'MIXED'

def beta(token,benchmark):
    pairs=[(benchmark[t],token[t]) for t in token.keys() & benchmark.keys()]
    if len(pairs)<168:return 1.,'INSUFFICIENT_HISTORY'
    x,y=zip(*pairs); xm,ym=mean(x),mean(y); den=sum((v-xm)**2 for v in x)
    return ((2/3)*sum((a-xm)*(b-ym) for a,b in pairs)/den+1/3,None) if den else (1.,'NOT_ESTIMABLE')

def market_context(r,rv,returns,vols):
    rr,vr=normal(returns),normal(vols)
    if not rr or not vr or r is None or rv is None:return None
    return 'STRESS' if r<rr[0] or rv>vr[1] else 'HEADWIND' if r<percentile(returns,.25) else 'NEUTRAL'

def repricing(value,event_history,day_history):
    er,dr=normal(event_history),normal(day_history)
    if value is None or not er or not dr:return None
    if value<er[0]:return 'NEGATIVE'
    if value>dr[1]:return 'HEAVY'
    if value>er[1]:return 'SUBSTANTIAL'
    return 'EARLY' if value>percentile(event_history,.75) else 'UNPRICED'

def _time(row):
    for k in ('minute_start','open_time','exchange_ts','ts','as_of','settlement_time','funding_time','observed_at'):
        if row.get(k) is not None:return parse_time(row[k])
    return None

def _venue(row):return row.get('venue') or str(row.get('instrument_id','')).split(':')[0]
def _market(row):return row.get('market_type') or ('SPOT' if ':spot:' in row.get('instrument_id','') else 'PERP_LINEAR')
def _sum(rows,key):
    vals=[number(r.get(key)) for r in rows]
    return sum(vals) if vals and None not in vals else None

def _composite(venues):
    v=sum(x['volume_native'] for x in venues); cvd=sum(x['cvd'] for x in venues); q=sum(x['sum_sq_qty'] for x in venues)
    sign=1 if cvd>0 and cvd>=2*sqrt(q) else -1 if cvd<0 and cvd<=-2*sqrt(q) else 0
    return dict(cvd=cvd,volume=v,buy_share=ratio(v+cvd,2*v),bs_ratio=ratio(v+cvd,v-cvd),venues=[x['venue'] for x in venues],sign=sign)

def spot_truth(rows,tickers,instruments,start,end):
    grouped=defaultdict(list)
    for r in rows:
        if _market(r)=='SPOT' and start<=_time(r)<end:grouped[_venue(r)].append(r)
    registered={_venue(i) for i in instruments if _market(i)=='SPOT'} | set(grouped)
    venues=[];excluded=[]
    expected=int((end-start).total_seconds()/60)
    for venue in sorted(registered):
        rs=grouped[venue]; minutes={_time(r).replace(second=0,microsecond=0) for r in rs}
        if len(minutes)<expected or not all(r.get('complete') is True for r in rs):
            excluded.append(dict(venue=venue,reason='INCOMPLETE'));continue
        b,s,q=(_sum(rs,k) for k in ('buy_qty','sell_qty','sum_sq_qty'))
        if None in (b,s,q):excluded.append(dict(venue=venue,reason='INCOMPLETE'));continue
        v=b+s;c=b-s; sign=1 if c>0 and c>=2*sqrt(q) else -1 if c<0 and c<=-2*sqrt(q) else 0
        venues.append(dict(venue=venue,complete=True,volume_native=v,buy_qty=b,sell_qty=s,cvd=c,buy_share=ratio(b,v),bs_ratio=ratio(b,s),sign=sign,sum_sq_qty=q))
    c=_composite(venues);v=c['volume'];sign=c['sign'];estimated=[]
    for ex in excluded:
        ts=[r for r in tickers if _venue(r)==ex['venue'] and _time(r) and _time(r)<=end]
        t=max(ts,key=_time) if ts else {}; n=number(t.get('volume_native',t.get('volume')))
        estimated.append(n*(end-start).total_seconds()/86400 if n is not None else None)
    covered=ratio(v,v+sum(estimated)) if None not in estimated else None
    cwb={name:ratio(sum(x['volume_native'] for x in venues if x['sign']==s),v) for name,s in [('buy',1),('sell',-1),('neutral',0)]}
    ranked=sorted(venues,key=lambda x:x['volume_native'],reverse=True);top=[];vol=0
    for x in ranked:
        top.append(x);vol+=x['volume_native']
        if vol>v/2:break
    contributions=[(max(0,sign*x['cvd']),x) for x in venues];total=sum(z[0] for z in contributions)
    tc=max(contributions,key=lambda x:x[0])[1] if contributions else None
    others=[x for x in venues if x is not tc];other_v=sum(x['volume_native'] for x in others)
    other_dir=ratio(sum(x['volume_native'] for x in others if x['sign']==sign),other_v)
    top_share=ratio(max((z[0] for z in contributions),default=0),total)
    direct=cwb[{1:'buy',-1:'sell',0:'neutral'}[sign]]
    cls='INSUFFICIENT_DATA' if len(venues)<2 or covered is None or covered<=.5 else 'CONCENTRATED' if top_share is not None and top_share>.5 and other_dir is not None and other_dir<.5 else 'BROAD' if direct is not None and direct>.5 and ranked[0]['sign']==sign else 'DIVERGENT'
    for x in venues:x['volume_share']=ratio(x['volume_native'],v)
    return dict(venues=venues,composite_max=c,composite_cohort=None,breadth=dict(n_pos=sum(x['sign']==1 for x in venues),n_neg=sum(x['sign']==-1 for x in venues),n_neutral=sum(x['sign']==0 for x in venues),pct_pos=ratio(sum(x['sign']==1 for x in venues),len(venues)),pct_neg=ratio(sum(x['sign']==-1 for x in venues),len(venues))),cw_breadth=cwb,topn=dict(n=len(top),venues=[x['venue'] for x in top],agreement=ratio(sum(x['sign']==sign for x in top),len(top))),concentration=dict(hhi=sum((ratio(x['volume_native'],v) or 0)**2 for x in venues) if v else None,top_volume_venue=ranked[0]['venue'] if ranked else None,top_volume_share=ratio(ranked[0]['volume_native'],v) if ranked else None,top_contrib_venue=tc['venue'] if tc else None,top_contrib_share=top_share,others_cwb_dir=other_dir),direction={1:'BUY',-1:'SELL',0:'NEUTRAL'}[sign] if venues else None,classification=cls,coverage=dict(covered_share_est=covered,n_complete=len(venues),excluded=excluded),cohort_consistent={},size_profile=None)

def _oi_stats(rows,start,end):
    def nearest(t):
        eligible=[r for r in rows if _time(r)<=end and abs((_time(r)-t).total_seconds())<=60]
        return min(eligible,key=lambda r:abs((_time(r)-t).total_seconds())) if eligible else None
    a,z=nearest(start),nearest(end)
    if not a or not z:return None
    n0,n1,u0,u1=(number(r.get(k)) for r,k in [(a,'oi_native'),(z,'oi_native'),(a,'oi_usd'),(z,'oi_usd')])
    if None in (n0,n1,u0,u1) or not n0 or not n1 or not u0:return None
    dec=decomposition(n0,n1,u0/n0,u1/n1);length=end-start
    bytime={_time(r):r for r in rows if _time(r)<start};hn=[];hu=[]
    if bytime and (max(bytime)-min(bytime)).total_seconds()>=7*86400:
        for t,r in bytime.items():
            old=bytime.get(t-length)
            if old:
                nn,oo=number(r.get('oi_native')),number(old.get('oi_native'));uu,ou=number(r.get('oi_usd')),number(old.get('oi_usd'))
                if nn is not None and oo:hn.append(nn/oo-1)
                if uu is not None and ou:hu.append(uu/ou-1)
    dn,du=n1/n0-1,u1/u0-1;nr,ur=normal(hn),normal(hu)
    return dict(native_oi_vs_normal=('ABOVE' if dn>nr[1] else 'BELOW' if dn<nr[0] else 'WITHIN') if nr else None,oi_native_start=n0,oi_native_end=n1,oi_native_chg_pct=dn,oi_usd_chg_pct=du,decomposition=dec,oi_class=oi_class(dn,du,dec,nr,ur),native_normal_range=nr,usd_normal_range=ur,native_beyond=beyond(dn,nr),usd_beyond=beyond(du,ur),start_observed_at=iso(_time(a)),end_observed_at=iso(_time(z)))

def derivatives_truth(snapshot,start,end):
    groups=defaultdict(list)
    for r in snapshot.get('oi',[]):groups[r['instrument_id']].append(r)
    sem={s['metric_key']:s for s in snapshot.get('semantics',[])};per=[];sync=[]
    for instrument,rs in groups.items():
        stats=_oi_stats(rs,start,end);venue=_venue(rs[0])
        if stats is None:stats={'oi_class':None,'reason':'INCOMPLETE'}
        fs=[f for f in snapshot.get('funding',[]) if f.get('instrument_id')==instrument and _time(f) and _time(f)<=end]
        settled=sorted([f for f in fs if f.get('kind')=='SETTLED'],key=_time);est=sorted([f for f in fs if f.get('kind')=='CURRENT_ESTIMATE'],key=_time)
        last=settled[-1] if settled else {};interval=number(last.get('interval_h'));rate=number(last.get('rate'));eq=rate*8/interval if rate is not None and interval else None
        fh=[number(f.get('rate_8h_eq')) if f.get('rate_8h_eq') is not None else number(f.get('rate'))*8/number(f['interval_h']) for f in settled if _time(f)<start and f.get('interval_h') and f.get('rate') is not None]
        fr=normal(fh) if settled and (_time(settled[-1])-_time(settled[0])).total_seconds()>=7*86400 else None
        liqs=[r for r in snapshot.get('liquidations',[]) if r.get('instrument_id')==instrument and start<=_time(r)<end]
        cov=rs[-1].get('liq_coverage') or ('COMPLETE' if venue=='bybit' else 'SAMPLED' if venue in ('binance','okx') else 'NOT_SUPPORTED')
        p=dict(venue=venue,instrument_id=instrument,**stats,funding_settled_last=rate,funding_estimate=number(est[-1].get('rate')) if est else None,interval_h=interval,funding_8h_eq=eq,funding_vs_normal=('BELOW' if eq<fr[0] else 'ABOVE' if eq>fr[1] else 'WITHIN') if eq is not None and fr else None,funding_start=next((number(f['rate']) for f in reversed(settled) if _time(f)<=start),None),liq_long_usd=sum(number(r['notional_usd']) for r in liqs if r.get('side')=='LONG_LIQ') if liqs else None,liq_short_usd=sum(number(r['notional_usd']) for r in liqs if r.get('side')=='SHORT_LIQ') if liqs else None,liq_coverage=cov)
        lastoi=max(rs,key=_time);mark=number(lastoi.get('mark_price',lastoi.get('mark')));index=number(lastoi.get('index_price',lastoi.get('index')));p['basis_bps']=(mark/index-1)*10000 if mark is not None and index else None
        per.append(p)
        semantics=[sem.get(r.get('metric_key'),{}) for r in rs if start<=_time(r)<=end]
        if venue in ('binance','okx','bybit') and semantics and all(s.get('semantic_confidence')=='VERIFIED' and s.get('sidedness')=='SINGLE_SIDED' for s in semantics) and stats.get('oi_native_start') is not None:sync.append((p,rs,semantics[0].get('aggregation_class')))
    synchronized=None
    if sync and len({s[2] for s in sync})==1 and sync[0][2] is not None:
        timestamps=sorted({_time(r) for _,rs,_ in sync for r in rs});combined=[]
        for t in timestamps:
            selected=[]
            for _,rs,_ in sync:
                candidates=[r for r in rs if abs((_time(r)-t).total_seconds())<=60 and _time(r)<=end]
                if not candidates:break
                selected.append(min(candidates,key=lambda r:abs((_time(r)-t).total_seconds())))
            if len(selected)==len(sync):combined.append(dict(ts=iso(t),oi_native=sum(number(r['oi_native']) for r in selected),oi_usd=sum(number(r['oi_usd']) for r in selected)))
        synchronized=_oi_stats(combined,start,end)
        if synchronized:synchronized['venues']=[p['venue'] for p,_,_ in sync]
    eqs=[p['funding_8h_eq'] for p in per if p['funding_8h_eq'] is not None]
    totals={}
    for key,cov in [('complete_venues_total','COMPLETE'),('sampled_venues_min','SAMPLED')]:
        eligible=[p for p in per if p['liq_coverage']==cov]
        totals[key]={side:sum(p['liq_'+side+'_usd'] for p in eligible) if eligible and all(p['liq_'+side+'_usd'] is not None for p in eligible) else None for side in ('long','short')}
    volumes={}
    for market in ('SPOT','PERP'):
        bars=[r for r in snapshot.get('klines',[]) if (_market(r)=='SPOT')==(market=='SPOT') and r.get('complete') is True]
        current=[r for r in bars if start<=_time(r)<end];previous=[r for r in bars if start-(end-start)<=_time(r)<start]
        volumes[market]=max(0,_sum(current,'quote_volume')-_sum(previous,'quote_volume')) if current and previous and _sum(current,'quote_volume') is not None and _sum(previous,'quote_volume') is not None else None
    inc={k.lower():ratio(v,sum(volumes.values())) for k,v in volumes.items()} if None not in volumes.values() and sum(volumes.values()) else None
    return dict(per_venue=per,synchronized=synchronized,funding_dispersion_8h=max(eqs)-min(eqs) if len(eqs)>1 else None,liquidations=totals,incremental_share=inc,lead_lag=dict(first_mover='UNKNOWN',spot_confirmed=None,sequence=[]))

def _prices(snapshot,token=None):
    groups=defaultdict(dict)
    for r in snapshot.get('klines',[]):
        if r.get('complete') is not True or _market(r)!='SPOT':continue
        if token and r.get('token_id')!=token and token not in r.get('instrument_id','').upper():continue
        t=parse_time(r['close_time']) if r.get('close_time') else _time(r)+timedelta(seconds=int(r.get('interval_s',60)));price=number(r.get('c',r.get('close')))
        if price and t<=parse_time(snapshot['cutoff']):groups[r['instrument_id']][t]=price
    return groups

def relative_truth(snapshot,start,end):
    groups=_prices(snapshot);token=snapshot.get('token_id','').upper();candidates={k:v for k,v in groups.items() if token in k.upper() or not token}
    if not candidates:candidates={}
    volumes={k:sum(number(r.get('quote_volume')) or 0 for r in snapshot.get('klines',[]) if r.get('instrument_id')==k and end-timedelta(days=1)<=_time(r)<end) for k in candidates}
    ref=max(candidates,key=lambda k:volumes[k]) if candidates else None;prices=candidates.get(ref,{})
    def series(symbol):
        matches={k:v for k,v in groups.items() if symbol in k.upper()}
        return max(matches.values(),key=len) if matches else {}
    btc=series('BTC');eth=series('ETH');history_end=start
    def hourly(p):
        return {t:log(p[t]/p[t-timedelta(hours=1)]) for t in p if t<history_end and t-timedelta(hours=1) in p and t>=history_end-timedelta(days=90)}
    bb,bflag=beta(hourly(prices),hourly(btc));be,eflag=beta(hourly(prices),hourly(eth))
    def price(p,t):
        ts=[x for x in p if x<=t]
        return p[max(ts)] if ts else None
    def ret(p,a,z):
        pa,pz=price(p,a),price(p,z)
        return pz/pa-1 if pa and pz else None
    def hist(length):
        if not prices or (max(prices)-min(prices)).total_seconds()<7*86400:return []
        out=[]
        for t in prices:
            a=t-timedelta(seconds=length)
            if t>=start or a not in prices or t not in btc or a not in btc:continue
            out.append(prices[t]/prices[a]-1-bb*(btc[t]/btc[a]-1))
        return out
    windows={}
    for name,length in dict(WINDOWS,EVENT=(end-start).total_seconds()).items():
        a=end-timedelta(seconds=length);r=ret(prices,a,end);rb=ret(btc,a,end);re=ret(eth,a,end);adj=r-bb*rb if r is not None and rb is not None else None;h=hist(length)
        windows[name]=dict(r_raw=r,r_btc_adj=adj,r_eth_adj=r-be*re if r is not None and re is not None else None,r_sector_adj=None,sector_reason='NOT_ESTIMABLE',pctile=ratio(sum(x<=adj for x in h),len(h)) if adj is not None else None,normal_range=normal(h),beyond_normal=beyond(adj,normal(h)))
    eh=hist((end-start).total_seconds());dh=hist(86400);sign=-1 if snapshot.get('expected_direction') in ('BEARISH','LOSER',-1) else 1;eventr=windows['EVENT']['r_btc_adj']
    br=ret(btc,end-timedelta(days=1),end);bh=[];rvhist=[];rv=None
    for t in sorted(btc):
        if t>=start:continue
        a=t-timedelta(days=1)
        if a in btc:bh.append(btc[t]/btc[a]-1)
        lr=[log(btc[x]/btc[x-timedelta(hours=1)]) for x in btc if a<x<=t and x-timedelta(hours=1) in btc]
        if len(lr)==24:rvhist.append(stdev(lr))
    lr=[log(btc[t]/btc[t-timedelta(hours=1)]) for t in btc if end-timedelta(days=1)<t<=end and t-timedelta(hours=1) in btc]
    if len(lr)==24:rv=stdev(lr)
    if not btc or (max(btc)-min(btc)).total_seconds()<7*86400:bh=[];rvhist=[]
    context=market_context(br,rv,bh,rvhist)
    pre=ret(prices,start-timedelta(days=1),start);preb=ret(btc,start-timedelta(days=1),start);ar=pre-bb*preb if pre is not None and preb is not None else None
    return dict(ref_venue=ref,anchor=dict(type=snapshot.get('anchor',{}).get('type','KNOWN_TO_US_AT') if isinstance(snapshot.get('anchor'),dict) else 'KNOWN_TO_US_AT',ts=iso(start),ref_price=price(prices,start-timedelta(microseconds=1))),windows=windows,beta_btc=bb,beta_eth=be,beta_flags=dict(btc=bflag,eth=eflag),pre_event_runup=dict(ar=ar,pctile=ratio(sum(x<=ar for x in dh),len(dh)) if ar is not None else None,flag=beyond(ar,normal(dh))),repricing_band=repricing(eventr*sign if eventr is not None else None,[x*sign for x in eh],[x*sign for x in dh]),market_context=context,context_inputs=dict(btc_return_24h=br,rv24=rv,return_normal_range=normal(bh),return_lower_quartile=percentile(bh,.25),rv_normal_range=normal(rvhist),rule='normal_range',known_rule_mismatch='F-16: baseline yields STRESS; fixture expects HEADWIND'))

def walk_book(levels,notional,mid,side='ask'):
    remaining=notional;native=0.;spent=0.
    for level in levels:
        p,q=(number(level[k]) for k in ('price','qty')) if isinstance(level,dict) else map(number,level[:2])
        take=min(remaining,p*q);native+=take/p;spent+=take;remaining-=take
        if remaining<=1e-8:break
    return ((spent/native/mid)-1)*10000 if native and remaining<=1e-8 else None

def liquidity_truth(snapshot,start,end,spot,relative):
    books=[b for b in snapshot.get('books',[]) if _time(b)<=end and b.get('integrity_ok') is True];groups=defaultdict(list)
    for b in books:groups[_venue(b)].append(b)
    flow=[r for r in snapshot.get('flow_bars',[]) if _market(r)=='SPOT' and start<=_time(r)<end and r.get('complete') is True]
    v15=[r for r in snapshot.get('flow_bars',[]) if _market(r)=='SPOT' and end-timedelta(minutes=15)<=_time(r)<end and r.get('complete') is True]
    v=_sum(v15,'buy_notional');sv=_sum(v15,'sell_notional');k=min(1,(v+sv)/5e6) if v is not None and sv is not None else None
    tiers={name:size*k if k is not None else None for name,size in [('T1',1e5),('T2',1e6),('T3',5e6)]};per=[];predepth=[];nowdepth=[];spreads=[];prespreads=[];burst_results=[]
    def depth(b,side):
        covered=b.get('range_covered_pct',{});coverage=number(covered.get(side)) if isinstance(covered,dict) else number(covered)
        return number(b.get('depth',{}).get(side,{}).get('p1',{}).get('usd')) if coverage is not None and coverage>=1 else None
    for venue,rs in groups.items():
        rs.sort(key=_time);b=rs[-1];bid,ask=depth(b,'bid'),depth(b,'ask');d=bid+ask if bid is not None and ask is not None else None
        pre=[x for x in rs if start-timedelta(hours=1)<=_time(x)<start];pd=[depth(x,'bid')+depth(x,'ask') for x in pre if depth(x,'bid') is not None and depth(x,'ask') is not None]
        if pd:predepth.append(mean(pd))
        if d is not None:nowdepth.append(d)
        spread=number(b.get('spread_bps'));spreads.append(spread) if spread is not None else None
        ps=[number(x['spread_bps']) for x in pre if x.get('spread_bps') is not None]
        if ps:prespreads.append(mean(ps))
        mid=number(b.get('mid'));levels=b.get('asks',b.get('levels',{}).get('asks',[]))
        per.append(dict(venue=venue,spread_bps=spread,depth_p1_usd=d,range_covered_pct=b.get('range_covered_pct'),tier_notional=tiers,slippage_bps={name:walk_book(levels,n,mid) if n and mid and levels else None for name,n in tiers.items()}))
        for f in flow:
            if _venue(f)!=venue:continue
            t=_time(f);before=[x for x in rs if t-timedelta(seconds=10)<=_time(x)<t]
            if not before:continue
            baseline=before[-1];bd=depth(baseline,'bid');net=(number(f.get('sell_notional')) or 0)-(number(f.get('buy_notional')) or 0)
            if bd is None or net<bd:continue
            after=[x for x in rs if t<_time(x)<=min(t+timedelta(seconds=60),end) and depth(x,'bid') is not None and depth(x,'bid')>=bd]
            recovered=bool(after);future=[number(x['low']) for x in flow if _venue(x)==venue and t<_time(x)<=t+timedelta(minutes=15) and x.get('low') is not None];low=number(f.get('low'));resilient=(min(future)>=low) if future and low is not None and t+timedelta(minutes=15)<=end else None
            burst_results.append(dict(venue=venue,ts=iso(t),replenished=recovered if t+timedelta(seconds=60)<=end else None,recovery_s=(_time(after[0])-t).total_seconds() if after else None,resilient=resilient))
    aggregate=sum(nowdepth) if nowdepth and len(nowdepth)==len(groups) else None;pre=sum(predepth) if predepth and len(predepth)==len(groups) else None;drop=1-aggregate/pre if aggregate is not None and pre else None
    bn,sn=_sum(flow,'buy_notional'),_sum(flow,'sell_notional');net=bn-sn if bn is not None and sn is not None else None;raw=relative['windows'].get(snapshot.get('_primary','W4H'),{}).get('r_raw');beyondprice=relative['windows'].get(snapshot.get('_primary','W4H'),{}).get('beyond_normal');avg=mean(nowdepth+predepth) if nowdepth and predepth else None
    weak=(abs(net)>=aggregate/2 and (raw if net>0 else -raw)<.01) if net and aggregate is not None and raw is not None else None;excess=bool(beyondprice and abs(net)<pre/2) if beyondprice is not None and net is not None and pre is not None else None
    vacuum=bool(beyondprice and drop>.5 and excess) if drop is not None and excess is not None and beyondprice is not None else None
    return dict(per_venue=per,aggregate_depth_p1_usd=aggregate,pre_event_depth_p1_usd=pre,depth_drop=drop,spread_ratio=ratio(mean(spreads),mean(prespreads)) if spreads and prespreads else None,replenishment=dict(available=bool(books),bursts=len(burst_results) if books else None,replenished=sum(x['replenished'] is True for x in burst_results) if books else None,resilient_bursts=sum(x['replenished'] is True and x['resilient'] is True for x in burst_results) if books else None,median_recovery_s=median([x['recovery_s'] for x in burst_results if x['recovery_s'] is not None]) if any(x['recovery_s'] is not None for x in burst_results) else None,observations=burst_results),elasticity=dict(bps_per_depth_unit=raw*10000/(abs(net)/avg) if raw is not None and net and avg else None,weak_response=weak,excess_response=excess),vol_compression=dict(rv24=None,normal_range=None,state=None,reason='INCOMPLETE'),liquidity_state='VACUUM' if vacuum else ('THIN' if aggregate<tiers['T2'] else 'NORMAL') if aggregate is not None and tiers['T2'] is not None else None)

def supply_truth(snapshot,start,end):
    rows=[r for r in snapshot.get('supply',[]) if _time(r) is not None and _time(r)<=end];flows=[];unlocks=[];inventory=None;lending=None;stable=None;available=False;evidence=False
    for r in rows:
        kind=r.get('kind',r.get('type'))
        if kind in ('FLOW','WALLET_FLOW') and start<=_time(r):
            f=dict(r);f['class']=r.get('class','AMBIGUOUS');flows.append(f);available=True
            if f['class'] in ('UNLOCK_DISTRIBUTION','TREASURY_INTERNAL','MM_INVENTORY') and r.get('to_exchange') is True:evidence=True
        elif kind in ('INVENTORY','EXCHANGE_INVENTORY'):
            inventory=r;available=True
            if beyond(number(r.get('chg_pct')),r.get('normal_range')) is True and number(r.get('chg_pct'))>0:evidence=True
        elif kind=='UNLOCK' and end<=parse_time(r.get('date',r.get('ts')))<=end+timedelta(days=14):unlocks.append(r);available=True
        elif kind=='LENDING':lending=r;available=True
        elif kind=='STABLECOIN_CHAIN':stable=r;available=True
    return dict(exchange_inventory=inventory or missing(),flows=flows,unlocks_next_14d=unlocks,lending=lending or missing(),stablecoin_chain=stable or missing(),supply_in_evidence='TRUE' if evidence else 'FALSE' if available else 'UNKNOWN')

def data_confidence(snapshot,spot,derivatives,end):
    failed=[];sem=snapshot.get('semantics',[]);critical=[s for s in sem if s.get('critical')];low=any(s.get('semantic_confidence') in ('INFERRED','UNKNOWN') for s in critical)
    verified=bool(critical) and all(s.get('semantic_confidence')=='VERIFIED' for s in critical)
    # Semantics must cover observed critical families; a single verified row cannot certify a snapshot.
    names=' '.join(s.get('metric_key','').lower() for s in critical)
    families=['aggressor','depth','price']+(['oi','funding'] if derivatives['per_venue'] else [])
    if any(f not in names for f in families):verified=False
    if not verified:failed.append(dict(condition='semantics',reason='UNVERIFIED_SEMANTICS'))
    cov=spot['coverage'];low=low or cov['n_complete']<2
    if cov['covered_share_est'] is None or cov['covered_share_est']<=.5 or cov['n_complete']<2:failed.append(dict(condition='spot_coverage',reason='INCOMPLETE'))
    if derivatives['per_venue']:
        synced=derivatives['synchronized'];synvenues=set(synced['venues']) if synced else set();perpvol=defaultdict(float)
        for r in snapshot.get('klines',[]):
            if _market(r)!='SPOT' and end-timedelta(hours=24)<=_time(r)<end and r.get('quote_volume') is not None:perpvol[_venue(r)]+=number(r['quote_volume'])
        if not synced or not perpvol or sum(v for k,v in perpvol.items() if k in synvenues)<=sum(perpvol.values())/2:failed.append(dict(condition='derivatives_coverage',reason='INCOMPLETE'))
    cadences={'flow_bars':60,'klines':60,'books':10,'oi':300,'funding':300,'tickers':300,'supply':21600}
    for collection,cadence in cadences.items():
        rows=snapshot.get(collection,[])
        if not rows:continue
        groups=defaultdict(list)
        for r in rows:
            if _time(r) is not None:groups[r.get('instrument_id',r.get('kind','all'))].append(_time(r))
        if any((end-max(ts)).total_seconds()>cadence for ts in groups.values()):failed.append(dict(condition='freshness',reason='INCOMPLETE',collection=collection))
    return dict(class_='LOW' if low else 'MEDIUM' if failed else 'HIGH',failed=failed,limiting=failed[0] if failed else None,critical_semantics_verified=verified)

def select_move(spot,deriv,liq,supply,relative,confidence,window):
    p=relative['windows'][window];r=p['r_btc_adj'];rng=p['normal_range'];up=r is not None and rng is not None and r>rng[1];notdown=r is not None and rng is not None and r>=rng[0];moved=beyond(r,rng) is True
    oi=deriv['synchronized'] or {};dn=oi.get('oi_native_chg_pct');nr=oi.get('native_normal_range');nb=beyond(dn,nr);nup=nb is True and dn>0;ndown=nb is True and dn<0;nwithin=within(dn,nr) is True
    funding=[x.get('funding_vs_normal') for x in deriv['per_venue']];fundok=bool(funding) and all(x in ('WITHIN','BELOW') for x in funding);fs=[x.get('funding_start') for x in deriv['per_venue']];fundnegative=bool(fs) and None not in fs and all(x<=0 for x in fs)
    inc=deriv.get('incremental_share') or {};perpmost=(inc.get('perp') or 0)>.5;spotmost=(inc.get('spot') or 0)>.5;lead=deriv['lead_lag']['first_mover'];weak=liq['elasticity']['weak_response'];excess=liq['elasticity']['excess_response'];drop=liq['depth_drop'];direction=spot['direction'];broad=spot['classification']=='BROAD';supplyyes=supply['supply_in_evidence']=='TRUE';liqs=deriv['liquidations']['complete_venues_total'];shortmore=liqs['short'] is not None and liqs['long'] is not None and liqs['short']>liqs['long']
    eligible={};divergence=dict(state='NONE',triggered=False,unresolved=[],confirmed=False)
    if direction=='BUY' and broad and notdown and weak is False and nwithin and fundok:eligible['SPOT_LED_ACCUMULATION']=sum([lead=='SPOT',spotmost,drop is not None and drop<=0])
    if direction=='BUY' and spot['classification'] in ('BROAD','CONCENTRATED') and dn is not None and dn>0 and nwithin and oi.get('oi_class') in ('REAL_NATIVE','MIXED') and fundok:eligible['ORGANIC_MIXED']=0
    if up and nup and (perpmost or lead=='PERP') and not(direction=='BUY' and broad):eligible['PERP_LED_CHASE']=sum([any(x=='ABOVE' for x in funding),direction in ('SELL','NEUTRAL')])
    if up and (ndown or shortmore) and fundnegative:eligible['SHORT_SQUEEZE']=0
    if direction=='BUY' and weak is True:eligible['SELL_SIDE_ABSORPTION']=int(supplyyes)+int(nb is False)
    if notdown and direction=='SELL' and (spot['cw_breadth']['sell'] or 0)>.5 and supplyyes:eligible['DISTRIBUTION']=0
    if moved and drop is not None and drop>.5 and excess is True:eligible['LIQUIDITY_VACUUM']=0
    if not eligible and oi.get('oi_class')=='PRICE_MARKUP' and oi.get('oi_usd_chg_pct',0)>0 and oi.get('usd_beyond') is True:eligible['PRICE_MARKUP_FAKE_OI']=0
    if notdown and direction=='SELL':
        divergence['triggered']=True;candidates={};refill=liq['replenishment'];bursts=refill['bursts'];rep=refill['replenished'];res=refill['resilient_bursts']
        if nup or perpmost:candidates['PERP_LED_MARKUP']=int(lead=='PERP')+int(not broad)+int(any(x=='ABOVE' for x in funding))
        refuted=(bursts is not None and bursts>=2 and rep is not None and rep<bursts/2) or (drop is not None and drop>.5)
        if not refuted:
            confirmed=bursts is not None and bursts>=2 and rep==bursts and res==bursts and nb is False
            candidates['ABSORPTION_CONFIRMED' if confirmed else 'ABSORPTION_CANDIDATE']=int(liq['spread_ratio'] is not None and liq['spread_ratio']<=1)+int(nb is False)
        if (drop is not None and drop>.5) or excess is True:candidates['LIQUIDITY_VACUUM']=int(liq['liquidity_state']=='THIN')
        if candidates:
            chosen=max(candidates,key=lambda x:(candidates[x], {'LIQUIDITY_VACUUM':4,'PERP_LED_MARKUP':3,'ABSORPTION_CONFIRMED':2,'ABSORPTION_CANDIDATE':1}[x]));divergence.update(state=chosen,unresolved=[x for x in candidates if x!=chosen],confirmed=chosen=='ABSORPTION_CONFIRMED');move={'PERP_LED_MARKUP':'PERP_LED_CHASE','LIQUIDITY_VACUUM':'LIQUIDITY_VACUUM','ABSORPTION_CONFIRMED':'PASSIVE_SPOT_ABSORPTION','ABSORPTION_CANDIDATE':'PASSIVE_SPOT_ABSORPTION'}[chosen];eligible={move:candidates[chosen]}
        else:eligible={};divergence['state']='UNKNOWN'
    ordered=sorted(eligible,key=lambda x:(-eligible[x],RISK_ORDER.index(x) if x in RISK_ORDER else 99));move=ordered[0] if ordered else 'UNKNOWN'
    if confidence=='LOW':move='UNKNOWN';divergence['state']='UNKNOWN' if divergence['triggered'] else 'NONE'
    return dict(move_class=move,support=eligible.get(move,0),alternatives=ordered[1:],divergence=divergence,class_confidence='MEDIUM' if len(ordered)>1 or divergence['unresolved'] else confidence,flags=['PRICE_MARKUP_OI'] if oi.get('oi_class')=='PRICE_MARKUP' and oi.get('oi_usd_chg_pct',0)>0 else [])

def compute_market_truth(snapshot,*,config=None):
    config=config or {};snapshot=dict(snapshot);end=parse_time(snapshot['cutoff']);anchor=snapshot.get('anchor');av=anchor.get('ts') if isinstance(anchor,dict) else anchor
    start=parse_time(av) if av else end-timedelta(hours=4)
    if start>=end:raise ValueError('anchor must precede cutoff')
    # Enforce both exchange and receipt availability to prevent replay leakage.
    for name in ('flow_bars','klines','books','oi','funding','liquidations','tickers','supply'):
        snapshot[name]=[r for r in snapshot.get(name,[]) if _time(r) is not None and _time(r)<=end and (name!='klines' or _time(r)+timedelta(seconds=int(r.get('interval_s',60)))<=end) and (not r.get('received_ts') or parse_time(r['received_ts'])<=end)]
    primary='EVENT' if av and 900<=(end-start).total_seconds()<=86400 else 'W4H';snapshot['_primary']=primary
    windows=dict(WINDOWS,EVENT=(end-start).total_seconds());spots={name:spot_truth(snapshot['flow_bars'],snapshot['tickers'],snapshot.get('instruments',[]),end-timedelta(seconds=seconds),end) for name,seconds in windows.items()}
    cohort=set.intersection(*(set(v['venue'] for v in s['venues']) for s in spots.values()))
    for name,s in spots.items():
        selected=[v for v in s['venues'] if v['venue'] in cohort];c=_composite(selected);share=ratio(c['volume'],s['composite_max']['volume']);valid=len(cohort)>=2 and share is not None and share>.5
        c.update(cohort=sorted(cohort),share_of_max=share,velocity=None,reason=None if valid else 'NOT_COHORT_CONSISTENT');s['composite_cohort']=c if valid else None;s['cohort_consistent']={other:valid for other in spots if other!=name}
        prev=spot_truth(snapshot['flow_bars'],snapshot['tickers'],snapshot.get('instruments',[]),end-timedelta(seconds=2*windows[name]),end-timedelta(seconds=windows[name]));pv=[v for v in prev['venues'] if v['venue'] in cohort]
        if valid and len(pv)==len(cohort):c['velocity']=ratio(c['volume'],sum(v['volume_native'] for v in pv))
    pstart=end-timedelta(seconds=windows[primary]);deriv=derivatives_truth(snapshot,pstart,end);relative=relative_truth(snapshot,start,end);liquidity=liquidity_truth(snapshot,pstart,end,spots[primary],relative);supply=supply_truth(snapshot,pstart,end);conf=data_confidence(snapshot,spots[primary],deriv,end);conf['class']=conf.pop('class_')
    selected=select_move(spots[primary],deriv,liquidity,supply,relative,conf['class'],primary)
    return dict(token_id=snapshot.get('token_id'),event_id=snapshot.get('event_id'),cutoff=iso(end),primary_window=primary,spot=spots,derivatives=deriv,liquidity=liquidity,supply=supply,relative=relative,data_confidence=conf,**selected,config_version=config.get('config_version',snapshot.get('config_version','unversioned')),code_version=config.get('code_version',snapshot.get('code_version','unversioned')))

class MarketTruthService:
    def __init__(self,repo,settings,market):self.repo,self.settings,self.market=repo,settings,market
    async def assess(self,event_id,token_id,cutoff=None):
        event=await self.repo.get('evt.events',event_id) if event_id else None;event=event or {};cutoff=cutoff or iso();anchor=None
        for key,kind in [('t0','T0'),('t1','T1'),('known_to_us_at','KNOWN_TO_US_AT')]:
            value=event.get(key)
            if isinstance(value,dict):value=value.get('value',value.get('ts'))
            if value and (key!='t0' or event.get('t0_verified') is True):anchor={'ts':value,'type':kind};break
        snapshot=await self.market.snapshot(token_id,cutoff,anchor=anchor);snapshot.update(event_id=event_id,token_id=token_id)
        result=compute_market_truth(snapshot,config={'config_version':self.settings.config_version,'code_version':self.settings.code_version});result['mta_id']=new_id('MTA');await self.repo.put('ana.market_truth',result['mta_id'],result,immutable=True);return result
