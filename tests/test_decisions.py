from copy import deepcopy
from tank.analysis.decisions import DecisionEngine
from tank.analysis.cards import alert_gate, lint_card, compose_card


def inputs():
    return ({'event_id':'e','event_state':'APPROVED','state_verified':True,'freshness':'FRESH'}, {'class':'MAJOR','certainty':'HIGH','impact_horizon':{'impact_start':'NOW'}}, {'token_id':'T','role':'PRIMARY','credible':True,'capture_class':'DIRECT'}, {'token_id':'T','move_class':'SPOT_LED_ACCUMULATION','data_confidence':{'class':'HIGH','critical_semantics_verified':True},'relative':{'repricing_band':'EARLY','market_context':'NEUTRAL','anchor':{'ref_price':'10'}},'liquidity':{'liquidity_state':'NORMAL'},'spot':{},'derivatives':{}})


def test_start_and_ordered_blockers():
    e,a,b,m=inputs(); engine=DecisionEngine()
    assert engine.evaluate(e,a,b,m)['action']=='START'
    cases=[({'class':'LOW'},'WAIT'),({'class':'MEDIUM'},'WATCH_TO_START'),({'class':'HIGH','critical_semantics_verified':False},'WAIT')]
    for patch,expected in cases:
        n=deepcopy(m); n['data_confidence'].update(patch)
        assert engine.evaluate(e,a,b,n)['action']==expected
    for kind,expected in [('FATAL','WAIT'),('UNRESOLVED','WATCH_TO_START')]:
        n=deepcopy(e); n['contradictions']=[{'type':kind}]
        assert engine.evaluate(n,a,b,m)['action']==expected
    b['role']='LOSER'; assert engine.evaluate(e,a,b,m)['action']=='WAIT'


def test_risk_classes_and_absorption():
    e,a,b,m=inputs(); engine=DecisionEngine()
    for cls,action in [('PERP_LED_CHASE','DONT_CHASE'),('SHORT_SQUEEZE','DONT_CHASE'),('LIQUIDITY_VACUUM','DONT_CHASE'),('UNKNOWN','WAIT'),('REAL_MARKET_FRAGMENTATION','WAIT'),('DISTRIBUTION','WAIT')]:
        m['move_class']=cls; assert engine.evaluate(e,a,b,m)['action']==action
    m['move_class']='PASSIVE_SPOT_ABSORPTION'; m['divergence']={'state':'ABSORPTION_CANDIDATE'}
    assert engine.evaluate(e,a,b,m)['action']=='WATCH_TO_START'
    m['divergence']['state']='ABSORPTION_CONFIRMED'; assert engine.evaluate(e,a,b,m)['action']=='START'


def test_context_lifecycle_safety():
    e,a,b,m=inputs(); engine=DecisionEngine(); call={'invalidation_price':'9'}
    m['relative']['market_context']='STRESS'; assert engine.evaluate(e,a,b,m)['trade_grade']=='B'
    a['class']='STRUCTURAL_SHOCK'; m['relative']['windows']={'EVENT':{'r_btc_adj':'1'}}
    assert engine.evaluate(e,a,b,m)['trade_grade']=='A'
    assert engine.evaluate(e,a,b,m,open_call=call)['action']=='HOLD'
    assert engine.evaluate(e,a,b,m,open_call=call,conditions=[{'status':'FIRED','role':'UPGRADE'}])['action']=='ADD'
    e['contradictions']=[{'type':'FATAL'}]; e['freshness']='STALE'; e['lifecycle']='INVALIDATED'
    d=engine.evaluate(e,a,b,m,open_call=call); assert d['action']=='EXIT'
    assert alert_gate(e,a,previous={'decisions':[{'token_id':'T','action':'START'}]},decisions=[d])[0]
    assert not alert_gate(e,a)[0]


def test_card_diagnostics_and_probability():
    e,a,b,m=inputs(); m['move_class']='PERP_LED_CHASE'; m['divergence']={'state':'PERP_LED_MARKUP','trigger':True,'unresolved':['ABSORPTION_CANDIDATE']}
    d=DecisionEngine().evaluate(e,a,b,m)
    text=compose_card(e,a,[b],[d],[m],verdict='A verified event changes the thesis.')
    assert 'ABSORPTION_CANDIDATE' in text and len(text)<=3500
    assert not any(x['rule']=='L12' for x in lint_card(text,{'event':e,'beneficiaries':[b],'market_truths':[m]}))
    assert any(x['rule']=='L7' for x in lint_card('High probability of upside.',{}))


def test_three_token_template_with_structured_figures():
    e,a,b,m=inputs(); bs=[]; ds=[]; ms=[]
    for token in ['AAA','BBB','CCC']:
        edge=dict(b,token_id=token); truth=deepcopy(m); truth['token_id']=token
        truth['decisive_facts']=[{'metric':'price','value':'3.2','unit':'%','window':'W4H'},{'metric':'native net flow','value':'12000','unit':'tokens','window':'W1H'}]
        bs.append(edge); ms.append(truth); ds.append(DecisionEngine().evaluate(e,a,edge,truth))
    text=compose_card(e,a,bs,ds,ms,verdict='The approval advances a direct token mechanism.',why=['What changed: approval verified.','Economic effect: not estimable.'])
    assert len(text)<=3500 and text.count('WHAT TO DO:')==3
