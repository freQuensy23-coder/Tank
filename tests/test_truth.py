from datetime import timedelta
import json
from pathlib import Path
import pytest
from tank.analysis.truth import decomposition,oi_class,spot_truth,compute_market_truth,repricing,market_context,walk_book,_oi_stats
from tank.util import parse_time,iso


def test_oi_formula_vectors():
    d=decomposition(100,108.18,1,1.0705)
    assert d['position']==pytest.approx(8.18)
    assert d['markup']==pytest.approx(7.05)
    assert d['interaction']==pytest.approx(.57669)
    assert d['pos_share']>.5
    assert oi_class(.0818,.1580669,d,[-.03,.035],[-.04,.05])=='REAL_NATIVE'
    d=decomposition(100,100.5,1,1.1)
    assert oi_class(.005,.1055,d,[-.03,.035],[-.04,.05])=='PRICE_MARKUP'
    assert oi_class(.005,.1055,d,None,[-.04,.05]) is None


def flow_case(values,qs):
    end=parse_time('2026-10-16T12:00:00Z');start=end-timedelta(minutes=1)
    rows=[dict(instrument_id=f'{v}:spot:T',minute_start=iso(start),buy_qty=(vol+cvd)/2,sell_qty=(vol-cvd)/2,sum_sq_qty=q,complete=True) for (v,vol,cvd),q in zip(values,qs)]
    return spot_truth(rows,[],[],start,end)


def test_t3_capital_weighted_broad_sell():
    s=flow_case([('A',600,-40),('B',250,-10),('C',100,5),('D',25,6),('E',25,3)],[1]*5)
    assert s['direction']=='SELL'
    assert s['classification']=='BROAD'
    assert s['breadth']['pct_pos']==.6
    assert s['cw_breadth']['sell']==.85
    assert s['concentration']['others_cwb_dir']==.625


def test_t4_single_venue_spike():
    s=flow_case([('A',300,60),('B',300,-2),('C',300,1),('D',100,0)],[1,4,4,4])
    assert s['classification']=='CONCENTRATED'
    assert s['composite_max']['cvd']==59
    assert s['concentration']['others_cwb_dir']==0


def test_missing_is_not_zero_and_no_leakage():
    s=compute_market_truth(dict(token_id='WLD',cutoff='2026-10-16T12:00:00Z',flow_bars=[dict(instrument_id='binance:spot:WLD',minute_start='2026-10-17T12:00:00Z',buy_qty=1,sell_qty=0,sum_sq_qty=1,complete=True)]))
    assert s['data_confidence']['class']=='LOW'
    assert s['move_class']=='UNKNOWN'
    assert s['relative']['market_context'] is None
    assert s['liquidity']['liquidity_state'] is None
    assert s['supply']['supply_in_evidence']=='UNKNOWN'
    assert s['spot']['W1H']['coverage']['n_complete']==0


def test_repricing_and_baseline_context():
    assert repricing(.23,[-.03,0,.02,.05],[-.04,0,.02,.046])=='HEAVY'
    assert market_context(-.0325,.0045,[-.0301,-.0085,0,.02],[.003,.005,.01])=='STRESS'


def test_walk_book_requires_actual_fill():
    assert walk_book([[100,1000],[101,1000]],100000,100)==0
    assert walk_book([[100,1]],100000,100) is None


def test_real_rest_fixture_endpoints():
    path=Path(__file__).resolve().parents[1]/'fixture_check/data/rest_openInterestHist_fixtures.json'
    data=json.loads(path.read_text())['data']
    cases=[('F-12:QNTUSDT:1h','2026-09-26T19:00:00Z','2026-09-26T23:00:00Z',.0042),('F-13:ONDOUSDT:1h','2026-09-24T12:00:00Z','2026-09-24T16:00:00Z',.5732),('F-14:QNTUSDT:1h','2026-09-27T19:00:00Z','2026-09-27T23:00:00Z',-.3036)]
    for key,a,z,expected in cases:
        rows=[dict(ts=iso(parse_time(r['timestamp'])),oi_native=r['sumOpenInterest'],oi_usd=r['sumOpenInterestValue']) for r in data[key]]
        result=_oi_stats(rows,parse_time(a),parse_time(z))
        assert result['oi_native_chg_pct']==pytest.approx(expected,abs=.0001)
