"""Deterministic written-rule decisions. No network or persistence."""
from decimal import Decimal
from tank.util import new_id


def _enum(value, default='UNKNOWN'):
    return str(value or default).upper().replace(' ', '_').replace('-', '_')


def _contradictions(event, assessment):
    return [c for c in event.get('contradictions', []) + assessment.get('contradictions', []) if not c.get('resolved_at')]


def _expr(metric, op, value, window='W1H', count=1):
    return {'metric': metric, 'window': window, 'op': op, 'value': value,
            'persist': {'count': count, 'unit': 'consecutive_windows'}}


def _condition(role, text, *expressions):
    return {'cond_id': new_id('CND'), 'role': role, 'human_text': text,
            'expr': expressions[0] if len(expressions) == 1 else {'all': list(expressions)}, 'status': 'ARMED'}


class DecisionEngine:
    def __init__(self, settings=None):
        self.settings = settings

    def evaluate(self, event, assessment, beneficiary, market_truth, *, open_call=None, conditions=None):
        m = market_truth
        dc = m.get('data_confidence') or {}
        data = _enum(dc.get('class'), 'LOW')
        move = _enum(m.get('move_class'))
        relative = m.get('relative') or {}
        band = _enum(relative.get('repricing_band'))
        context = _enum(relative.get('market_context'))
        liq = m.get('liquidity') or {}
        liquidity = _enum(liq.get('liquidity_state'))
        divergence = m.get('divergence') or {}
        confirmed = _enum(divergence.get('state')) in {'ABSORPTION_CONFIRMED', 'CONFIRMED'} or m.get('absorption_confirmed') is True
        spot = (m.get('spot') or {}).get(m.get('primary_window', 'W4H'), {})
        direction, breadth = _enum(spot.get('direction')), _enum(spot.get('classification'))
        selling = direction == 'SELL' and (spot.get('cw_breadth') or {}).get('sell', 0) > .5
        deriv = m.get('derivatives') or {}
        if not isinstance(deriv, dict): deriv = {}
        sync = deriv.get('synchronized') or {}
        funding_above = deriv.get('funding_vs_normal') == 'ABOVE' or any(v.get('funding_vs_normal') == 'ABOVE' for v in deriv.get('per_venue', []))
        oi_above = (sync.get('native_beyond') is True and (sync.get('oi_native_chg_pct') or 0) > 0) or sync.get('native_oi_vs_normal', sync.get('oi_native_vs_normal', deriv.get('native_oi_vs_normal'))) == 'ABOVE' or sync.get('oi_native_up_beyond_normal') is True
        crowded = funding_above and oi_above
        role = _enum(beneficiary.get('role'))
        capture = _enum(beneficiary.get('capture_class', beneficiary.get('capture', beneficiary.get('token_capture', assessment.get('capture_class', assessment.get('capture'))))))
        if isinstance(beneficiary.get('capture'), dict): capture = _enum(beneficiary['capture'].get('capture_class', beneficiary['capture'].get('class')))
        cls = _enum(assessment.get('class', assessment.get('ems_class')))
        shock = cls == 'STRUCTURAL_SHOCK'
        contradictions = _contradictions(event, assessment)
        fatal = any(c.get('type') == 'FATAL' for c in contradictions) or event.get('lifecycle') == 'INVALIDATED'
        unresolved = any(c.get('type') == 'UNRESOLVED' for c in contradictions)
        credible = beneficiary.get('credible', role == 'PRIMARY' and bool(beneficiary.get('evidence'))) and role in {'PRIMARY','SECONDARY','LOSER'}
        state = _enum(event.get('event_state', event.get('state')))
        verified = event.get('state_verified', event.get('verified', False))
        later = state in {'APPROVED','SIGNED','DEPLOYED','LIVE','MONEY_FLOWING'}
        if fatal or not credible: thesis = 'D'
        elif capture in {'INDIRECT','NARRATIVE_ONLY','UNKNOWN'}: thesis = 'C'
        elif capture == 'DIRECT' and cls in {'MAJOR','STRUCTURAL_SHOCK'} and (role == 'PRIMARY' or beneficiary.get('exposure_score', beneficiary.get('exposure_level')) in {8,10}) and later and verified and not unresolved: thesis = 'A'
        else: thesis = 'B'
        if data == 'LOW' or move in {'DISTRIBUTION','LIQUIDITY_VACUUM'} or (role != 'LOSER' and selling and move != 'PASSIVE_SPOT_ABSORPTION'): trade = 'D'
        elif move in {'PERP_LED_CHASE','SHORT_SQUEEZE','PRICE_MARKUP_FAKE_OI','REAL_MARKET_FRAGMENTATION','UNKNOWN'} or (move == 'SELL_SIDE_ABSORPTION' and not m.get('seller_exhaustion')) or band == 'HEAVY' or crowded: trade = 'C'
        elif (move in {'SPOT_LED_ACCUMULATION','ORGANIC_MIXED'} or (move == 'PASSIVE_SPOT_ABSORPTION' and confirmed)) and band in {'NEGATIVE','UNPRICED','EARLY'} and liquidity == 'NORMAL' and data == 'HIGH': trade = 'A'
        else: trade = 'B'
        raw_trade = trade
        resilience = shock and Decimal(str((relative.get('windows', {}).get('EVENT') or {}).get('r_btc_adj') or 0)) > 0
        if context in {'HEADWIND','STRESS'} and not resilience: trade = 'ABCD'[min(3, 'ABCD'.index(trade)+1)]
        if data == 'LOW' or (move == 'UNKNOWN' and capture in {'NARRATIVE_ONLY','UNKNOWN'} and not shock): bias='UNKNOWN'
        elif move in {'SHORT_SQUEEZE','LIQUIDITY_VACUUM'} or (role != 'LOSER' and (move == 'DISTRIBUTION' or (direction == 'SELL' and breadth == 'BROAD'))) or (role == 'LOSER' and move == 'SPOT_LED_ACCUMULATION'): bias='TWO_SIDED'
        elif role == 'LOSER' or (move == 'DISTRIBUTION' and direction == 'SELL'): bias='BEARISH'
        elif credible and band != 'HEAVY' and (context != 'STRESS' or shock): bias='BULLISH'
        else: bias='NEUTRAL'
        early = band in {'UNPRICED','EARLY'}
        compressed = (liq.get('vol_compression') or {}).get('state') == 'COMPRESSED'
        squeeze = oi_above and any(v.get('funding_vs_normal') == 'BELOW' for v in deriv.get('per_venue', []))
        if band == 'HEAVY' or crowded: potential='LOW'
        elif shock and early and (liquidity == 'THIN' or compressed): potential='EXPLOSIVE'
        elif (shock and early) or (cls == 'MAJOR' and early and (liquidity == 'THIN' or compressed or squeeze)): potential='HIGH'
        elif cls in {'MAJOR','STRUCTURAL_SHOCK'}: potential='MEDIUM'
        else: potential='LOW'
        impact = _enum(assessment.get('impact_start', (assessment.get('impact_horizon') or {}).get('impact_start')))
        if move in {'SHORT_SQUEEZE','LIQUIDITY_VACUUM'}: horizon, reason='1h','market override'
        elif move == 'PERP_LED_CHASE': horizon, reason='4h','market override'
        else:
            horizon = {'NOW': '12h' if move in {'SPOT_LED_ACCUMULATION','ORGANIC_MIXED'} else '4h', 'DAYS': '24h' if assessment.get('all_dependencies_within_24h') else '3d', 'WEEKS':'7d','MONTHS':'7d','YEARS':'7d'}.get(impact,'24h')
            reason='impact start: '+impact
        horizon={'1h':'H1','4h':'H4','12h':'H12','24h':'H24','3d':'D3','7d':'D7'}[horizon]
        fired = [c for c in conditions or [] if c.get('status') == 'FIRED']
        roles = {c.get('role') for c in fired}
        if open_call:
            if fatal or 'INVALIDATION' in roles: action,rule='EXIT','B1'
            elif trade == 'D' or 'DOWNGRADE' in roles: action,rule='REDUCE','B2'
            elif 'UPGRADE' in roles and trade == 'A' and thesis in {'A','B'} and band != 'HEAVY' and data == 'HIGH' and not unresolved and role != 'LOSER': action,rule='ADD','B3'
            else: action,rule='HOLD','B4'
        elif data == 'LOW' or not dc.get('critical_semantics_verified', False): action,rule='WAIT','A1'
        elif shock and role == 'PRIMARY' and band == 'HEAVY': action,rule='DONT_CHASE','A0'
        elif role == 'LOSER' or bias == 'BEARISH': action,rule='WAIT','A2'
        elif thesis == 'D' or fatal: action,rule='WAIT','A3'
        elif trade == 'A':
            if thesis in {'A','B'}: action,rule=('WATCH_TO_START','A6') if unresolved else ('START','A5')
            elif thesis == 'C' and potential in {'HIGH','EXPLOSIVE'}: action,rule='START','A7'
            else: action,rule='WAIT','A8'
        elif trade == 'B': action,rule=('WATCH_TO_START','A9') if thesis in {'A','B'} else ('WAIT','A10')
        elif thesis in {'A','B'} and (move in {'PERP_LED_CHASE','SHORT_SQUEEZE','PRICE_MARKUP_FAKE_OI','LIQUIDITY_VACUUM'} or band == 'HEAVY'): action,rule='DONT_CHASE','A11'
        else: action,rule='WAIT','A12'
        if action == 'START' and (data != 'HIGH' or unresolved): action,rule='WATCH_TO_START','START_HIGH_DATA_GUARD' if data != 'HIGH' else 'START_UNRESOLVED_GUARD'
        watches = self._conditions(action, move, band, relative, open_call)
        certainty = _enum(assessment.get('certainty', event.get('certainty')), 'LOW')
        class_conf = 'LOW' if move == 'UNKNOWN' else 'MEDIUM' if m.get('alternatives') or divergence.get('unresolved') else _enum(m.get('class_confidence'),'HIGH')
        confidence = min([data,certainty,class_conf],key=lambda x: {'LOW':0,'MEDIUM':1,'HIGH':2}.get(x,0))
        tactical = thesis == 'C'
        return {'dec_id':new_id('DEC'),'event_id':event.get('event_id'),'token_id':beneficiary.get('token_id',m.get('token_id')),'cutoff':m.get('cutoff'), 'thesis_grade':thesis,'thesis_rule':thesis,'trade_grade':trade,'trade_rule':raw_trade,'action':action,'action_rule':rule,'tactical':tactical,'investigate':move in {'UNKNOWN','REAL_MARKET_FRAGMENTATION'},'conditions':watches,'outlook':{'direction':bias,'direction_rule':bias,'potential':potential,'potential_rule':potential,'best_horizon':horizon,'horizon_reason':reason,'confidence':confidence,'caps':{'data':data,'event':certainty,'market_truth':class_conf},'tactical':tactical,'scenarios':{'base':'Thesis persists while market structure holds.','upside':'Demand strengthens without crowded positioning.','failure':'Denial or erasure of the event move invalidates the thesis.'}},'config_version':getattr(self.settings,'config_version',assessment.get('config_version','unversioned')),'code_version':getattr(self.settings,'code_version','0.1.0'),'provenance':{'ems_id':assessment.get('ems_id'),'mta_id':m.get('mta_id')}}

    def _conditions(self, action, move, band, relative, open_call):
        anchor=(relative.get('anchor') or {}).get('ref_price')
        down=_condition('DOWNGRADE','Spot SELL with most volume selling for two consecutive hourly windows.',_expr('spot.cohort.direction','==','SELL',count=2),_expr('spot.cohort.cwb_sell','>',.5,count=2))
        up=_condition('UPGRADE','Spot BUY with most volume buying for two consecutive hourly windows and native OI not above normal.',_expr('spot.cohort.direction','==','BUY',count=2),_expr('spot.cohort.cwb_buy','>',.5,count=2),_expr('deriv.native_oi_vs_normal','in',['WITHIN','BELOW']))
        if action == 'WAIT': up=_condition('UPGRADE','Data HIGH and class resolves to spot led accumulation or organic mixed.',_expr('dc.class','==','HIGH'),_expr('mt.move_class','in',['SPOT_LED_ACCUMULATION','ORGANIC_MIXED']))
        if action == 'DONT_CHASE':
            if move == 'LIQUIDITY_VACUUM': up=_condition('UPGRADE','Depth above half its pre-event level and spot BUY.',_expr('liq.depth_p1_vs_pre','>',.5),_expr('spot.cohort.direction','==','BUY'))
            elif band == 'HEAVY': up=_condition('UPGRADE','Repricing returns to EARLY with spot BUY maintained.',_expr('price.repricing_band','==','EARLY'),_expr('spot.cohort.direction','==','BUY'))
            else: up=_condition('UPGRADE','Spot BUY with most volume buying while funding is not above normal.',_expr('spot.cohort.direction','==','BUY'),_expr('spot.cohort.cwb_buy','>',.5),_expr('deriv.funding_vs_normal','!=','ABOVE'))
        result=[up,down]
        level=(open_call or {}).get('invalidation_price')
        if level is None and anchor is not None:
            entry=(relative.get('windows') or {}).get('EVENT',{}).get('entry_ref_price', relative.get('entry_ref_price', anchor))
            level=str(min(Decimal(str(anchor)),Decimal(str(entry))))
        if level is not None: result.append(_condition('INVALIDATION','Hourly close below the event invalidation level.',_expr('price.close_vs_level','<',level)))
        result.append(_condition('INVALIDATION','A fatal contradiction invalidates the thesis.',_expr('event.contradiction_fatal','==',True)))
        return result
