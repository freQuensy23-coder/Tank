"""Strict exchange normalization. Missing semantic fields are errors, never estimates."""
from __future__ import annotations
from datetime import timedelta
from decimal import Decimal
from tank.util import iso, parse_time, jsonable

D = Decimal
class SemanticError(ValueError):
    pass

def decimal(value):
    if value is None or isinstance(value, bool):
        raise SemanticError('missing or invalid decimal')
    result = D(str(value))
    if not result.is_finite():
        raise SemanticError('nonfinite decimal')
    return result

def venue_key(instrument):
    venue = instrument['venue'].lower()
    perp = instrument.get('market_type', 'SPOT').lower() not in ('spot',)
    if venue.startswith('binance'): return 'binance.usdm' if perp or venue.endswith('usdm') else 'binance.spot'
    if venue.startswith('bybit'): return 'bybit.linear' if perp else 'bybit.spot'
    if venue.startswith('okx'): return 'okx.swap' if perp else 'okx.spot'
    if venue.startswith('hyperliquid'): return 'hyperliquid.perp'
    return venue.split('.')[0] + '.spot'

def trade(instrument, raw, *, received_ts=None):
    key = venue_key(instrument); venue = key.split('.')[0]
    aggregation = 'PER_FILL'
    if venue == 'binance':
        if not isinstance(raw['m'], bool): raise SemanticError('buyer-is-maker must be boolean')
        side, qty, price, ts, tid = ('SELL' if raw['m'] else 'BUY'), raw['q'], raw['p'], raw['T'], raw['a']
        aggregation = 'PER_TAKER_ORDER_SAME_PRICE'
    elif venue == 'coinbase':
        if raw['side'] not in ('buy', 'sell'): raise SemanticError('unknown Coinbase maker side')
        side, qty, price, ts, tid = ('BUY' if raw['side'] == 'sell' else 'SELL'), raw['size'], raw['price'], raw['time'], raw['trade_id']
    elif venue == 'okx':
        if raw['side'] not in ('buy', 'sell'): raise SemanticError('unknown OKX taker side')
        side, qty, price, ts, tid = raw['side'].upper(), raw['sz'], raw['px'], raw['ts'], raw['tradeId']
        if key == 'okx.swap':
            multiplier = instrument.get('contract_multiplier')
            if multiplier is None: raise SemanticError('OKX ctVal base multiplier unverified')
            qty = decimal(qty) * decimal(multiplier)
        aggregation = 'PER_TAKER_ORDER_SAME_PRICE'
    elif venue == 'bybit':
        s = raw.get('S', raw.get('side'))
        if s not in ('Buy', 'Sell'): raise SemanticError('unknown Bybit taker side')
        side, qty, price, ts, tid = s.upper(), raw.get('v', raw.get('size')), raw.get('p', raw.get('price')), raw.get('T', raw.get('time')), raw.get('i', raw.get('execId'))
    elif venue == 'kraken':
        if isinstance(raw, list):
            if len(raw) < 7: raise SemanticError('Kraken trade ID absent')
            price, qty, ts, s, _, _, tid = raw[:7]
        else: price, qty, ts, s, tid = raw['price'], raw['qty'], raw['timestamp'], raw['side'], raw['trade_id']
        if s not in ('buy','sell','b','s'): raise SemanticError('unknown Kraken taker side')
        side = 'BUY' if s in ('buy','b') else 'SELL'
    elif venue == 'hyperliquid':
        if raw['side'] not in ('B', 'A'): raise SemanticError('unknown Hyperliquid side')
        side, qty, price, ts, tid = ('BUY' if raw['side'] == 'B' else 'SELL'), raw['sz'], raw['px'], raw['time'], raw['tid']
    else: raise SemanticError(f'unsupported venue {venue}')
    if tid is None: raise SemanticError('missing trade ID')
    qty, price = decimal(qty), decimal(price)
    if qty <= 0 or price <= 0: raise SemanticError('nonpositive trade')
    quote = instrument.get('quote_asset','').upper()
    # Stablecoin quote units are retained as quote units, never silently called USD.
    fx = instrument.get('quote_usd_rate')
    if quote == 'USD': fx = '1'
    usd = qty * price * decimal(fx) if fx is not None else None
    return jsonable(dict(instrument_id=instrument['instrument_id'], token_id=instrument['token_id'],
        venue=venue, market_type=instrument['market_type'], trade_id=str(tid), ts=iso(ts),
        received_ts=iso(received_ts), side=side, qty_native=qty, price=price,
        quote_notional=qty*price, notional_usd=usd, quote_asset=quote,
        usd_reason=None if usd is not None else 'QUOTE_USD_RATE_UNAVAILABLE',
        metric_key=key + ('.aggTrade' if venue == 'binance' else '.trades' if venue in ('okx','hyperliquid') else '.matches' if venue=='coinbase' else '.publicTrade' if venue=='bybit' else '.trade'),
        trade_aggregation=aggregation, raw_first_id=raw.get('f') if isinstance(raw,dict) else None,
        raw_last_id=raw.get('l') if isinstance(raw,dict) else None,
        sequence=raw.get('seq') if isinstance(raw,dict) else None))

def kline(instrument, row, *, now=None):
    key = venue_key(instrument)
    extra = {}
    if key.startswith('binance'):
        ts,o,h,l,c,v,closed,q,n,b,bq = row[:11]
        extra = dict(quote_volume=q,n_trades=int(n),taker_buy_native=b,taker_buy_quote=bq)
        complete = parse_time(closed) < parse_time(now or iso())
    elif key.startswith('okx'):
        ts,o,h,l,c,v = row[:6]
        if key=='okx.swap':
            if instrument.get('contract_multiplier') is None: raise SemanticError('OKX multiplier absent')
            v = decimal(v)*decimal(instrument['contract_multiplier'])
        extra = dict(quote_volume=row[7] if len(row)>7 else None,n_trades=None,taker_buy_native=None,taker_buy_quote=None)
        complete = str(row[8]) == '1' if len(row)>8 else False
    elif key.startswith('bybit'):
        ts,o,h,l,c,v,q = row[:7]
        complete = parse_time(ts)+timedelta(minutes=1) <= parse_time(now or iso())
        extra = dict(quote_volume=q,n_trades=None,taker_buy_native=None,taker_buy_quote=None)
    elif key.startswith('coinbase'):
        ts,l,h,o,c,v = row[:6]
        complete = parse_time(ts)+timedelta(minutes=1) <= parse_time(now or iso())
    elif key.startswith('kraken'):
        ts,o,h,l,c,_,v,n = row[:8]
        complete = parse_time(ts)+timedelta(minutes=1) <= parse_time(now or iso())
        extra = dict(n_trades=int(n))
    elif key.startswith('hyperliquid'):
        ts,o,h,l,c,v = (row[x] for x in ('t','o','h','l','c','v'))
        complete = parse_time(row['T']) < parse_time(now or iso())
        extra = dict(n_trades=int(row['n']))
    else: raise SemanticError('unsupported kline')
    result = dict(instrument_id=instrument['instrument_id'],token_id=instrument['token_id'],open_time=iso(ts),
                  o=str(decimal(o)),h=str(decimal(h)),l=str(decimal(l)),c=str(decimal(c)),
                  volume_native=str(decimal(v)),complete=complete,received_ts=iso(now))
    for name in ('quote_volume','n_trades','taker_buy_native','taker_buy_quote'): result[name]=extra.get(name)
    return jsonable(result)

def oi(instrument, raw, *, mark=None, received_ts=None):
    key=venue_key(instrument); sidedness='SINGLE_SIDED'; reason=None
    if key=='binance.usdm':
        field='sumOpenInterest' if 'sumOpenInterest' in raw else 'openInterest'
        value=raw[field]; ts=raw.get('time',raw.get('timestamp')); metric=key+'.'+('openInterestHist' if field=='sumOpenInterest' else 'openInterest')
    elif key=='okx.swap': value=raw['oiCcy'];ts=raw['ts'];metric=key+'.open-interest'
    elif key=='bybit.linear':
        if 'singleOpenInterest' not in raw: raise SemanticError('Bybit singleOpenInterest absent; openInterest cannot substitute')
        value=raw['singleOpenInterest'];ts=raw['timestamp'];metric=key+'.open-interest'
    elif key=='hyperliquid.perp':
        value=raw['openInterest'];ts=received_ts or iso();metric=key+'.ctx';sidedness='UNKNOWN';reason='SIDEDNESS_UNPUBLISHED'
    else: raise SemanticError('spot has no OI')
    value=decimal(value)
    if value<0: raise SemanticError('negative open interest')
    usd=decimal(raw['sumOpenInterestValue']) if raw.get('sumOpenInterestValue') is not None else decimal(raw['oiUsd']) if raw.get('oiUsd') is not None else value*decimal(mark) if mark is not None else None
    return jsonable(dict(instrument_id=instrument['instrument_id'],token_id=instrument['token_id'],ts=iso(ts),received_ts=iso(received_ts),raw_value=str(value),raw_unit='BASE_QTY',sidedness=sidedness,metric_key=metric,oi_native=value,oi_usd=usd,semantic_confidence='VERIFIED',synchronized_eligible=sidedness=='SINGLE_SIDED',gap_reason=reason,timestamp_precision='COLLECTION_ONLY' if key=='hyperliquid.perp' else 'BUCKET' if key=='bybit.linear' or 'Hist' in metric else 'EXACT'))

def funding(instrument, raw, *, kind, interval_h, received_ts=None):
    key=venue_key(instrument); interval=decimal(interval_h)
    if interval<=0: raise SemanticError('invalid funding interval')
    if key.startswith('binance'):
        rate=raw['fundingRate'] if kind=='SETTLED' else raw.get('lastFundingRate',raw.get('r'))
        timestamp=raw['fundingTime'] if kind=='SETTLED' else raw.get('time',raw.get('E',received_ts or iso()))
    elif key.startswith('okx'):
        rate=raw.get('realizedRate') or raw['fundingRate'] if kind=='SETTLED' else raw['fundingRate']
        timestamp=raw['fundingTime'] if kind=='SETTLED' else raw.get('ts',received_ts or iso())
    elif key.startswith('bybit'):
        rate=raw['fundingRate'];timestamp=raw['fundingRateTimestamp'] if kind=='SETTLED' else received_ts or iso()
    elif key.startswith('hyperliquid'):
        rate=raw['fundingRate'] if kind=='SETTLED' else raw['funding'];timestamp=raw.get('time',received_ts or iso())
    else: raise SemanticError('spot has no funding')
    rate=decimal(rate)
    return jsonable(dict(instrument_id=instrument['instrument_id'],token_id=instrument['token_id'],kind=kind,rate=rate,interval_h=interval,rate_8h_eq=rate*8/interval,**({'funding_time':iso(timestamp),'settlement_time':iso(timestamp)} if kind=='SETTLED' else {'as_of':iso(timestamp)}),received_ts=iso(received_ts),metric_key=key+'.funding'))

def liquidation(instrument, raw, *, received_ts=None):
    key=venue_key(instrument)
    if key=='binance.usdm':
        x=raw.get('o',raw);side='LONG_LIQ' if x['S']=='SELL' else 'SHORT_LIQ';qty=x['z'] if x.get('z') else x['q'];price=x['ap'];ts=x['T'];coverage='SAMPLED'
    elif key=='bybit.linear':
        # Bybit S identifies the liquidated position, unlike Binance order side.
        side='LONG_LIQ' if raw['S']=='Buy' else 'SHORT_LIQ';qty=raw['v'];price=raw['p'];ts=raw['T'];coverage='COMPLETE'
    elif key=='okx.swap':
        side='LONG_LIQ' if raw['posSide']=='long' else 'SHORT_LIQ' if raw['posSide']=='short' else None
        if side is None: raise SemanticError('OKX liquidation position side absent')
        if instrument.get('contract_multiplier') is None: raise SemanticError('OKX multiplier absent')
        qty=decimal(raw['sz'])*decimal(instrument['contract_multiplier']);price=raw['bkPx'];ts=raw['ts'];coverage='SAMPLED'
    else: raise SemanticError('liquidations unsupported')
    qty,price=decimal(qty),decimal(price)
    fx='1' if instrument.get('quote_asset')=='USD' else instrument.get('quote_usd_rate')
    return jsonable(dict(instrument_id=instrument['instrument_id'],token_id=instrument['token_id'],ts=iso(ts),received_ts=iso(received_ts),side=side,qty_native=qty,price=price,notional_usd=qty*price*decimal(fx) if fx is not None else None,coverage=coverage,price_kind='BANKRUPTCY' if key in ('okx.swap','bybit.linear') else 'EXECUTION_AVERAGE'))
