"""Minute flow accumulator; persistent trade dedup is performed by MarketService."""
from decimal import Decimal as D
from tank.util import parse_time,iso,jsonable

class FlowAccumulator:
    def __init__(self): self.bars={};self.last={};self.connected={}

    def add(self,trade,*,continuity=True):
        iid=trade['instrument_id'];ts=parse_time(trade['ts']);minute=iso(ts.replace(second=0,microsecond=0));key=(iid,minute)
        prior=self.last.get(iid);gap=None
        if continuity and prior and trade['venue'] in ('binance','coinbase','kraken'):
            if int(trade['trade_id'])!=int(prior['trade_id'])+1:gap='TRADE_ID_GAP'
        self.last[iid]=trade
        q,p=D(trade['qty_native']),D(trade['price']);usd=D(trade['notional_usd']) if trade.get('notional_usd') is not None else None
        if key not in self.bars:
            self.bars[key]=dict(instrument_id=iid,token_id=trade['token_id'],minute_start=minute,buy_qty=D(0),sell_qty=D(0),buy_notional=D(0) if usd is not None else None,sell_notional=D(0) if usd is not None else None,n_buy=0,n_sell=0,sum_sq_qty=D(0),open=p,high=p,low=p,close=p,vwap=p,size_buckets={f'B{i}':{'buy_qty':D(0),'sell_qty':D(0),'buy_notional':D(0),'sell_notional':D(0),'n':0} for i in range(1,6)},first_trade_id=trade['trade_id'],last_trade_id=trade['trade_id'],complete=False,gap_reason=gap,trade_aggregation=trade['trade_aggregation'],_qty=D(0),_quote=D(0),_first_ts=ts,_last_ts=ts)
        b=self.bars[key];side=trade['side'].lower();b[side+'_qty']+=q;b['n_'+side]+=1;b['sum_sq_qty']+=q*q
        if usd is None:b['buy_notional']=None;b['sell_notional']=None;b['size_buckets']=None
        elif b[side+'_notional'] is not None:
            b[side+'_notional']+=usd
            if b['size_buckets'] is not None:
                index=next((i for i,t in enumerate((1000,10000,100000,1000000),1) if usd<t),5);bucket=b['size_buckets'][f'B{index}'];bucket[side+'_qty']+=q;bucket[side+'_notional']+=usd;bucket['n']+=1
        b['high']=max(b['high'],p);b['low']=min(b['low'],p)
        if ts<b['_first_ts']:b['_first_ts']=ts;b['open']=p;b['first_trade_id']=trade['trade_id']
        if ts>=b['_last_ts']:b['_last_ts']=ts;b['close']=p;b['last_trade_id']=trade['trade_id']
        b['_qty']+=q;b['_quote']+=q*p;b['vwap']=b['_quote']/b['_qty']
        if gap:b['gap_reason']=gap
        return self.record(key)

    def record(self,key,*,complete=None):
        bar=self.bars[key]
        if complete is not None:bar['complete']=bool(complete and not bar['gap_reason'])
        return jsonable({k:v for k,v in bar.items() if not k.startswith('_')})

    def mark_gap(self,instrument_id,start,end,reason):
        for key,b in self.bars.items():
            if key[0]==instrument_id and parse_time(start).replace(second=0,microsecond=0)<=parse_time(key[1])<=parse_time(end):b['gap_reason']=reason;b['complete']=False
