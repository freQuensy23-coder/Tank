"""Stateful level 2 books, continuity and venue checksum verification."""
from __future__ import annotations
import zlib
from decimal import Decimal
from tank.util import iso, jsonable
from .normalize import decimal, venue_key, SemanticError

class OrderBook:
    def __init__(self, instrument):
        self.instrument=instrument;self.bids={};self.asks={};self.sequence=None
        self.integrity_ok=False;self.gap_reason='NO_SNAPSHOT';self.exchange_ts=None
        self._strings={'bid':{},'ask':{}}

    def _levels(self, side, levels, *, snapshot=False):
        book=self.bids if side=='bid' else self.asks
        strings=self._strings[side]
        if snapshot: book.clear();strings.clear()
        for level in levels:
            if isinstance(level,dict): p,q=str(level['price']),str(level['qty'])
            else: p,q=str(level[0]),str(level[1])
            price,size=decimal(p),decimal(q)
            if price<=0 or size<0: raise SemanticError('invalid book level')
            if not size: book.pop(price,None);strings.pop(price,None)
            else: book[price]=size;strings[price]=(p,q)

    def snapshot(self,bids,asks,*,sequence=None,ts=None,checksum=None):
        self._levels('bid',bids,snapshot=True);self._levels('ask',asks,snapshot=True)
        self.sequence=sequence;self.exchange_ts=iso(ts);self.integrity_ok=bool(self.bids and self.asks)
        self.gap_reason=None if self.integrity_ok else 'EMPTY_BOOK'
        if checksum is not None: self.verify(checksum)

    def update(self,bids,asks,*,sequence=None,previous=None,first=None,ts=None,checksum=None):
        if self.sequence is None or not self.integrity_ok: raise SemanticError('book requires fresh snapshot')
        key=venue_key(self.instrument)
        if sequence is not None and int(sequence)<=int(self.sequence): return False
        if key.startswith('binance'):
            valid=(int(previous)==int(self.sequence)) if previous is not None else (first is not None and int(first)<=int(self.sequence)+1<=int(sequence))
        elif key.startswith('okx'): valid=previous is not None and int(previous)==int(self.sequence)
        else: valid=True  # Kraken checksum and Coinbase full level2 delivery; Bybit u is not consecutive.
        if not valid:
            self.integrity_ok=False;self.gap_reason='SEQUENCE_GAP';raise SemanticError('book sequence gap')
        self._levels('bid',bids);self._levels('ask',asks)
        if sequence is not None:self.sequence=sequence
        self.exchange_ts=iso(ts)
        if checksum is not None:self.verify(checksum)
        return True

    def verify(self,expected):
        key=venue_key(self.instrument)
        bids=sorted(self.bids,reverse=True);asks=sorted(self.asks)
        if key.startswith('okx'):
            parts=[]
            for i in range(25):
                if i<len(bids):parts.extend(self._strings['bid'][bids[i]])
                if i<len(asks):parts.extend(self._strings['ask'][asks[i]])
            crc=zlib.crc32(':'.join(parts).encode());crc=crc if crc<2**31 else crc-2**32
        elif key.startswith('kraken'):
            parts=[]
            for side,prices in (('ask',asks[:10]),('bid',bids[:10])):
                for p in prices:
                    for value in self._strings[side][p]: parts.append(value.replace('.','').lstrip('0'))
            crc=zlib.crc32(''.join(parts).encode())
        else:raise SemanticError('checksum not defined for venue')
        self.integrity_ok=crc==int(expected);self.gap_reason=None if self.integrity_ok else 'CHECKSUM_MISMATCH'
        if not self.integrity_ok: raise SemanticError('book checksum mismatch')
        return True

    def summary(self,*,received_ts=None):
        if not self.bids or not self.asks: raise SemanticError('empty order book')
        bid,ask=max(self.bids),min(self.asks)
        if ask<=bid: self.integrity_ok=False;self.gap_reason='CROSSED_BOOK'
        mid=(ask+bid)/2
        multiplier=decimal(self.instrument['contract_multiplier']) if venue_key(self.instrument)=='okx.swap' and self.instrument.get('contract_multiplier') is not None else Decimal(1)
        if venue_key(self.instrument)=='okx.swap' and self.instrument.get('contract_multiplier') is None: raise SemanticError('OKX multiplier absent')
        fx=Decimal(1) if self.instrument.get('quote_asset')=='USD' else decimal(self.instrument['quote_usd_rate']) if self.instrument.get('quote_usd_rate') is not None else None
        depth={};ranges={}
        for side,book in (('bid',self.bids),('ask',self.asks)):
            far=min(book) if side=='bid' else max(book)
            covered=abs(far/mid-1)*100;ranges[side]=covered;depth[side]={}
            for name,pct in (('p0_5',Decimal('.5')),('p1',Decimal(1)),('p2',Decimal(2))):
                if covered<pct:depth[side][name]=None;continue
                levels=[(p,q*multiplier) for p,q in book.items() if abs(p/mid-1)*100<=pct]
                depth[side][name]={'native':sum((q for p,q in levels),Decimal(0)), 'usd':sum((p*q*fx for p,q in levels),Decimal(0)) if fx is not None else None}
        return jsonable(dict(instrument_id=self.instrument['instrument_id'],token_id=self.instrument['token_id'],exchange_ts=self.exchange_ts,received_ts=iso(received_ts),mid=mid,best_bid=bid,best_ask=ask,spread_bps=(ask-bid)/mid*10000,depth=depth,range_covered_pct=ranges,levels_received=len(self.bids)+len(self.asks),integrity_ok=self.integrity_ok,gap_reason=self.gap_reason))
