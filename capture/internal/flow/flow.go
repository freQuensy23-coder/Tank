// Package flow is tank.market.flow.FlowAccumulator: 1m flow bars with size buckets,
// boundary segments and reference points, built from normalized prints and coverage
// proofs. Record returns the same dict the Python accumulator records.
package flow

import (
	"sort"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const prec = pydec.Precise

var bucketLimits = []pydec.Dec{
	pydec.FromInt(1000), pydec.FromInt(10000), pydec.FromInt(100000), pydec.FromInt(1000000),
}

// BucketLimitsUSD are the size bucket edges (03 §5.2): B1 < 1k ... B5 >= 1M.
var BucketLimitsUSD = []int64{1000, 10000, 100000, 1000000}

// Key identifies one instrument minute.
type Key struct {
	IID    string
	Minute int64 // unix seconds of the minute start
}

// MinuteTime is the minute start.
func (k Key) MinuteTime() time.Time { return time.Unix(k.Minute, 0).UTC() }

// MinuteISO is the minute start as iso().
func (k Key) MinuteISO() string { return pytime.ISO(k.MinuteTime()) }

// KeyOf is the bar key of a time.
func KeyOf(iid string, t time.Time) Key { return Key{iid, pytime.MinuteFloor(t).Unix()} }

// Proof is one coverage proof: [Start, End) is (in)complete.
type Proof struct {
	Start, End time.Time
	Complete   bool
	Source     string // "" when absent
	Reason     string // "" is None
	Observed   time.Time
}

func (p Proof) sameKind(o Proof) bool {
	return p.Start.Equal(o.Start) && p.Complete == o.Complete && p.Source == o.Source && p.Reason == o.Reason
}

// TradePoint is FlowAccumulator.trade_point.
type TradePoint struct {
	TradeID      string
	Price        string
	TS           time.Time
	TSPrec       string
	TSPrecReason string
}

// Map is the trade point dict, or None (an untyped nil) without a point.
func (p *TradePoint) Map() any {
	if p == nil {
		return nil
	}
	return map[string]any{
		"trade_id": p.TradeID, "price": p.Price, "ts": pytime.ISO(p.TS),
		"ts_prec": nilIf(p.TSPrec), "ts_prec_reason": nilIf(p.TSPrecReason),
	}
}

// PointOf is trade_point(trade).
func PointOf(t *normalize.Trade) *TradePoint {
	if t == nil {
		return nil
	}
	p := &TradePoint{TradeID: t.TradeID, Price: t.Price.String(), TS: t.TS, TSPrec: t.TSPrec}
	if p.TSPrec == "" {
		p.TSPrecReason = pytime.Incomplete
	}
	return p
}

func nilIf(s string) any {
	if s == "" {
		return nil
	}
	return s
}

type bucket struct {
	buyQty, sellQty, buyNotional, sellNotional pydec.Dec
	n                                          int64
}

func (b *bucket) dict() map[string]any {
	return map[string]any{
		"buy_qty": b.buyQty.String(), "sell_qty": b.sellQty.String(),
		"buy_notional": b.buyNotional.String(), "sell_notional": b.sellNotional.String(), "n": b.n,
	}
}

// Bar is one instrument minute under accumulation.
type Bar struct {
	InstrumentID, TokenID string
	Venue, MarketType     any
	QuoteAsset            any
	MinuteStart           string

	BuyQty, SellQty, SumSq    pydec.Dec
	BuyNotional, SellNotional *pydec.Dec
	NotionalReason            string
	NBuy, NSell               int64
	Open, High, Low, Close    *pydec.Dec
	VWAP                      *pydec.Dec
	Buckets                   *[5]bucket
	FirstTradeID, LastTradeID *string
	FirstTrade, LastTrade     *TradePoint
	Complete                  bool
	GapReason                 string
	TradeAggregation          string
	QuoteVolume               pydec.Dec
	VolumeUSD                 *pydec.Dec
	ZeroTrade                 bool // zero_trade_minute is present (an empty minute without prints)
	fromEmpty                 bool
	notionalReasonSet         bool
	qty, quote                pydec.Dec
	firstTS, lastTS           *time.Time
	Trades                    []*normalize.Trade
	Coverage                  []Proof
	PriorTrade                *TradePoint
	RESTVerified              bool
}

// Accumulator is FlowAccumulator.
type Accumulator struct {
	Bars           map[Key]*Bar
	Last           map[string]*normalize.Trade
	Boundaries     map[Key]map[int64]time.Time // minute -> exact boundary points (unix micro)
	References     map[string]map[string]any   // iid + "|" + iso(boundary) -> explicit point
	CoverageRanges map[string][]Proof
	Dirty          map[Key]bool
}

// New is FlowAccumulator().
func New() *Accumulator {
	return &Accumulator{
		Bars:           map[Key]*Bar{},
		Last:           map[string]*normalize.Trade{},
		Boundaries:     map[Key]map[int64]time.Time{},
		References:     map[string]map[string]any{},
		CoverageRanges: map[string][]Proof{},
		Dirty:          map[Key]bool{},
	}
}

// RefKey is the references key of (instrument, boundary).
func RefKey(iid string, boundary time.Time) string { return iid + "|" + pytime.ISO(boundary) }

func newBuckets() *[5]bucket { return &[5]bucket{} }

func appendProof(list []Proof, proof Proof) []Proof {
	if n := len(list); n > 0 && list[n-1].sameKind(proof) && !list[n-1].End.After(proof.End) {
		list[n-1] = proof
		return list
	}
	return append(list, proof)
}

// ProveRange is prove_range: record that [start, end) of an instrument is (in)complete
// and attach the proof to every overlapping bar held.
func (a *Accumulator) ProveRange(iid string, start, end time.Time, complete bool, source, reason string, observed time.Time) Proof {
	proof := Proof{Start: start, End: end, Complete: complete, Source: source, Reason: reason, Observed: observed}
	recent := appendProof(a.CoverageRanges[iid], proof)
	latest := recent[0].End
	for _, p := range recent {
		if p.End.After(latest) {
			latest = p.End
		}
	}
	kept := recent[:0:0]
	for _, p := range recent {
		if !p.End.Before(latest.Add(-3 * time.Minute)) {
			kept = append(kept, p)
		}
	}
	a.CoverageRanges[iid] = kept
	for key, bar := range a.Bars {
		m := key.MinuteTime()
		if key.IID == iid && m.Before(end) && m.Add(time.Minute).After(start) {
			a.Dirty[key] = true
			bar.Coverage = appendProof(bar.Coverage, proof)
		}
	}
	return proof
}

// EmptyMinute is empty_minute: a minute without prints (yet).
func (a *Accumulator) EmptyMinute(inst *venue.Instrument, minute time.Time) {
	key := KeyOf(inst.ID, minute)
	if _, ok := a.Bars[key]; ok {
		return
	}
	a.Dirty[key] = true
	zero := pydec.Zero
	b := &Bar{
		InstrumentID: key.IID, TokenID: inst.TokenID, Venue: inst.Venue, MarketType: inst.MarketType,
		MinuteStart: key.MinuteISO(), QuoteAsset: inst.QuoteValue(),
		BuyQty: zero, SellQty: zero, SumSq: zero, BuyNotional: &zero, SellNotional: &zero,
		QuoteVolume: zero, VolumeUSD: &zero, ZeroTrade: true, fromEmpty: true,
		Buckets: newBuckets(), qty: zero, quote: zero,
		Coverage: append([]Proof(nil), a.CoverageRanges[key.IID]...),
	}
	if last := a.Last[key.IID]; last != nil && last.TS.Before(key.MinuteTime()) {
		b.PriorTrade = PointOf(last)
	}
	a.Bars[key] = b
}

// RegisterBoundaries is register_boundaries: exact window edges inside minutes get
// boundary segments and reference points; an edge on a minute start also bounds the
// previous minute.
func (a *Accumulator) RegisterBoundaries(iid string, points []time.Time) {
	add := func(key Key, at time.Time) {
		set := a.Boundaries[key]
		if set == nil {
			set = map[int64]time.Time{}
			a.Boundaries[key] = set
		}
		set[at.UnixMicro()] = at
		a.Dirty[key] = true
	}
	for _, at := range points {
		at = at.UTC()
		add(KeyOf(iid, at), at)
		if at.Second() == 0 && at.Nanosecond() == 0 {
			add(KeyOf(iid, at.Add(-time.Minute)), at)
		}
	}
}

// Covered is FlowAccumulator.covered: every sub-interval of [start, end) has a covering
// proof and the last such proof is complete.
func Covered(start, end time.Time, ranges []Proof) bool {
	if start.Equal(end) {
		return true
	}
	clamp := func(t time.Time) time.Time {
		if t.After(end) {
			t = end
		}
		if t.Before(start) {
			t = start
		}
		return t
	}
	set := map[int64]time.Time{start.UnixMicro(): start, end.UnixMicro(): end}
	for _, p := range ranges {
		for _, t := range []time.Time{p.Start, p.End} {
			c := clamp(t)
			set[c.UnixMicro()] = c
		}
	}
	points := make([]time.Time, 0, len(set))
	for _, t := range set {
		points = append(points, t)
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Before(points[j]) })
	for i := 0; i+1 < len(points); i++ {
		lo, hi := points[i], points[i+1]
		var last *Proof
		for j := range ranges {
			if !ranges[j].Start.After(lo) && !ranges[j].End.Before(hi) {
				last = &ranges[j]
			}
		}
		if last == nil || !last.Complete {
			return false
		}
	}
	return true
}

func intID(id string) (int64, bool) {
	v, err := pyval.Int(id)
	return v, err == nil
}

// Add is FlowAccumulator.add without the record: with continuity, consecutive-ID venues
// flag TRADE_ID_GAP, OKX a non-monotonic ID and Bybit a missing or regressing sequence.
func (a *Accumulator) Add(t *normalize.Trade, continuity bool) {
	iid := t.InstrumentID
	key := KeyOf(iid, t.TS)
	prior := a.Last[iid]
	gap := ""
	if continuity && prior != nil && (t.Venue == "binance" || t.Venue == "coinbase" || t.Venue == "kraken") {
		cur, ok1 := intID(t.TradeID)
		prev, ok2 := intID(prior.TradeID)
		if ok1 && ok2 && cur != prev+1 {
			gap = "TRADE_ID_GAP"
		}
	}
	if continuity && prior != nil && t.Venue == "okx" {
		cur, ok1 := intID(t.TradeID)
		prev, ok2 := intID(prior.TradeID)
		if ok1 && ok2 && cur <= prev {
			gap = "NON_MONOTONIC_TRADE_ID"
		}
	}
	if continuity && t.Venue == "bybit" {
		if t.Sequence == nil {
			gap = "BYBIT_SEQUENCE_UNAVAILABLE"
		} else if prior != nil && prior.Sequence != nil {
			cur, err1 := pyval.Int(t.Sequence)
			prev, err2 := pyval.Int(prior.Sequence)
			if err1 == nil && err2 == nil && cur < prev {
				gap = "BYBIT_SEQUENCE_REGRESSION"
			}
		}
	}
	if prior == nil || !t.TS.Before(prior.TS) {
		a.Last[iid] = t
	}
	q, p := t.Qty, t.Price
	usd := t.NotionalUSD
	b, ok := a.Bars[key]
	if !ok {
		zero := pydec.Zero
		b = &Bar{
			InstrumentID: iid, TokenID: t.TokenID, MarketType: t.MarketType, Venue: t.Venue,
			QuoteAsset: t.QuoteAsset, MinuteStart: key.MinuteISO(),
			BuyQty: zero, SellQty: zero, SumSq: zero,
			Open: &p, High: &p, Low: &p, Close: &p, VWAP: &p,
			Buckets: newBuckets(), GapReason: gap, TradeAggregation: t.TradeAggregation,
			qty: zero, quote: zero, Coverage: append([]Proof(nil), a.CoverageRanges[iid]...),
		}
		id := t.TradeID
		b.FirstTradeID, b.LastTradeID = &id, &id
		ts := t.TS
		b.firstTS, b.lastTS = &ts, &ts
		if usd != nil {
			z1, z2 := pydec.Zero, pydec.Zero
			b.BuyNotional, b.SellNotional = &z1, &z2
		} else {
			b.NotionalReason = "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
		}
		if prior != nil && prior.TS.Before(pytime.MinuteFloor(t.TS)) {
			b.PriorTrade = PointOf(prior)
		}
		a.Bars[key] = b
	}
	a.Dirty[key] = true
	b.ZeroTrade = false
	b.Trades = append(b.Trades, t)
	buy := t.Side == "BUY"
	if buy {
		b.BuyQty = pydec.Add(b.BuyQty, q, prec)
		b.NBuy++
	} else {
		b.SellQty = pydec.Add(b.SellQty, q, prec)
		b.NSell++
	}
	b.SumSq = pydec.Add(b.SumSq, pydec.Mul(q, q, prec), prec)
	if usd == nil {
		b.NotionalReason, b.notionalReasonSet = "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE", true
		b.BuyNotional, b.SellNotional, b.Buckets = nil, nil, nil
	} else if (buy && b.BuyNotional != nil) || (!buy && b.SellNotional != nil) {
		if buy {
			v := pydec.Add(*b.BuyNotional, *usd, prec)
			b.BuyNotional = &v
		} else {
			v := pydec.Add(*b.SellNotional, *usd, prec)
			b.SellNotional = &v
		}
		if b.Buckets != nil {
			index := 4
			for i, limit := range bucketLimits {
				if usd.Cmp(limit) < 0 {
					index = i
					break
				}
			}
			bk := &b.Buckets[index]
			if buy {
				bk.buyQty = pydec.Add(bk.buyQty, q, prec)
				bk.buyNotional = pydec.Add(bk.buyNotional, *usd, prec)
			} else {
				bk.sellQty = pydec.Add(bk.sellQty, q, prec)
				bk.sellNotional = pydec.Add(bk.sellNotional, *usd, prec)
			}
			bk.n++
		}
	}
	if b.High == nil {
		b.High = &p
	} else {
		v := pydec.Max(*b.High, p)
		b.High = &v
	}
	if b.Low == nil {
		b.Low = &p
	} else {
		v := pydec.Min(*b.Low, p)
		b.Low = &v
	}
	if b.firstTS == nil || t.TS.Before(*b.firstTS) {
		ts, id := t.TS, t.TradeID
		b.firstTS, b.Open, b.FirstTradeID, b.FirstTrade = &ts, &p, &id, PointOf(t)
	}
	if b.lastTS == nil || !t.TS.Before(*b.lastTS) {
		ts, id := t.TS, t.TradeID
		b.lastTS, b.Close, b.LastTradeID, b.LastTrade = &ts, &p, &id, PointOf(t)
	}
	if !b.fromEmpty && b.FirstTrade == nil {
		b.FirstTrade = PointOf(t) // setdefault: only the bar created by this print lacks the key
	}
	b.qty = pydec.Add(b.qty, q, prec)
	b.quote = pydec.Add(b.quote, pydec.Mul(q, p, prec), prec)
	b.QuoteVolume = b.quote
	if b.BuyNotional != nil && b.SellNotional != nil {
		v := pydec.Add(*b.BuyNotional, *b.SellNotional, prec)
		b.VolumeUSD = &v
	} else {
		b.VolumeUSD = nil
	}
	vwap := pydec.MustDiv(b.quote, b.qty, prec)
	b.VWAP = &vwap
	if gap != "" {
		b.GapReason = gap
	}
}

func decOrNil(d *pydec.Dec) any {
	if d == nil {
		return nil
	}
	return d.String()
}

func strOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// fields is the bar's own dict (keys without a leading underscore).
func (b *Bar) fields() map[string]any {
	r := map[string]any{
		"instrument_id": b.InstrumentID, "token_id": b.TokenID, "venue": b.Venue,
		"market_type": b.MarketType, "quote_asset": b.QuoteAsset, "minute_start": b.MinuteStart,
		"buy_qty": b.BuyQty.String(), "sell_qty": b.SellQty.String(),
		"buy_notional": decOrNil(b.BuyNotional), "sell_notional": decOrNil(b.SellNotional),
		"n_buy": b.NBuy, "n_sell": b.NSell, "sum_sq_qty": b.SumSq.String(),
		"open": decOrNil(b.Open), "high": decOrNil(b.High), "low": decOrNil(b.Low),
		"close": decOrNil(b.Close), "vwap": decOrNil(b.VWAP),
		"first_trade_id": strOrNil(b.FirstTradeID), "last_trade_id": strOrNil(b.LastTradeID),
		"complete": b.Complete, "gap_reason": nilIf(b.GapReason),
		"first_trade": b.FirstTrade.Map(), "last_trade": b.LastTrade.Map(),
		"quote_volume": b.QuoteVolume.String(), "volume_usd": decOrNil(b.VolumeUSD),
	}
	if b.Buckets == nil {
		r["size_buckets"] = nil
	} else {
		buckets := map[string]any{}
		for i := range b.Buckets {
			buckets["B"+string(rune('1'+i))] = b.Buckets[i].dict()
		}
		r["size_buckets"] = buckets
	}
	if !b.fromEmpty || b.notionalReasonSet {
		r["notional_reason"] = nilIf(b.NotionalReason)
	}
	if !b.fromEmpty {
		r["trade_aggregation"] = b.TradeAggregation
	}
	if b.ZeroTrade {
		r["zero_trade_minute"] = true
	}
	return r
}

func emptySegment() map[string]any {
	buckets := map[string]any{}
	for i := 0; i < 5; i++ {
		buckets["B"+string(rune('1'+i))] = map[string]any{
			"buy_qty": "0", "sell_qty": "0", "buy_notional": "0", "sell_notional": "0", "n": int64(0),
		}
	}
	return map[string]any{
		"buy_qty": "0", "sell_qty": "0", "buy_notional": "0", "sell_notional": "0",
		"n_buy": int64(0), "n_sell": int64(0), "sum_sq_qty": "0",
		"open": nil, "high": nil, "low": nil, "close": nil, "vwap": nil,
		"size_buckets": buckets, "first_trade": nil, "last_trade": nil,
	}
}

func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if inner, ok := v.(map[string]any); ok {
			v = copyMap(inner)
		}
		out[k] = v
	}
	return out
}

// Record is FlowAccumulator.record(key, complete=complete, segments=segments) with the
// collection clock at now. complete nil leaves the bar's flag unchanged.
func (a *Accumulator) Record(key Key, complete *bool, segments bool, now time.Time) map[string]any {
	b := a.Bars[key]
	if complete != nil {
		b.Complete = *complete && b.GapReason == ""
	}
	r := b.fields()
	for k, v := range pytime.Fields("minute_start", r["minute_start"], pytime.Bucket60s) {
		r[k] = v
	}
	pytime.CollectionClock(now).Into(r, "received_ts")
	reasons := map[string]any{}
	for _, pair := range [][2]string{{"buy_usd", "buy_notional"}, {"sell_usd", "sell_notional"}, {"size_buckets", "size_buckets"}} {
		if r[pair[1]] == nil {
			reasons[pair[0]], reasons[pair[1]] = "INCOMPLETE", "INCOMPLETE"
		}
	}
	r["null_reasons"] = reasons
	if !segments {
		return r
	}
	minuteStart := key.MinuteTime()
	end := minuteStart.Add(time.Minute)
	var points []time.Time
	for _, p := range a.Boundaries[key] {
		if !p.Before(minuteStart) && !p.After(end) {
			points = append(points, p)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Before(points[j]) })
	type interval struct{ lo, hi time.Time }
	seen := map[[2]int64]interval{}
	addInterval := func(lo, hi time.Time) { seen[[2]int64{lo.UnixMicro(), hi.UnixMicro()}] = interval{lo, hi} }
	for _, p := range points {
		if p.After(minuteStart) {
			addInterval(minuteStart, p)
		}
		if p.Before(end) {
			addInterval(p, end)
		}
	}
	adjacent := uniqueSorted(append([]time.Time{minuteStart, end}, points...))
	for i := 0; i+1 < len(adjacent); i++ {
		addInterval(adjacent[i], adjacent[i+1])
	}
	segmentsList := []any{}
	if len(points) > 0 {
		intervals := make([]interval, 0, len(seen))
		for _, iv := range seen {
			intervals = append(intervals, iv)
		}
		sort.Slice(intervals, func(i, j int) bool {
			if !intervals[i].lo.Equal(intervals[j].lo) {
				return intervals[i].lo.Before(intervals[j].lo)
			}
			return intervals[i].hi.Before(intervals[j].hi)
		})
		for _, iv := range intervals {
			var captured []*normalize.Trade
			for _, t := range b.Trades {
				if !t.TS.Before(iv.lo) && t.TS.Before(iv.hi) {
					captured = append(captured, t)
				}
			}
			var segment map[string]any
			if len(captured) > 0 {
				sub := New()
				for _, t := range captured {
					sub.Add(t, false)
				}
				segment = sub.Record(KeyOf(key.IID, captured[0].TS), nil, false, now)
			} else {
				segment = emptySegment()
			}
			known := Covered(iv.lo, iv.hi, b.Coverage)
			segment["instrument_id"] = key.IID
			for k, v := range pytime.Fields("start", pytime.ISO(iv.lo), pytime.Exact) {
				segment[k] = v
			}
			for k, v := range pytime.Fields("end", pytime.ISO(iv.hi), pytime.Exact) {
				segment[k] = v
			}
			segment["complete"] = known
			pytime.CollectionClock(now).Into(segment, "observed_at")
			segment["coverage_reason"] = nil
			if !known {
				segment["coverage_reason"] = "INCOMPLETE"
			}
			segmentsList = append(segmentsList, segment)
		}
	}
	r["boundary_segments"] = segmentsList
	refs := []any{}
	boundaries := append([]time.Time(nil), points...)
	if b.LastTrade == nil {
		boundaries = append(boundaries, end)
	}
	for _, boundary := range uniqueSorted(boundaries) {
		explicit := a.References[RefKey(key.IID, boundary)]
		var lastTrade *normalize.Trade
		for _, t := range b.Trades {
			if t.TS.Before(boundary) && (lastTrade == nil || t.TS.After(lastTrade.TS)) {
				lastTrade = t
			}
		}
		var earlier []*TradePoint
		if lastTrade != nil {
			earlier = append(earlier, PointOf(lastTrade))
		}
		if b.PriorTrade != nil {
			earlier = append(earlier, b.PriorTrade)
		}
		var latest *TradePoint
		for _, p := range earlier {
			if latest == nil || p.TS.After(latest.TS) {
				latest = p
			}
		}
		known := latest != nil && Covered(minuteStart, boundary, b.Coverage)
		if explicit != nil && known && latest != nil {
			lt := pyval.Map(explicit["last_trade"])
			if lt == nil {
				explicit = nil
			} else if ets, err := pytime.Parse(lt["ts"]); err == nil && latest.TS.After(ets) {
				explicit = nil
			}
		}
		var point map[string]any
		if explicit != nil {
			point = copyMap(explicit)
		} else {
			point = map[string]any{"last_trade": latest.Map(), "complete": known, "coverage_reason": nil}
			if !known {
				point["coverage_reason"] = "INCOMPLETE"
			}
			for k, v := range pytime.Fields("boundary", pytime.ISO(boundary), pytime.Exact) {
				point[k] = v
			}
			pytime.CollectionClock(now).Into(point, "observed_at")
		}
		point["instrument_id"] = key.IID
		refs = append(refs, point)
	}
	r["reference_points"] = refs
	return r
}

func uniqueSorted(ts []time.Time) []time.Time {
	set := map[int64]time.Time{}
	for _, t := range ts {
		set[t.UnixMicro()] = t
	}
	out := make([]time.Time, 0, len(set))
	for _, t := range set {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// MarkGap is mark_gap: bars of [floor(start), end] become incomplete with reason.
func (a *Accumulator) MarkGap(iid string, start, end time.Time, reason string) {
	lo := pytime.MinuteFloor(start)
	for key, b := range a.Bars {
		m := key.MinuteTime()
		if key.IID == iid && !m.Before(lo) && !m.After(end) {
			b.GapReason, b.Complete = reason, false
			a.Dirty[key] = true
		}
	}
}
