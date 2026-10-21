package normalize

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const P = pydec.Precise

var (
	eight  = pydec.FromInt(8)
	minute = time.Minute
	pyStr  = pyval.Str
	pyDec  = pyval.Decimal
	errSem = pyval.Semantic
	isTrue = pyval.Truthy
	keyOf  = pyval.Need
	getOr  = pyval.GetOr
	asMap  = pyval.Map
	asList = pyval.List
)

func none(s string) any {
	if s == "" {
		return nil
	}
	return s
}

type Trade struct {
	InstrumentID      string
	TokenID           string
	Venue             string
	VenueSymbol       any
	MarketType        string
	TradeID           string
	TS                time.Time
	TSPrec            string
	Received          pytime.Timestamp
	Side              string
	Qty               pydec.Dec
	Price             pydec.Dec
	QuoteNotional     pydec.Dec
	NotionalUSD       *pydec.Dec
	QuoteAsset        string
	MetricKey         string
	TradeAggregation  string
	RawFirstID        any
	RawLastID         any
	Sequence          any
	SequenceSemantics string
	CaptureSource     string
	SourcePayload     any
}

func (t *Trade) TSISO() string { return pytime.ISO(t.TS) }

func (t *Trade) Record() map[string]any {
	var usd any
	usdReason := any("QUOTE_USD_RATE_UNAVAILABLE")
	if t.NotionalUSD != nil {
		usd, usdReason = t.NotionalUSD.String(), nil
	}
	r := map[string]any{
		"instrument_id":       t.InstrumentID,
		"token_id":            t.TokenID,
		"venue":               t.Venue,
		"venue_symbol":        t.VenueSymbol,
		"market_type":         t.MarketType,
		"trade_id":            t.TradeID,
		"source_payload_json": pyjson.Canonical(t.SourcePayload),
		"side":                t.Side,
		"qty_native":          t.Qty.String(),
		"price":               t.Price.String(),
		"quote_notional":      t.QuoteNotional.String(),
		"notional_usd":        usd,
		"quote_asset":         t.QuoteAsset,
		"usd_reason":          usdReason,
		"metric_key":          t.MetricKey,
		"trade_aggregation":   t.TradeAggregation,
		"raw_first_id":        jsonable(t.RawFirstID),
		"raw_last_id":         jsonable(t.RawLastID),
		"sequence":            jsonable(t.Sequence),
		"sequence_semantics":  t.SequenceSemantics,
		"capture_source":      t.CaptureSource,
	}
	for k, v := range pytime.Fields("ts", pytime.ISO(t.TS), t.TSPrec) {
		r[k] = v
	}
	t.Received.Into(r, "received_ts")
	return r
}

func jsonable(v any) any {
	if n, ok := v.(json.Number); ok {
		if pyval.IsInt(n) {
			i, err := n.Int64()
			if err == nil {
				return i
			}
		}
		return pyStr(n)
	}
	return v
}

var TradeAggregations = map[string]string{
	"binance": "PER_TAKER_ORDER_SAME_PRICE",
	"okx":     "PER_TAKER_ORDER_SAME_PRICE",
}

func NormalizeTrade(inst *venue.Instrument, raw any, received time.Time, source string, fx *FX) (*Trade, error) {
	key := inst.Key()
	venueName := strings.SplitN(key, ".", 2)[0]
	aggregation := "PER_FILL"
	var side string
	var qtyRaw, priceRaw, tsRaw, tid any
	var qty pydec.Dec
	qtyDone := false
	m := asMap(raw)
	need := func(k string) (any, error) {
		if m == nil {
			return nil, fmt.Errorf("trade frame is not an object")
		}
		return keyOf(m, k)
	}
	var err error
	switch venueName {
	case "binance":
		mv, e := need("m")
		if e != nil {
			return nil, e
		}
		maker, isBool := mv.(bool)
		if !isBool {
			return nil, errSem("buyer-is-maker must be boolean")
		}
		side = "BUY"
		if maker {
			side = "SELL"
		}
		if qtyRaw, err = need("q"); err == nil {
			if priceRaw, err = need("p"); err == nil {
				if tsRaw, err = need("T"); err == nil {
					tid, err = need("a")
				}
			}
		}
		aggregation = "PER_TAKER_ORDER_SAME_PRICE"
	case "coinbase":
		s, e := need("side")
		if e != nil {
			return nil, e
		}
		if s != "buy" && s != "sell" {
			return nil, errSem("unknown Coinbase maker side")
		}
		side = "SELL"
		if s == "sell" {
			side = "BUY"
		}
		if qtyRaw, err = need("size"); err == nil {
			if priceRaw, err = need("price"); err == nil {
				if tsRaw, err = need("time"); err == nil {
					tid, err = need("trade_id")
				}
			}
		}
	case "okx":
		s, e := need("side")
		if e != nil {
			return nil, e
		}
		if s != "buy" && s != "sell" {
			return nil, errSem("unknown OKX taker side")
		}
		side = strings.ToUpper(s.(string))
		if qtyRaw, err = need("sz"); err == nil {
			if priceRaw, err = need("px"); err == nil {
				if tsRaw, err = need("ts"); err == nil {
					tid, err = need("tradeId")
				}
			}
		}
		if err == nil && key == "okx.swap" {
			if inst.ContractMultiplier == nil {
				return nil, errSem("OKX ctVal base multiplier unverified")
			}
			q, e1 := pyDec(qtyRaw)
			cm, e2 := pyDec(inst.ContractMultiplier)
			if e1 != nil {
				return nil, e1
			}
			if e2 != nil {
				return nil, e2
			}
			qty, qtyDone = pydec.Mul(q, cm, P), true
		}
		aggregation = "PER_TAKER_ORDER_SAME_PRICE"
	case "bybit":
		if m == nil {
			return nil, fmt.Errorf("trade frame is not an object")
		}
		s := getOr(m, "S", m["side"])
		if s != "Buy" && s != "Sell" {
			return nil, errSem("unknown Bybit taker side")
		}
		side = strings.ToUpper(s.(string))
		qtyRaw = getOr(m, "v", m["size"])
		priceRaw = getOr(m, "p", m["price"])
		tsRaw = getOr(m, "T", m["time"])
		tid = getOr(m, "i", m["execId"])
	case "kraken":
		var s any
		if row := asList(raw); row != nil {
			if len(row) < 7 {
				return nil, errSem("Kraken trade ID absent")
			}
			priceRaw, qtyRaw, tsRaw, s, tid = row[0], row[1], row[2], row[3], row[6]
		} else {
			if priceRaw, err = need("price"); err == nil {
				if qtyRaw, err = need("qty"); err == nil {
					if tsRaw, err = need("timestamp"); err == nil {
						if s, err = need("side"); err == nil {
							tid, err = need("trade_id")
						}
					}
				}
			}
			if err != nil {
				return nil, err
			}
		}
		switch s {
		case "buy", "b":
			side = "BUY"
		case "sell", "s":
			side = "SELL"
		default:
			return nil, errSem("unknown Kraken taker side")
		}
	case "hyperliquid":
		s, e := need("side")
		if e != nil {
			return nil, e
		}
		if s != "B" && s != "A" {
			return nil, errSem("unknown Hyperliquid side")
		}
		side = "SELL"
		if s == "B" {
			side = "BUY"
		}
		if qtyRaw, err = need("sz"); err == nil {
			if priceRaw, err = need("px"); err == nil {
				if tsRaw, err = need("time"); err == nil {
					tid, err = need("tid")
				}
			}
		}
	default:
		return nil, errSem("unsupported venue %s", venueName)
	}
	if err != nil {
		return nil, err
	}
	if tid == nil {
		return nil, errSem("missing trade ID")
	}
	if !qtyDone {
		if qty, err = pyDec(qtyRaw); err != nil {
			return nil, err
		}
	}
	price, err := pyDec(priceRaw)
	if err != nil {
		return nil, err
	}
	if qty.Sign() <= 0 || price.Sign() <= 0 {
		return nil, errSem("nonpositive trade")
	}
	ts, err := pytime.Parse(tsRaw)
	if err != nil {
		return nil, err
	}
	t := &Trade{
		InstrumentID:     inst.ID,
		TokenID:          inst.TokenID,
		Venue:            venueName,
		VenueSymbol:      inst.Symbol,
		MarketType:       inst.MarketType,
		TradeID:          pyStr(tid),
		TS:               ts,
		TSPrec:           pytime.Exact,
		Received:         pytime.CollectionClock(received),
		Side:             side,
		Qty:              qty,
		Price:            price,
		QuoteNotional:    pydec.Mul(qty, price, P),
		QuoteAsset:       inst.Quote(),
		TradeAggregation: aggregation,
		CaptureSource:    source,
		SourcePayload:    raw,
	}
	if rate, _, ok := fx.Rate(inst, ts); ok {
		r, err := pyDec(rate)
		if err != nil {
			return nil, err
		}
		usd := pydec.Mul(pydec.Mul(qty, price, P), r, P)
		t.NotionalUSD = &usd
	}
	switch venueName {
	case "binance":
		t.MetricKey = key + ".aggTrade"
	case "okx", "hyperliquid":
		t.MetricKey = key + ".trades"
	case "coinbase":
		if source == "REST" {
			t.MetricKey = key + ".trades"
		} else {
			t.MetricKey = key + ".matches"
		}
	case "bybit":
		t.MetricKey = key + ".publicTrade"
	default:
		t.MetricKey = key + ".trade"
	}
	if m != nil {
		t.RawFirstID, t.RawLastID = m["f"], m["l"]
		t.Sequence = getOr(m, "seq", getOr(m, "seqId", m["_stream_sequence"]))
	}
	t.SequenceSemantics = "N_A"
	if venueName == "okx" || venueName == "bybit" {
		t.SequenceSemantics = "MONOTONIC"
	}
	return t, nil
}

func KlineRecord(inst *venue.Instrument, row any, now time.Time, fx *FX) (map[string]any, error) {
	key := inst.Key()
	extra := map[string]any{}
	var ts, o, h, low, c, v any
	var complete bool
	list := asList(row)
	at := func(i int) any {
		if i < len(list) {
			return list[i]
		}
		return nil
	}
	short := func(n int) error {
		if len(list) < n {
			return fmt.Errorf("kline row has %d fields, want %d", len(list), n)
		}
		return nil
	}
	switch {
	case strings.HasPrefix(key, "binance"):
		if err := short(11); err != nil {
			return nil, err
		}
		ts, o, h, low, c, v = at(0), at(1), at(2), at(3), at(4), at(5)
		n, err := pyval.Int(at(8))
		if err != nil {
			return nil, err
		}
		extra = map[string]any{"quote_volume": at(7), "n_trades": n, "taker_buy_native": at(9), "taker_buy_quote": at(10)}
		closed, err := pytime.Parse(at(6))
		if err != nil {
			return nil, err
		}
		complete = closed.Before(now)
	case strings.HasPrefix(key, "okx"):
		if err := short(6); err != nil {
			return nil, err
		}
		ts, o, h, low, c, v = at(0), at(1), at(2), at(3), at(4), at(5)
		if key == "okx.swap" {
			if inst.ContractMultiplier == nil {
				return nil, errSem("OKX multiplier absent")
			}
			vd, err := pyDec(v)
			if err != nil {
				return nil, err
			}
			cm, err := pyDec(inst.ContractMultiplier)
			if err != nil {
				return nil, err
			}
			v = pydec.Mul(vd, cm, P)
		}
		var quote any
		if len(list) > 7 {
			quote = list[7]
		}
		extra = map[string]any{"quote_volume": quote, "n_trades": nil, "taker_buy_native": nil, "taker_buy_quote": nil}
		complete = len(list) > 8 && pyStr(list[8]) == "1"
	case strings.HasPrefix(key, "bybit"):
		if err := short(7); err != nil {
			return nil, err
		}
		ts, o, h, low, c, v = at(0), at(1), at(2), at(3), at(4), at(5)
		t, err := pytime.Parse(ts)
		if err != nil {
			return nil, err
		}
		complete = !t.Add(minute).After(now)
		extra = map[string]any{"quote_volume": at(6), "n_trades": nil, "taker_buy_native": nil, "taker_buy_quote": nil}
	case strings.HasPrefix(key, "coinbase"):
		if err := short(6); err != nil {
			return nil, err
		}
		ts, low, h, o, c, v = at(0), at(1), at(2), at(3), at(4), at(5)
		t, err := pytime.Parse(ts)
		if err != nil {
			return nil, err
		}
		complete = !t.Add(minute).After(now)
	case strings.HasPrefix(key, "kraken"):
		if err := short(8); err != nil {
			return nil, err
		}
		ts, o, h, low, c, v = at(0), at(1), at(2), at(3), at(4), at(6)
		t, err := pytime.Parse(ts)
		if err != nil {
			return nil, err
		}
		complete = !t.Add(minute).After(now)
		n, err := pyval.Int(at(7))
		if err != nil {
			return nil, err
		}
		vwap, err := pyDec(at(5))
		if err != nil {
			return nil, err
		}
		vol, err := pyDec(at(6))
		if err != nil {
			return nil, err
		}
		extra = map[string]any{"n_trades": n, "quote_volume": pydec.Mul(vwap, vol, P), "quote_volume_method": "REPORTED_VWAP_X_NATIVE_VOLUME"}
	case strings.HasPrefix(key, "hyperliquid"):
		m := asMap(row)
		if m == nil {
			return nil, fmt.Errorf("hyperliquid candle is not an object")
		}
		var err error
		for _, f := range []struct {
			dst *any
			k   string
		}{{&ts, "t"}, {&o, "o"}, {&h, "h"}, {&low, "l"}, {&c, "c"}, {&v, "v"}} {
			if *f.dst, err = keyOf(m, f.k); err != nil {
				return nil, err
			}
		}
		closeT, err := keyOf(m, "T")
		if err != nil {
			return nil, err
		}
		closed, err := pytime.Parse(closeT)
		if err != nil {
			return nil, err
		}
		complete = closed.Before(now)
		nv, err := keyOf(m, "n")
		if err != nil {
			return nil, err
		}
		n, err := pyval.Int(nv)
		if err != nil {
			return nil, err
		}
		extra = map[string]any{"n_trades": n}
	default:
		return nil, errSem("unsupported kline")
	}
	openTime, err := pytime.Parse(ts)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"instrument_id": inst.ID, "token_id": inst.TokenID, "complete": complete, "interval_s": int64(60)}
	for k, val := range pytime.Fields("open_time", pytime.ISO(openTime), pytime.Bucket60s) {
		result[k] = val
	}
	for name, raw := range map[string]any{"o": o, "h": h, "l": low, "c": c, "volume_native": v} {
		d, err := pyDec(raw)
		if err != nil {
			return nil, err
		}
		result[name] = d.String()
	}
	pytime.CollectionClock(now).Into(result, "received_ts")
	for _, name := range []string{"quote_volume", "n_trades", "taker_buy_native", "taker_buy_quote"} {
		result[name] = jsonValue(extra[name])
	}
	quote := result["quote_volume"]
	rate, asOf, haveRate := fx.Rate(inst, openTime.Add(minute))
	method, hasMethod := extra["quote_volume_method"]
	if !hasMethod {
		method = nil
		if quote != nil {
			method = "REPORTED"
		}
	}
	result["quote_asset"] = inst.QuoteValue()
	result["quote_volume_method"] = method
	result["quote_volume_usd"] = nil
	switch {
	case quote == nil:
		result["quote_volume_usd_reason"] = "QUOTE_VOLUME_NOT_REPORTED"
	case !haveRate:
		result["quote_volume_usd_reason"] = "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
	default:
		q, err := pyDec(quote)
		if err != nil {
			return nil, err
		}
		r, err := pyDec(rate)
		if err != nil {
			return nil, err
		}
		result["quote_volume_usd"] = pydec.Mul(q, r, P).String()
		result["quote_volume_usd_reason"] = nil
	}
	result["quote_usd_rate"] = nil
	if haveRate {
		result["quote_usd_rate"] = rate
	}
	precision := ""
	var asOfValue any
	if asOf != "" {
		precision, asOfValue = pytime.CollectionOnly, asOf
	}
	for k, val := range pytime.Fields("quote_usd_rate_as_of", asOfValue, precision) {
		result[k] = val
	}
	return result, nil
}

func jsonValue(v any) any {
	switch x := v.(type) {
	case pydec.Dec:
		return x.String()
	case json.Number:
		return jsonable(x)
	}
	return v
}

type OIOptions struct {
	Mark       any
	MarkTS     any
	MarkTSPrec string
	Received   *time.Time
	BucketS    any
}

var (
	bybitBuckets   = []int64{300, 900, 1800, 3600, 14400, 86400}
	binanceBuckets = []int64{300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400}
)

func OIRecord(inst *venue.Instrument, raw map[string]any, opt OIOptions, now time.Time) (map[string]any, error) {
	var received any
	if opt.Received != nil {
		received = *opt.Received
	} else if v := raw["_capture_observed_at"]; isTrue(v) {
		received = v
	}
	key := inst.Key()
	sidedness, reason := "SINGLE_SIDED", ""
	var value, ts any
	var metric string
	var err error
	switch key {
	case "binance.usdm":
		field := "openInterest"
		if pyval.Has(raw, "sumOpenInterest") {
			field = "sumOpenInterest"
		}
		if value, err = keyOf(raw, field); err != nil {
			return nil, err
		}
		ts = getOr(raw, "time", raw["timestamp"])
		metric = key + ".openInterest"
		if field == "sumOpenInterest" {
			metric = key + ".openInterestHist"
		}
	case "okx.swap":
		if value, err = keyOf(raw, "oiCcy"); err != nil {
			return nil, err
		}
		if ts, err = keyOf(raw, "ts"); err != nil {
			return nil, err
		}
		metric = key + ".open-interest"
	case "bybit.linear":
		if !pyval.Has(raw, "singleOpenInterest") {
			return nil, errSem("Bybit singleOpenInterest absent; openInterest cannot substitute")
		}
		value, ts = raw["singleOpenInterest"], raw["timestamp"]
		metric = key + ".open-interest"
	case "hyperliquid.perp":
		if value, err = keyOf(raw, "openInterest"); err != nil {
			return nil, err
		}
		ts = received
		if !isTrue(ts) {
			ts = pytime.ISO(now)
		}
		metric = key + ".ctx"
		sidedness, reason = "UNKNOWN", "SIDEDNESS_UNPUBLISHED"
	default:
		return nil, errSem("spot has no OI")
	}
	if ts == nil {
		return nil, errSem("OI source timestamp absent")
	}
	bucketed := strings.Contains(metric, "Hist")
	bucket := opt.BucketS
	if bucket == nil {
		bucket = int64(300)
	}
	var bucketS int64
	if bucketed || key == "bybit.linear" {
		allowed := binanceBuckets
		if key == "bybit.linear" {
			allowed = bybitBuckets
		}
		if _, isBool := bucket.(bool); isBool {
			return nil, errSem("OI bucket must match a documented source request period in seconds")
		}
		d, err := pyDec(jsonValue(bucket))
		if err != nil {
			return nil, err
		}
		match := false
		for _, a := range allowed {
			if d.Equal(pydec.FromInt(a)) {
				match, bucketS = true, a
			}
		}
		if !match {
			return nil, errSem("OI bucket must match a documented source request period in seconds")
		}
	}
	precision := pytime.Exact
	if key == "hyperliquid.perp" {
		precision = pytime.CollectionOnly
	} else if bucketed {
		precision = fmt.Sprintf("BUCKET(%ds)", bucketS)
	}
	amount, err := pyDec(value)
	if err != nil {
		return nil, err
	}
	if amount.Sign() < 0 {
		return nil, errSem("negative open interest")
	}
	tsTime, err := pytime.Parse(ts)
	if err != nil {
		return nil, err
	}
	aligned := false
	if opt.Mark != nil && opt.MarkTS != nil {
		mt, err := pytime.Parse(opt.MarkTS)
		if err != nil {
			return nil, err
		}
		d := mt.Sub(tsTime)
		aligned = d <= time.Minute && d >= -time.Minute
	}
	var fxRate any
	if inst.QuoteValue() == "USD" {
		fxRate = "1"
	} else {
		fxRate = inst.QuoteUSDRate
	}
	var usd any
	switch {
	case raw["sumOpenInterestValue"] != nil:
		d, err := pyDec(raw["sumOpenInterestValue"])
		if err != nil {
			return nil, err
		}
		usd = d.String()
	case raw["oiUsd"] != nil:
		d, err := pyDec(raw["oiUsd"])
		if err != nil {
			return nil, err
		}
		usd = d.String()
	case aligned && fxRate != nil:
		mark, err := pyDec(opt.Mark)
		if err != nil {
			return nil, err
		}
		rate, err := pyDec(fxRate)
		if err != nil {
			return nil, err
		}
		usd = pydec.Mul(pydec.Mul(amount, mark, P), rate, P).String()
	}
	var receivedTime time.Time
	if received != nil {
		if receivedTime, err = pytime.Parse(received); err != nil {
			return nil, err
		}
	} else {
		receivedTime = now
	}
	r := map[string]any{
		"instrument_id":         inst.ID,
		"token_id":              inst.TokenID,
		"ts_bucket_size_s":      nil,
		"raw_value":             amount.String(),
		"raw_unit":              "BASE_QTY",
		"sidedness":             sidedness,
		"metric_key":            metric,
		"oi_native":             amount.String(),
		"oi_usd":                usd,
		"oi_usd_reason":         nil,
		"mark_price":            nil,
		"synchronized_eligible": sidedness == "SINGLE_SIDED",
		"gap_reason":            none(reason),
		"timestamp_precision":   precision,
	}
	if usd == nil {
		r["oi_usd_reason"] = "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"
	}
	for k, val := range pytime.Fields("ts", pytime.ISO(tsTime), precision) {
		r[k] = val
	}
	pytime.CollectionClock(receivedTime).Into(r, "received_ts")
	if bucketed {
		r["ts_bucket_size_s"] = bucketS
		r["ts_bucket_anchor"] = "UNSPECIFIED"
		if key == "binance.usdm" {
			r["ts_bucket_anchor"] = "END"
		}
	}
	var markAsOf any
	markPrec := ""
	if aligned {
		r["mark_price"] = pyStr(opt.Mark)
		markAsOf, markPrec = opt.MarkTS, opt.MarkTSPrec
	}
	for k, val := range pytime.Fields("mark_as_of", markAsOf, markPrec) {
		r[k] = val
	}
	return r, nil
}

func FundingRecord(inst *venue.Instrument, raw map[string]any, kind string, intervalH any, received *time.Time, now time.Time) (map[string]any, error) {
	var receivedValue any
	if received != nil {
		receivedValue = *received
	} else if v := raw["_capture_observed_at"]; isTrue(v) {
		receivedValue = v
	}
	fallback := receivedValue
	if !isTrue(fallback) {
		fallback = pytime.ISO(now)
	}
	key := inst.Key()
	interval, err := pyDec(jsonValue(intervalH))
	if err != nil {
		return nil, err
	}
	if interval.Sign() <= 0 {
		return nil, errSem("invalid funding interval")
	}
	settled := kind == "SETTLED"
	var rate, stamp any
	switch {
	case strings.HasPrefix(key, "binance"):
		if settled {
			if rate, err = keyOf(raw, "fundingRate"); err != nil {
				return nil, err
			}
			if stamp, err = keyOf(raw, "fundingTime"); err != nil {
				return nil, err
			}
		} else {
			rate = getOr(raw, "lastFundingRate", raw["r"])
			stamp = getOr(raw, "time", getOr(raw, "E", fallback))
		}
	case strings.HasPrefix(key, "okx"):
		if settled {
			rate = raw["realizedRate"]
			if !isTrue(rate) {
				if rate, err = keyOf(raw, "fundingRate"); err != nil {
					return nil, err
				}
			}
			if stamp, err = keyOf(raw, "fundingTime"); err != nil {
				return nil, err
			}
		} else {
			if rate, err = keyOf(raw, "fundingRate"); err != nil {
				return nil, err
			}
			stamp = getOr(raw, "ts", fallback)
		}
	case strings.HasPrefix(key, "bybit"):
		if rate, err = keyOf(raw, "fundingRate"); err != nil {
			return nil, err
		}
		if settled {
			if stamp, err = keyOf(raw, "fundingRateTimestamp"); err != nil {
				return nil, err
			}
		} else {
			stamp = fallback
		}
	case strings.HasPrefix(key, "hyperliquid"):
		field := "funding"
		if settled {
			field = "fundingRate"
		}
		if rate, err = keyOf(raw, field); err != nil {
			return nil, err
		}
		stamp = getOr(raw, "time", fallback)
	default:
		return nil, errSem("spot has no funding")
	}
	r, err := pyDec(rate)
	if err != nil {
		return nil, err
	}
	sourceClock := settled ||
		strings.HasPrefix(key, "binance") && (raw["time"] != nil || raw["E"] != nil) ||
		strings.HasPrefix(key, "okx") && raw["ts"] != nil ||
		strings.HasPrefix(key, "hyperliquid") && raw["time"] != nil
	at, err := pytime.Parse(stamp)
	if err != nil {
		return nil, err
	}
	rate8h := pydec.MustDiv(pydec.Mul(r, eight, P), interval, P)
	out := map[string]any{
		"instrument_id": inst.ID,
		"token_id":      inst.TokenID,
		"kind":          kind,
		"rate":          r.String(),
		"interval_h":    interval.String(),
		"rate_8h_eq":    rate8h.String(),
		"metric_key":    key + ".funding",
	}
	precision := pytime.CollectionOnly
	if sourceClock {
		precision = pytime.Exact
	}
	for k, val := range pytime.Fields("as_of", pytime.ISO(at), precision) {
		out[k] = val
	}
	if settled {
		for _, f := range []string{"funding_time", "settlement_time"} {
			for k, val := range pytime.Fields(f, pytime.ISO(at), pytime.Exact) {
				out[k] = val
			}
		}
	}
	receivedTime := now
	if receivedValue != nil {
		if receivedTime, err = pytime.Parse(receivedValue); err != nil {
			return nil, err
		}
	}
	pytime.CollectionClock(receivedTime).Into(out, "received_ts")
	return out, nil
}

func LiquidationRecord(inst *venue.Instrument, raw map[string]any, received time.Time) (map[string]any, error) {
	key := inst.Key()
	var side, coverage string
	var qtyRaw, priceRaw, ts any
	var qty pydec.Dec
	qtyDone := false
	var err error
	switch key {
	case "binance.usdm":
		x := raw
		if o, ok := raw["o"]; ok {
			x = asMap(o)
			if x == nil {
				return nil, fmt.Errorf("liquidation order is not an object")
			}
		}
		s := x["S"]
		if s != "BUY" && s != "SELL" {
			return nil, errSem("unknown Binance liquidation order side")
		}
		side = "SHORT_LIQ"
		if s == "SELL" {
			side = "LONG_LIQ"
		}
		if isTrue(x["z"]) {
			qtyRaw = x["z"]
		} else if qtyRaw, err = keyOf(x, "q"); err != nil {
			return nil, err
		}
		if priceRaw, err = keyOf(x, "ap"); err != nil {
			return nil, err
		}
		if ts, err = keyOf(x, "T"); err != nil {
			return nil, err
		}
		coverage = "SAMPLED"
	case "bybit.linear":
		s := raw["S"]
		if s != "Buy" && s != "Sell" {
			return nil, errSem("unknown Bybit liquidated position side")
		}
		side = "SHORT_LIQ"
		if s == "Buy" {
			side = "LONG_LIQ"
		}
		if qtyRaw, err = keyOf(raw, "v"); err != nil {
			return nil, err
		}
		if priceRaw, err = keyOf(raw, "p"); err != nil {
			return nil, err
		}
		if ts, err = keyOf(raw, "T"); err != nil {
			return nil, err
		}
		coverage = "COMPLETE"
	case "okx.swap":
		pos, err := keyOf(raw, "posSide")
		if err != nil {
			return nil, err
		}
		switch pos {
		case "long":
			side = "LONG_LIQ"
		case "short":
			side = "SHORT_LIQ"
		default:
			return nil, errSem("OKX liquidation position side absent")
		}
		if inst.ContractMultiplier == nil {
			return nil, errSem("OKX multiplier absent")
		}
		sz, err := keyOf(raw, "sz")
		if err != nil {
			return nil, err
		}
		q, err := pyDec(sz)
		if err != nil {
			return nil, err
		}
		cm, err := pyDec(inst.ContractMultiplier)
		if err != nil {
			return nil, err
		}
		qty, qtyDone = pydec.Mul(q, cm, P), true
		if priceRaw, err = keyOf(raw, "bkPx"); err != nil {
			return nil, err
		}
		if ts, err = keyOf(raw, "ts"); err != nil {
			return nil, err
		}
		coverage = "SAMPLED"
	default:
		return nil, errSem("liquidations unsupported")
	}
	if !qtyDone {
		if qty, err = pyDec(qtyRaw); err != nil {
			return nil, err
		}
	}
	price, err := pyDec(priceRaw)
	if err != nil {
		return nil, err
	}
	if qty.Sign() < 0 || price.Sign() <= 0 {
		return nil, errSem("invalid liquidation quantity/price")
	}
	at, err := pytime.Parse(ts)
	if err != nil {
		return nil, err
	}
	var fxRate any
	if inst.QuoteValue() == "USD" {
		fxRate = "1"
	} else {
		fxRate = inst.QuoteUSDRate
	}
	out := map[string]any{
		"instrument_id": inst.ID,
		"token_id":      inst.TokenID,
		"side":          side,
		"qty_native":    qty.String(),
		"price":         price.String(),
		"coverage":      coverage,
		"price_kind":    "EXECUTION_AVERAGE",
	}
	if key == "okx.swap" || key == "bybit.linear" {
		out["price_kind"] = "BANKRUPTCY"
	}
	if fxRate != nil {
		rate, err := pyDec(fxRate)
		if err != nil {
			return nil, err
		}
		out["notional_usd"] = pydec.Mul(pydec.Mul(qty, price, P), rate, P).String()
		out["null_reasons"] = map[string]any{}
		out["usd_reason"] = nil
	} else {
		out["notional_usd"] = nil
		out["null_reasons"] = map[string]any{"notional_usd": "INCOMPLETE"}
		out["usd_reason"] = "QUOTE_USD_RATE_UNAVAILABLE"
	}
	for k, val := range pytime.Fields("ts", pytime.ISO(at), pytime.Exact) {
		out[k] = val
	}
	pytime.CollectionClock(received).Into(out, "received_ts")
	return out, nil
}
