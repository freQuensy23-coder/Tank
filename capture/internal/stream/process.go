package stream

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/books"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// Mark is one save_mark call.
type Mark struct {
	Price, TS, Index, IndexTS any
	Persist                   bool
}

// Sink receives what process_frame produces; the service implements it.
type Sink interface {
	Trade(inst *venue.Instrument, raw any) error
	BookUpdated(inst *venue.Instrument, book *books.Book) error
	Liquidation(inst *venue.Instrument, raw map[string]any) error
	Mark(inst *venue.Instrument, m Mark) error
	// OKXIndex is the index-tickers branch: the index joins the instrument's price
	// context and, with a mark already held, produces a mark observation.
	OKXIndex(inst *venue.Instrument, idxPx, ts any, persist func() bool) error
	Funding(inst *venue.Instrument, raw map[string]any, kind string, interval any) error
	OI(inst *venue.Instrument, raw map[string]any) error
	ContextDue(iid, kind string) bool
	SummaryDue(iid string) bool
	Now() time.Time
}

// Member is the per-(socket, instrument) state process_frame reads.
type Member struct {
	Inst      *venue.Instrument
	Book      *books.Book
	Bridging  bool // Binance: the first depth event must bridge the REST snapshot
	TickerCtx map[string]any
}

// ErrSkip marks a frame the snapshot already contains (no watermark either).
var ErrSkip = errors.New("frame already in snapshot")

func need(m map[string]any, k string) (any, error) { return pyval.Need(m, k) }

func list(v any) []any { return pyval.List(v) }

// Process is streams.process_frame for one routed payload. It returns ErrSkip for a
// Binance depth event older than the snapshot (Python returns before the watermark).
func Process(sink Sink, mem *Member, raw any) error {
	inst := mem.Inst
	iid := inst.ID
	key := inst.Key()
	book := mem.Book
	rm := pyval.Map(raw)
	if rm == nil {
		return fmt.Errorf("frame is not an object")
	}
	data := pyval.GetOr(rm, "data", raw)
	dm := pyval.Map(data)
	switch {
	case strings.HasPrefix(key, "binance"):
		if dm == nil {
			return fmt.Errorf("binance frame without data object")
		}
		switch dm["e"] {
		case "aggTrade":
			return sink.Trade(inst, data)
		case "depthUpdate":
			if book == nil {
				return fmt.Errorf("binance depth without a book")
			}
			u, err := intField(dm, "u")
			if err != nil {
				return err
			}
			seq, err := pyval.Int(book.Sequence)
			if err != nil {
				return err
			}
			offset := int64(0)
			if !mem.Bridging || strings.HasSuffix(key, "spot") {
				offset = 1
			}
			if u < seq+offset {
				return ErrSkip
			}
			if mem.Bridging {
				target := seq
				if strings.HasSuffix(key, "spot") {
					target++
				}
				first, err := intField(dm, "U")
				if err != nil {
					return err
				}
				if !(first <= target && target <= u) {
					return pyval.Semantic("initial Binance snapshot bridge missing")
				}
				mem.Bridging = false
				if err := book.ApplyLevels(list(dm["b"]), list(dm["a"])); err != nil {
					return err
				}
				book.Sequence = dm["u"]
				iso, err := pytime.ISOOf(dm["E"])
				if err != nil {
					return err
				}
				book.ExchangeTS, book.ExchangeTSPrec = iso, pytime.Exact
			} else {
				for _, k := range []string{"b", "a", "u", "U", "E"} {
					if _, err := need(dm, k); err != nil {
						return err
					}
				}
				if _, err := book.Update(list(dm["b"]), list(dm["a"]), dm["u"], dm["pu"], dm["U"], dm["E"], nil, false); err != nil {
					return err
				}
			}
			return sink.BookUpdated(inst, book)
		case "forceOrder":
			return sink.Liquidation(inst, dm)
		case "markPriceUpdate":
			due := sink.ContextDue(iid, "mark")
			p, err := need(dm, "p")
			if err != nil {
				return err
			}
			e, err := need(dm, "E")
			if err != nil {
				return err
			}
			if err := sink.Mark(inst, Mark{Price: p, TS: e, Index: dm["i"], IndexTS: e, Persist: due}); err != nil {
				return err
			}
			if due && pyval.Truthy(inst.FundingIntervalH) {
				r, err := need(dm, "r")
				if err != nil {
					return err
				}
				return sink.Funding(inst, map[string]any{"lastFundingRate": r, "time": e}, "CURRENT_ESTIMATE", inst.FundingIntervalH)
			}
		}
		return nil
	case strings.HasPrefix(key, "coinbase"):
		switch rm["type"] {
		case "match":
			return sink.Trade(inst, raw)
		case "snapshot":
			now := sink.Now()
			if err := book.Snapshot(list(rm["bids"]), list(rm["asks"]), int64(0), pytime.ISO(now), nil, pytime.CollectionOnly); err != nil {
				return err
			}
			return sink.BookUpdated(inst, book)
		case "l2update":
			if book.Sequence == nil {
				return nil // updates before this subscription's snapshot belong to no book yet
			}
			var bids, asks []any
			for _, r := range list(rm["changes"]) {
				row := list(r)
				if len(row) < 1 {
					continue
				}
				switch row[0] {
				case "buy":
					bids = append(bids, row[1:])
				case "sell":
					asks = append(asks, row[1:])
				}
			}
			seq, err := pyval.Int(book.Sequence)
			if err != nil {
				return err
			}
			t, err := need(rm, "time")
			if err != nil {
				return err
			}
			if _, err := book.Update(bids, asks, seq+1, nil, nil, t, nil, false); err != nil {
				return err
			}
			return sink.BookUpdated(inst, book)
		}
		return nil
	case strings.HasPrefix(key, "okx"):
		channel := pyval.Map(rm["arg"])["channel"]
		for _, row := range list(data) {
			item := pyval.Map(row)
			if item == nil {
				continue
			}
			expected := inst.Symbol
			if channel == "index-tickers" {
				expected = strings.TrimSuffix(inst.Symbol, "-SWAP")
			}
			if pyval.GetOr(item, "instId", expected) != expected {
				continue
			}
			switch channel {
			case "trades":
				if err := sink.Trade(inst, item); err != nil {
					return err
				}
			case "books":
				for _, k := range []string{"bids", "asks", "seqId", "ts"} {
					if _, err := need(item, k); err != nil {
						return err
					}
				}
				if rm["action"] == "snapshot" {
					if err := book.Snapshot(list(item["bids"]), list(item["asks"]), item["seqId"], item["ts"], item["checksum"], ""); err != nil {
						return err
					}
				} else {
					prev, err := need(item, "prevSeqId")
					if err != nil {
						return err
					}
					if _, err := book.Update(list(item["bids"]), list(item["asks"]), item["seqId"], prev, nil, item["ts"], item["checksum"], sink.SummaryDue(iid)); err != nil {
						return err
					}
				}
				if err := sink.BookUpdated(inst, book); err != nil {
					return err
				}
			case "open-interest":
				if !sink.ContextDue(iid, "oi") {
					continue
				}
				if err := sink.OI(inst, item); err != nil {
					return err
				}
			case "funding-rate":
				prev, cur := item["prevFundingTime"], item["fundingTime"]
				if pyval.Truthy(prev) && pyval.Truthy(cur) && sink.ContextDue(iid, "funding") {
					a, err := pydec.Parse(pyval.Str(cur))
					if err != nil {
						return err
					}
					b, err := pydec.Parse(pyval.Str(prev))
					if err != nil {
						return err
					}
					interval := pydec.MustDiv(pydec.Sub(a, b, pydec.DefaultPrec), pydec.FromInt(3600000), pydec.DefaultPrec)
					if err := sink.Funding(inst, item, "CURRENT_ESTIMATE", interval); err != nil {
						return err
					}
				}
			case "mark-price":
				px, err := need(item, "markPx")
				if err != nil {
					return err
				}
				ts, err := need(item, "ts")
				if err != nil {
					return err
				}
				if err := sink.Mark(inst, Mark{Price: px, TS: ts, Persist: sink.ContextDue(iid, "mark")}); err != nil {
					return err
				}
			case "index-tickers":
				px, err := need(item, "idxPx")
				if err != nil {
					return err
				}
				ts, err := need(item, "ts")
				if err != nil {
					return err
				}
				if err := sink.OKXIndex(inst, px, ts, func() bool { return sink.ContextDue(iid, "mark") }); err != nil {
					return err
				}
			case "liquidation-orders":
				for _, d := range list(pyval.GetOr(item, "details", []any{})) {
					if dm := pyval.Map(d); dm != nil {
						if err := sink.Liquidation(inst, dm); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	case strings.HasPrefix(key, "bybit"):
		topic, _ := pyval.GetOr(rm, "topic", "").(string)
		switch {
		case strings.HasPrefix(topic, "publicTrade."):
			for _, row := range list(data) {
				item := pyval.Map(row)
				if item == nil {
					return fmt.Errorf("bybit trade row is not an object")
				}
				if rm["cs"] != nil {
					copyItem := make(map[string]any, len(item)+1)
					for k, v := range item {
						copyItem[k] = v
					}
					copyItem["_stream_sequence"] = rm["cs"]
					item = copyItem
				}
				if err := sink.Trade(inst, item); err != nil {
					return err
				}
			}
		case strings.HasPrefix(topic, "orderbook."):
			if dm == nil {
				return fmt.Errorf("bybit book without data")
			}
			for _, k := range []string{"b", "a", "u"} {
				if _, err := need(dm, k); err != nil {
					return err
				}
			}
			ts := pyval.GetOr(rm, "cts", rm["ts"])
			u, _ := pyval.Int(dm["u"])
			if rm["type"] == "snapshot" || u == 1 {
				if err := book.Snapshot(list(dm["b"]), list(dm["a"]), dm["u"], ts, nil, ""); err != nil {
					return err
				}
			} else if _, err := book.Update(list(dm["b"]), list(dm["a"]), dm["u"], nil, nil, ts, nil, false); err != nil {
				return err
			}
			return sink.BookUpdated(inst, book)
		case strings.HasPrefix(topic, "allLiquidation."):
			for _, row := range list(data) {
				if item := pyval.Map(row); item != nil {
					if err := sink.Liquidation(inst, item); err != nil {
						return err
					}
				}
			}
		case strings.HasPrefix(topic, "tickers."):
			if dm == nil {
				return fmt.Errorf("bybit tickers without data")
			}
			if mem.TickerCtx == nil || rm["type"] == "snapshot" {
				mem.TickerCtx = map[string]any{}
			}
			for k, v := range dm {
				mem.TickerCtx[k] = v
			}
			if (pyval.Truthy(dm["markPrice"]) || pyval.Truthy(dm["indexPrice"])) && mem.TickerCtx["markPrice"] != nil {
				ts, err := need(rm, "ts")
				if err != nil {
					return err
				}
				if err := sink.Mark(inst, Mark{
					Price: mem.TickerCtx["markPrice"], TS: ts, Index: mem.TickerCtx["indexPrice"], IndexTS: ts,
					Persist: sink.ContextDue(iid, "mark"),
				}); err != nil {
					return err
				}
			}
			if pyval.Truthy(dm["fundingRate"]) && pyval.Truthy(inst.FundingIntervalH) && sink.ContextDue(iid, "funding") {
				return sink.Funding(inst, dm, "CURRENT_ESTIMATE", inst.FundingIntervalH)
			}
		}
		return nil
	case strings.HasPrefix(key, "kraken"):
		channel := rm["channel"]
		for _, row := range list(data) {
			item := pyval.Map(row)
			if item == nil {
				continue
			}
			switch channel {
			case "trade":
				if err := sink.Trade(inst, item); err != nil {
					return err
				}
			case "book":
				if rm["type"] == "snapshot" {
					for _, k := range []string{"bids", "asks"} {
						if _, err := need(item, k); err != nil {
							return err
						}
					}
					if err := book.Snapshot(list(item["bids"]), list(item["asks"]), int64(0), item["timestamp"], item["checksum"], ""); err != nil {
						return err
					}
				} else if book.Sequence == nil {
					continue // updates before this subscription's snapshot belong to no book yet
				} else {
					seq, err := pyval.Int(book.Sequence)
					if err != nil {
						return err
					}
					ts, err := need(item, "timestamp")
					if err != nil {
						return err
					}
					if _, err := book.Update(list(pyval.GetOr(item, "bids", []any{})), list(pyval.GetOr(item, "asks", []any{})),
						seq+1, nil, nil, ts, item["checksum"], sink.SummaryDue(iid)); err != nil {
						return err
					}
				}
				if err := sink.BookUpdated(inst, book); err != nil {
					return err
				}
			}
		}
		return nil
	}
	switch rm["channel"] {
	case "trades":
		for _, row := range list(data) {
			if err := sink.Trade(inst, row); err != nil {
				return err
			}
		}
	case "l2Book":
		if dm == nil {
			return fmt.Errorf("hyperliquid book without data")
		}
		levels := list(dm["levels"])
		if len(levels) != 2 {
			return fmt.Errorf("hyperliquid book levels")
		}
		side := func(rows []any) ([]any, error) {
			out := make([]any, 0, len(rows))
			for _, r := range rows {
				m := pyval.Map(r)
				px, err := need(m, "px")
				if err != nil {
					return nil, err
				}
				sz, err := need(m, "sz")
				if err != nil {
					return nil, err
				}
				out = append(out, []any{px, sz})
			}
			return out, nil
		}
		bids, err := side(list(levels[0]))
		if err != nil {
			return err
		}
		asks, err := side(list(levels[1]))
		if err != nil {
			return err
		}
		t, err := need(dm, "time")
		if err != nil {
			return err
		}
		if err := book.Snapshot(bids, asks, t, t, nil, ""); err != nil {
			return err
		}
		return sink.BookUpdated(inst, book)
	case "activeAssetCtx":
		ctx := pyval.Map(dm["ctx"])
		if ctx == nil {
			return fmt.Errorf("hyperliquid context without ctx")
		}
		due := sink.ContextDue(iid, "context")
		if due {
			if err := sink.OI(inst, ctx); err != nil {
				return err
			}
			if err := sink.Funding(inst, ctx, "CURRENT_ESTIMATE", "1"); err != nil {
				return err
			}
		}
		px, err := need(ctx, "markPx")
		if err != nil {
			return err
		}
		now := pytime.ISO(sink.Now())
		return sink.Mark(inst, Mark{Price: px, TS: now, Index: ctx["oraclePx"], IndexTS: now, Persist: due})
	}
	return nil
}

func intField(m map[string]any, k string) (int64, error) {
	v, err := need(m, k)
	if err != nil {
		return 0, err
	}
	return pyval.Int(v)
}

// WatermarkStamps are the exchange times a frame carries (record_stream_watermark):
// observed public socket progress, never next-funding or event times.
func WatermarkStamps(inst *venue.Instrument, raw any) []any {
	rm := pyval.Map(raw)
	if rm == nil {
		return nil
	}
	key := inst.Key()
	data := pyval.GetOr(rm, "data", raw)
	var stamps []any
	switch {
	case key == "binance.usdm" && pyval.Map(data)["e"] == "depthUpdate":
		return nil // independent public depth cannot prove regular trade/liquidation continuity
	case strings.HasPrefix(key, "binance"):
		if e := pyval.Map(data)["E"]; e != nil {
			stamps = append(stamps, e)
		}
	case strings.HasPrefix(key, "bybit"):
		if s := pyval.GetOr(rm, "cts", rm["ts"]); s != nil {
			stamps = append(stamps, s)
		}
	case strings.HasPrefix(key, "okx"):
		for _, row := range list(data) {
			item := pyval.Map(row)
			if item["ts"] != nil {
				stamps = append(stamps, item["ts"])
			}
			for _, d := range list(item["details"]) {
				if ts := pyval.Map(d)["ts"]; ts != nil {
					stamps = append(stamps, ts)
				}
			}
		}
	case strings.HasPrefix(key, "coinbase"):
		if pyval.Truthy(rm["time"]) {
			stamps = append(stamps, rm["time"])
		}
	case strings.HasPrefix(key, "kraken"):
		latest := ""
		for _, row := range list(data) {
			if s, ok := pyval.Map(row)["timestamp"].(string); ok && s > latest {
				latest = s // one ISO format per feed: the latest string is the latest time
			}
		}
		if latest != "" {
			stamps = append(stamps, latest)
		}
	default:
		if d := pyval.Map(data); d != nil {
			if d["time"] != nil {
				stamps = append(stamps, d["time"])
			}
		} else {
			for _, row := range list(data) {
				if t := pyval.Map(row)["time"]; t != nil {
					stamps = append(stamps, t)
				}
			}
		}
	}
	return stamps
}
