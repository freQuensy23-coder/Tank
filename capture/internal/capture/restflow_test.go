package capture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type fakeTrade struct {
	id  int64
	ts  time.Time
	buy bool
}

type fakeVenue struct {
	inst       *venue.Instrument
	trades     []fakeTrade
	retention  time.Time
	calls      int
	multiplier string
}

func (f *fakeVenue) raw(t fakeTrade) any {
	switch f.inst.VenueName() {
	case "coinbase":
		side := "sell"
		if !t.buy {
			side = "buy"
		}
		return map[string]any{"trade_id": json.Number(strconv.FormatInt(t.id, 10)), "time": pytime.ISO(t.ts), "price": "10.5", "size": "2", "side": side}
	case "okx":
		side := "sell"
		if t.buy {
			side = "buy"
		}
		return map[string]any{"instId": f.inst.Symbol, "tradeId": strconv.FormatInt(t.id, 10), "px": "10.5", "sz": "2", "side": side, "ts": strconv.FormatInt(t.ts.UnixMilli(), 10)}
	case "binance":
		return map[string]any{"a": json.Number(strconv.FormatInt(t.id, 10)), "p": "10.5", "q": "2", "T": json.Number(strconv.FormatInt(t.ts.UnixMilli(), 10)), "m": !t.buy}
	case "kraken":
		side := "s"
		if t.buy {
			side = "b"
		}
		secs := strconv.FormatFloat(float64(t.ts.UnixMicro())/1e6, 'f', 6, 64)
		return []any{"10.5", "2", json.Number(secs), side, "l", "", json.Number(strconv.FormatInt(t.id, 10))}
	}
	return nil
}

func (f *fakeVenue) Trades(_ context.Context, start, end *time.Time, cursor any) ([]any, any, error) {
	f.calls++
	var rows []any
	available := func(t fakeTrade) bool { return f.retention.IsZero() || !t.ts.Before(f.retention) }
	switch f.inst.VenueName() {
	case "coinbase", "okx":
		limit := 1000
		if f.inst.VenueName() == "okx" {
			limit = 100
		}
		var below int64 = 1 << 62
		var before time.Time
		switch {
		case cursor != nil:
			below, _ = strconv.ParseInt(text(cursor), 10, 64)
		case end != nil && f.inst.VenueName() == "okx":
			before = end.Add(time.Millisecond)
		}
		var picked []fakeTrade
		for i := len(f.trades) - 1; i >= 0 && len(picked) < limit; i-- {
			t := f.trades[i]
			if t.id < below && (before.IsZero() || t.ts.Before(before)) && available(t) {
				picked = append(picked, t)
			}
		}
		for _, t := range picked {
			rows = append(rows, f.raw(t))
		}
		if len(picked) == 0 {
			return rows, nil, nil
		}
		return rows, strconv.FormatInt(picked[len(picked)-1].id, 10), nil
	case "binance":
		for _, t := range f.trades {
			if len(rows) == 1000 {
				break
			}
			if cursor != nil {
				from, _ := strconv.ParseInt(text(cursor), 10, 64)
				if t.id >= from {
					rows = append(rows, f.raw(t))
				}
				continue
			}
			if (start == nil || !t.ts.Before(*start)) && (end == nil || !t.ts.After(*end)) {
				rows = append(rows, f.raw(t))
			}
		}
		return rows, nil, nil
	case "kraken":
		var since int64
		if cursor != nil {
			since, _ = strconv.ParseInt(text(cursor), 10, 64)
		} else if start != nil {
			since = start.UnixNano()
		}
		last := since
		for _, t := range f.trades {
			if len(rows) == 1000 {
				break
			}
			if t.ts.UnixNano() > since {
				rows = append(rows, f.raw(t))
				last = t.ts.UnixNano()
			}
		}
		return rows, strconv.FormatInt(last, 10), nil
	}
	return nil, nil, errors.New("NO_LISTED_TRADE_BACKFILL")
}

func (f *fakeVenue) Discover(context.Context) (map[string]any, error) {
	if f.multiplier != "" {
		return map[string]any{"contract_multiplier": f.multiplier}, nil
	}
	return map[string]any{"contract_multiplier": "1"}, nil
}
func (f *fakeVenue) Klines(context.Context, *time.Time, *time.Time) ([]any, error) { return nil, nil }
func (f *fakeVenue) Ticker(context.Context) (map[string]any, error) {
	return nil, errors.New("no ticker")
}
func (f *fakeVenue) Derivatives(context.Context) (*Derivatives, error) {
	return nil, errors.New("no derivatives")
}
func (f *fakeVenue) OIHistory(context.Context, time.Time, time.Time, string) ([]any, string, error) {
	return nil, "", nil
}
func (f *fakeVenue) DepthSnapshot(context.Context) (map[string]any, error) {
	return nil, errors.New("no depth")
}
func (f *fakeVenue) IndependentBook(context.Context, map[string]any) (map[string]any, error) {
	return nil, errors.New("no book")
}
func (f *fakeVenue) RecentTrades(context.Context, int) ([]any, error) { return nil, nil }

type fakeVenues struct{ v *fakeVenue }

func (f fakeVenues) Venue(*venue.Instrument, int) Venue             { return f.v }
func (f fakeVenues) FXRate(context.Context, string) (string, error) { return "1", nil }

type restRig struct {
	t     *testing.T
	clock *testClock
	mem   *memStore
	v     *fakeVenue
	s     *Service
	inst  *venue.Instrument
	base  time.Time
}

func newRESTRig(t *testing.T, venueName string) *restRig {
	base := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	inst := &venue.Instrument{ID: venueName + ":spot:TEST", TokenID: "test:TEST", Venue: venueName, MarketType: "SPOT", Symbol: "TEST-USD", QuoteAsset: "USD", Active: true}
	v := &fakeVenue{inst: inst}
	mem := newMemStore()
	clock := &testClock{base.Add(40 * time.Minute)}
	s := New(Options{Instruments: map[string]*venue.Instrument{inst.ID: inst}, Order: []string{inst.ID}, ConfigVersion: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CodeVersion: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, mem, mem, fakeVenues{v}, nil, clock)
	s.ctx = context.Background()
	return &restRig{t: t, clock: clock, mem: mem, v: v, s: s, inst: inst, base: base}
}

func (r *restRig) prints(first int64, from, to time.Time, every time.Duration) {
	id := first
	for at := from; at.Before(to); at = at.Add(every) {
		r.v.trades = append(r.v.trades, fakeTrade{id: id, ts: at, buy: id%3 != 0})
		id++
	}
}

func (r *restRig) repair(lo, hi time.Time) {
	r.s.runRepair(context.Background(), r.inst, &repairJob{start: lo, end: hi, reason: "TEST", spans: []span{{lo, hi}}})
	if err := r.mem.Flush(context.Background()); err != nil {
		r.t.Fatal(err)
	}
}

func (r *restRig) bar(minute time.Time) map[string]any {
	return r.mem.bars(r.inst.ID)[pytime.ISO(minute)]
}

func (r *restRig) gaps(kind string) []map[string]any {
	var out []map[string]any
	for _, row := range r.mem.rows["mkt.capture_gaps"] {
		if row["kind"] == kind {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return text(out[i]["start"]) < text(out[j]["start"]) })
	return out
}

func TestCoinbaseBackwardPagingRebuildsProvenMinutes(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.prints(1000, r.base.Add(-5*time.Minute), r.base.Add(32*time.Minute), time.Second)
	r.repair(r.base, r.base.Add(30*time.Minute))
	for m := 0; m < 30; m++ {
		row := r.bar(minuteOf(r.base, m))
		if row == nil || row["complete"] != true || row["bar_version"] != int64(1) || row["n_buy"].(int64)+row["n_sell"].(int64) != 60 {
			t.Fatalf("minute %d rebuilt complete from 60 prints: %v", m, row)
		}
	}
	if gaps := r.gaps("RECONNECT"); len(gaps) != 0 {
		t.Fatalf("an exhausted range records no gap: %v", gaps)
	}

	writes := r.mem.writes["mkt.flow_bars_1m"]
	r.repair(r.base, r.base.Add(30*time.Minute))
	if r.mem.writes["mkt.flow_bars_1m"] != writes {
		t.Fatalf("a rebuild that reproduces stored bars rewrote them: %d -> %d", writes, r.mem.writes["mkt.flow_bars_1m"])
	}
}

func TestOKXPagingRunningOutIsNotSuccess(t *testing.T) {
	r := newRESTRig(t, "okx")
	r.prints(1, r.base.Add(-5*time.Minute), r.base.Add(32*time.Minute), 2*time.Second)
	r.v.retention = r.base.Add(12 * time.Minute)
	r.repair(r.base, r.base.Add(30*time.Minute))
	if bars := r.mem.bars(r.inst.ID); len(bars) != 0 {
		t.Fatalf("an unexhausted range proves nothing and writes no bar: %d bars", len(bars))
	}
	if gaps := r.gaps("RECONNECT"); len(gaps) != 1 || gaps[0]["reason"] != "CHECKPOINT_RANGE_NOT_EXHAUSTED" {
		t.Fatalf("the unexhausted range is an explicit gap: %v", gaps)
	}
}

func TestBinanceIDHoleInRESTKeepsBothMinutesIncomplete(t *testing.T) {
	r := newRESTRig(t, "binance")
	r.prints(1, r.base.Add(-2*time.Minute), r.base.Add(12*time.Minute), time.Second)

	hole := r.base.Add(4*time.Minute + 59*time.Second)
	kept := r.v.trades[:0]
	for _, tr := range r.v.trades {
		if !tr.ts.Equal(hole) {
			kept = append(kept, tr)
		}
	}
	r.v.trades = kept
	r.repair(r.base, r.base.Add(10*time.Minute))
	for m := 0; m < 10; m++ {
		row := r.bar(minuteOf(r.base, m))
		inHole := m == 4 || m == 5
		if row == nil || (row["complete"] == true) == inHole {
			t.Fatalf("minute %d complete=%v (hole minutes 4 and 5 incomplete): %v", m, row["complete"], row)
		}
		if inHole && fmt.Sprint(row["gap_reason"]) != "REST_TRADE_ID_GAP" {
			t.Fatalf("hole minute names its gap: %v", row["gap_reason"])
		}
	}
	if gaps := r.gaps("TRADE_INTEGRITY"); len(gaps) != 1 {
		t.Fatalf("the hole is recorded: %v", gaps)
	}
}

func TestStoredBarRevisionRules(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.prints(1000, r.base.Add(-5*time.Minute), r.base.Add(12*time.Minute), time.Second)
	stored := func(minute time.Time, complete bool, version any, n int64) {
		r.mem.Sink(pointsRow(r.inst.ID, minute, map[string]any{
			"instrument_id": r.inst.ID, "minute_start": pytime.ISO(minute), "complete": complete,
			"n_buy": n, "n_sell": int64(0), "buy_qty": "2", "sell_qty": "0", "sum_sq_qty": "4",
			"bar_version": version, "gap_reason": map[bool]any{true: nil, false: "STREAM_DISCONNECTED: x"}[complete],
		}))
	}
	stored(minuteOf(r.base, 2), true, int64(1), 1)
	stored(minuteOf(r.base, 3), false, int64(1), 59)
	stored(minuteOf(r.base, 4), false, int64(0), 10)
	stored(minuteOf(r.base, 5), true, nil, 1)
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.repair(r.base, r.base.Add(10*time.Minute))
	for m, want := range map[int]int64{2: 2, 3: 2, 4: 1, 5: 2, 6: 1} {
		row := r.bar(minuteOf(r.base, m))
		if row["complete"] != true || row["bar_version"] != want || row["revised"] != (want > 1) {
			t.Fatalf("minute %d: complete, bar_version %d: %v", m, want, row)
		}
		if want > 1 {
			if rev, _ := row["capture_proof"].(map[string]any)["revision"].(map[string]any); rev == nil || rev["reason"] != "REST_REPAIR" {
				t.Fatalf("minute %d names its revision: %v", m, row["capture_proof"])
			}
		}
	}
}

func TestCompleteStoredBarNeverDowngraded(t *testing.T) {
	r := newRESTRig(t, "binance")
	r.prints(1, r.base.Add(-2*time.Minute), r.base.Add(12*time.Minute), time.Second)
	minute := minuteOf(r.base, 3)
	r.mem.Sink(pointsRow(r.inst.ID, minute, map[string]any{
		"instrument_id": r.inst.ID, "minute_start": pytime.ISO(minute), "complete": true,
		"n_buy": int64(40), "n_sell": int64(20), "bar_version": int64(1),
	}))
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	kept := r.v.trades[:0]
	for _, tr := range r.v.trades {
		if !tr.ts.Equal(minute.Add(30 * time.Second)) {
			kept = append(kept, tr)
		}
	}
	r.v.trades = kept
	r.repair(r.base, r.base.Add(10*time.Minute))
	if row := r.bar(minute); row["complete"] != true || row["n_buy"] != int64(40) {
		t.Fatalf("a complete stored bar was replaced by an incomplete rebuild: %v", row)
	}
}

func TestKrakenQuietWindowProvenByConsecutiveIDs(t *testing.T) {
	r := newRESTRig(t, "kraken")
	r.v.trades = []fakeTrade{
		{id: 10, ts: r.base.Add(-90 * time.Second), buy: true},
		{id: 11, ts: r.base.Add(12 * time.Minute), buy: false},
	}
	r.repair(r.base, r.base.Add(10*time.Minute))
	for m := 0; m < 10; m++ {
		row := r.bar(minuteOf(r.base, m))
		if row == nil || row["complete"] != true || row["zero_trade_minute"] != true {
			t.Fatalf("quiet Kraken minute %d proven empty: %v", m, row)
		}
	}

	r = newRESTRig(t, "kraken")
	r.v.trades = []fakeTrade{
		{id: 10, ts: r.base.Add(-90 * time.Second), buy: true},
		{id: 12, ts: r.base.Add(12 * time.Minute), buy: false},
	}
	r.repair(r.base, r.base.Add(10*time.Minute))
	if bars := r.mem.bars(r.inst.ID); len(bars) != 0 {
		t.Fatalf("a window behind an ID hole proves nothing: %d bars", len(bars))
	}
}

func TestSweepRepairsOKXSwapWithDiscoveredContractMultiplier(t *testing.T) {
	r := newRESTRig(t, "okx")
	swap := &venue.Instrument{ID: "okx:perp_linear:TEST-USD-SWAP", TokenID: r.inst.TokenID, Venue: "okx", MarketType: "PERP_LINEAR", Symbol: "TEST-USD-SWAP", QuoteAsset: "USD", Active: true}
	r.inst, r.v.inst, r.v.multiplier = swap, swap, "0.01"
	r.s = New(Options{Instruments: map[string]*venue.Instrument{swap.ID: swap}, Order: []string{swap.ID}}, r.mem, r.mem, fakeVenues{r.v}, nil, r.clock)
	r.s.ctx = context.Background()
	r.prints(1, r.base.Add(-2*time.Minute), r.base.Add(12*time.Minute), time.Second)
	r.s.repairUnrepaired(context.Background(), swap.TokenID, r.base, r.base.Add(10*time.Minute), true)
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gaps := r.gaps("PRE_EVENT_FLOW"); len(gaps) != 0 {
		t.Fatalf("the sweep normalizes swap prints with the discovered ctVal: %v", gaps)
	}
	for m := 0; m < 10; m++ {
		row := r.bar(minuteOf(r.base, m))
		if row == nil || row["complete"] != true || row["n_buy"].(int64)+row["n_sell"].(int64) != 60 {
			t.Fatalf("swap minute %d rebuilt complete from 60 prints: %v", m, row)
		}
		if qty := text(row["buy_qty"]); qty != "0.80" {
			t.Fatalf("swap minute %d sizes are contracts times ctVal: buy_qty %s", m, qty)
		}
	}
}
