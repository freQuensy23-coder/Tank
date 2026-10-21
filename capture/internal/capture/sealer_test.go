package capture

import (
	"strconv"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type testClock struct{ t time.Time }

func (c *testClock) Now() time.Time           { return c.t }
func (c *testClock) Monotonic() time.Duration { return time.Duration(c.t.UnixNano()) }
func (c *testClock) set(t time.Time)          { c.t = t }
func (c *testClock) advance(d time.Duration)  { c.t = c.t.Add(d) }
func at(base time.Time, s float64) time.Time {
	return base.Add(time.Duration(s * float64(time.Second)))
}
func minuteOf(base time.Time, n int) time.Time { return base.Add(time.Duration(n) * time.Minute) }

type written struct {
	key string
	row map[string]any
}

type sealerRig struct {
	t       *testing.T
	clock   *testClock
	s       *Sealer
	rows    []written
	repairs []span
	late    []*normalize.Trade
	inst    *venue.Instrument
}

func newRig(t *testing.T, venueName, marketType string) *sealerRig {
	base := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	r := &sealerRig{t: t, clock: &testClock{base}}
	r.inst = &venue.Instrument{ID: venueName + ":TEST", TokenID: "test:TEST", Venue: venueName, MarketType: marketType, Symbol: "TEST", QuoteAsset: "USD", Active: true}
	r.s = NewSealer(r.clock, func(collection, key string, row map[string]any) map[string]any {
		if collection == "mkt.flow_bars_1m" {
			r.rows = append(r.rows, written{key, row})
		}
		return row
	}, 45*time.Second)
	r.s.Repair = func(_ *venue.Instrument, start, end time.Time, _ string) {
		r.repairs = append(r.repairs, span{start, end})
	}
	r.s.Late = func(_ *venue.Instrument, t *normalize.Trade) { r.late = append(r.late, t) }
	return r
}

func (r *sealerRig) trade(id int, ts time.Time) *normalize.Trade {
	qty := pydec.MustParse("1.5")
	price := pydec.MustParse("100")
	usd := pydec.MustParse("150")
	return &normalize.Trade{
		InstrumentID: r.inst.ID, TokenID: r.inst.TokenID, Venue: r.inst.Venue, MarketType: r.inst.MarketType,
		TradeID: strconv.Itoa(id), TS: ts, TSPrec: "EXACT", Side: "BUY", Qty: qty, Price: price,
		QuoteNotional: usd, NotionalUSD: &usd, QuoteAsset: "USD", TradeAggregation: "INDIVIDUAL",
		Sequence: int64(id),
	}
}

// last is the newest written row of a minute.
func (r *sealerRig) last(minute time.Time) map[string]any {
	key := r.inst.ID + ":" + flow.KeyOf(r.inst.ID, minute).MinuteISO()
	for i := len(r.rows) - 1; i >= 0; i-- {
		if r.rows[i].key == key {
			return r.rows[i].row
		}
	}
	return nil
}

func (r *sealerRig) tick(now time.Time) []HeadCheck {
	r.clock.set(now)
	return r.s.Tick(now)
}

func TestSealOnNextTradeAndConsecutiveIDs(t *testing.T) {
	r := newRig(t, "coinbase", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(100, at(base, 5)), 1)
	r.s.Trade(r.inst, r.trade(101, at(base, 30)), 1)
	r.tick(at(base, 59))
	if row := r.last(base); row == nil || row["complete"] != false || row["bar_version"] != int64(0) {
		t.Fatalf("open minute is written provisionally: %v", row)
	}
	// No print at or after the minute end yet: never sealed by a timer.
	r.tick(at(base, 70))
	if row := r.last(base); row["bar_version"] != int64(0) {
		t.Fatalf("minute sealed without a later print: %v", row)
	}
	r.s.Trade(r.inst, r.trade(102, at(base, 61)), 1)
	r.tick(at(base, 62))
	row := r.last(base)
	if row["complete"] != true || row["bar_version"] != int64(1) || row["n_buy"] != int64(2) {
		t.Fatalf("minute sealed complete by the next print: %v", row)
	}
	if proof := row["capture_proof"].(map[string]any); proof["sealed_by"] != sourceStream {
		t.Fatalf("seal method: %v", proof)
	}
}

func TestTradeIDHoleWaitsForRepairBothMinutesIncluded(t *testing.T) {
	r := newRig(t, "binance", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(100, at(base, 50)), 1)
	r.s.Trade(r.inst, r.trade(104, at(base, 70)), 1) // 101..103 missing
	if len(r.repairs) != 1 || !r.repairs[0].start.Equal(base) || !r.repairs[0].end.After(at(base, 70)) {
		t.Fatalf("hole repair [floor(before), after]: %v", r.repairs)
	}
	r.s.Trade(r.inst, r.trade(105, at(base, 130)), 1)
	r.tick(at(base, 131))
	for _, m := range []time.Time{base, minuteOf(base, 1)} {
		if row := r.last(m); row == nil || row["bar_version"] != int64(0) {
			t.Fatalf("minute %v of the hole must wait for its repair: %v", m, row)
		}
	}
	r.s.RepairDone(r.inst.ID, r.repairs[0].start, r.repairs[0].end)
	r.tick(at(base, 300))
	for _, m := range []time.Time{base, minuteOf(base, 1)} {
		row := r.last(m)
		if row["complete"] != false || row["bar_version"] != int64(1) || row["gap_reason"] == nil {
			t.Fatalf("unrepaired hole minute sealed incomplete with its reason: %v", row)
		}
	}
}

func TestQuietMinutesSealedByHeadCheckOnly(t *testing.T) {
	r := newRig(t, "kraken", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	last := r.trade(500, at(base, 10))
	r.s.Trade(r.inst, last, 1)
	if checks := r.tick(at(base, 100)); len(checks) != 0 {
		t.Fatalf("head check before minute end + delay: %v", checks)
	}
	checks := r.tick(at(base, 106))
	if len(checks) != 1 {
		t.Fatalf("head check due 45 s after the quiet minute: %v", checks)
	}
	if checks2 := r.tick(at(base, 107)); len(checks2) != 0 {
		t.Fatal("one head check in flight at a time")
	}
	observed := minuteOf(base, 3)
	r.clock.set(observed)
	r.s.HeadResult(checks[0], []*normalize.Trade{r.trade(499, at(base, 2)), r.trade(500, at(base, 10))}, observed, false)
	r.tick(observed)
	row := r.last(base)
	if row["complete"] != true || row["bar_version"] != int64(1) {
		t.Fatalf("minute with a print sealed by the head check: %v", row)
	}
	if row["capture_proof"].(map[string]any)["sealed_by"] != sourceHead {
		t.Fatalf("seal method: %v", row["capture_proof"])
	}
	// Minute 1 is quiet and ends before observed - delay (2:15): an empty complete bar.
	quiet := r.last(minuteOf(base, 1))
	if quiet == nil || quiet["complete"] != true || quiet["zero_trade_minute"] != true {
		t.Fatalf("quiet minute sealed as a complete empty bar: %v", quiet)
	}
	// Minute 2 ends after observed - delay: still open.
	if open := r.last(minuteOf(base, 2)); open != nil && open["bar_version"] != int64(0) {
		t.Fatalf("minute past the proven head stays open: %v", open)
	}
}

func TestHeadCheckContinuesChainOrReportsHole(t *testing.T) {
	r := newRig(t, "coinbase", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(10, at(base, 10)), 1)
	checks := r.tick(at(base, 110))
	observed := at(base, 110)
	// REST has 11 and 12 the stream has not delivered: they continue the chain.
	r.s.HeadResult(checks[0], []*normalize.Trade{r.trade(10, at(base, 10)), r.trade(11, at(base, 40)), r.trade(12, at(base, 62))}, observed, false)
	r.tick(observed)
	if row := r.last(base); row["complete"] != true || row["n_buy"] != int64(2) {
		t.Fatalf("contiguous REST prints continue the chain: %v", row)
	}
	// A later check finds 15 after 12: a hole repaired over REST.
	r.clock.set(at(base, 400))
	r.s.mu.Lock()
	r.s.chains[r.inst.ID].headNext = time.Time{}
	r.s.mu.Unlock()
	checks = r.tick(at(base, 400))
	if len(checks) != 1 {
		t.Fatalf("second head check: %v", checks)
	}
	r.s.HeadResult(checks[0], []*normalize.Trade{r.trade(15, at(base, 200))}, at(base, 400), false)
	if len(r.repairs) != 1 {
		t.Fatalf("hole after the chain repaired over REST: %v", r.repairs)
	}
}

func TestRevisionOnLatePrintNeverSilent(t *testing.T) {
	r := newRig(t, "okx", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(1, at(base, 10)), 1)
	r.s.Trade(r.inst, r.trade(2, at(base, 61)), 1)
	r.tick(at(base, 62))
	if row := r.last(base); row["bar_version"] != int64(1) {
		t.Fatalf("sealed: %v", row)
	}
	r.s.Trade(r.inst, r.trade(3, at(base, 30)), 1) // a print for the sealed minute
	row := r.last(base)
	if row["bar_version"] != int64(2) || row["revised"] != true || row["n_buy"] != int64(2) {
		t.Fatalf("late print is a revision: %v", row)
	}
	rev := row["capture_proof"].(map[string]any)["revision"].(map[string]any)
	if rev["reason"] != "LATE_PRINT" || rev["version"] != int64(2) {
		t.Fatalf("revision reason: %v", rev)
	}
	// The same print again changes nothing.
	n := len(r.rows)
	r.s.Trade(r.inst, r.trade(3, at(base, 30)), 1)
	if len(r.rows) != n {
		t.Fatal("a repeated print rewrote the bar")
	}
}

func TestRebuildNeverReplacesCompleteBarWithIncomplete(t *testing.T) {
	r := newRig(t, "coinbase", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(1, at(base, 10)), 1)
	r.s.Trade(r.inst, r.trade(2, at(base, 61)), 1)
	r.tick(at(base, 62))
	n := len(r.rows)
	key := flow.KeyOf(r.inst.ID, base)
	acc := flow.New()
	acc.EmptyMinute(r.inst, base)
	acc.Add(r.trade(1, at(base, 10)), false)
	bar := acc.Bars[key]
	bar.Coverage = nil
	if !r.s.Rebuilt(key, bar, nil, at(base, 70)) {
		t.Fatal("a sealed minute in memory is held")
	}
	if len(r.rows) != n {
		t.Fatalf("an incomplete rebuild rewrote a complete bar: %v", r.rows[len(r.rows)-1].row)
	}
}

func TestReconnectBridgeKeepsMinutesOpenUntilRepaired(t *testing.T) {
	r := newRig(t, "okx", "PERP_LINEAR")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(1, at(base, 10)), 1)
	r.clock.set(at(base, 40))
	r.s.Disconnected(r.inst, 1, "STREAM_DISCONNECTED: reset")
	bridge := r.s.Connected(r.inst, 2, at(base, 150), nil)
	if bridge == nil || !bridge.start.Equal(base) || !bridge.end.Equal(at(base, 150)) {
		t.Fatalf("bridge from the last print's minute to the new socket: %v", bridge)
	}
	r.s.Trade(r.inst, r.trade(9, at(base, 185)), 2)
	r.tick(at(base, 186))
	if row := r.last(base); row == nil || row["bar_version"] != int64(0) {
		t.Fatalf("minute of the disconnect waits for the bridge: %v", row)
	}
	r.s.RepairDone(r.inst.ID, bridge.start, bridge.end)
	r.tick(at(base, 190))
	if row := r.last(base); row["bar_version"] != int64(1) || row["complete"] != false {
		t.Fatalf("unbridged minute sealed incomplete: %v", row)
	}
	// Minute 3 starts after the new socket started: proven by its own chain.
	r.s.Trade(r.inst, r.trade(10, at(base, 245)), 2)
	r.tick(at(base, 246))
	if row := r.last(minuteOf(base, 3)); row == nil || row["complete"] != true {
		t.Fatalf("minute inside the new chain sealed complete: %v", row)
	}
}

func TestPrintBeforeFirstConnectionTakesStoredPath(t *testing.T) {
	r := newRig(t, "coinbase", "SPOT")
	base := r.clock.t
	r.s.Connected(r.inst, 1, at(base, 130), nil)
	r.s.Trade(r.inst, r.trade(1, at(base, 30)), 1)
	if len(r.late) != 1 || len(r.rows) != 0 {
		t.Fatalf("a print of a minute before this process's first connection goes to the stored path: %v %v", r.late, r.rows)
	}
}

func TestHyperliquidQuietMinutesByWatermark(t *testing.T) {
	r := newRig(t, "hyperliquid", "PERP_LINEAR")
	base := r.clock.t
	r.s.Connected(r.inst, 1, base, nil)
	r.s.Trade(r.inst, r.trade(1, at(base, 10)), 1)
	if checks := r.tick(at(base, 200)); len(checks) != 0 {
		t.Fatal("no REST head checks without REST trade history")
	}
	r.s.Watermark(r.inst.ID, 1, at(base, 106))
	r.tick(at(base, 107))
	if row := r.last(base); row["complete"] != true || row["capture_proof"].(map[string]any)["sealed_by"] != sourceWatermark {
		t.Fatalf("quiet minute sealed by the same connection's watermark: %v", row)
	}
}
