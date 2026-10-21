package capture

import (
	"context"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

func TestHyperliquidLatePrintRevisesWithoutDowngrade(t *testing.T) {
	r := newRESTRig(t, "hyperliquid")
	minute := r.base.Add(5 * time.Minute)
	key := flow.KeyOf(r.inst.ID, minute)
	storageKey := r.inst.ID + ":" + key.MinuteISO()
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{storageKey: {
		"instrument_id": r.inst.ID, "minute_start": key.MinuteISO(), "complete": true, "gap_reason": nil,
		"n_buy": int64(3), "n_sell": int64(2), "buy_qty": "1.5", "sell_qty": "0.7", "bar_version": int64(1), "revised": false,
		"last_trade_id": "907", "last_trade": map[string]any{"trade_id": "907", "price": "100", "ts": pytime.ISO(minute.Add(30 * time.Second))},
		"capture_proof": map[string]any{"sealed_by": sourceStream},
	}}
	stored, _ := r.mem.Get(context.Background(), "mkt.flow_bars_1m", storageKey)
	print := func(at time.Duration, id string) *normalize.Trade {
		return &normalize.Trade{
			InstrumentID: r.inst.ID, Venue: "hyperliquid", TradeID: id, TS: minute.Add(at), Side: "BUY",
			Qty: pydec.MustParse("0.25"), Price: pydec.MustParse("101"),
		}
	}

	r.s.latePrintRevision(key, stored, print(20*time.Second, "905"), r.clock.Now())
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.mem.writes["mkt.flow_bars_1m"] != 0 {
		t.Fatalf("a replayed print rewrote the bar: %v", r.mem.rows["mkt.flow_bars_1m"][storageKey])
	}

	r.s.latePrintRevision(key, stored, print(45*time.Second, "911"), r.clock.Now())
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	row := r.mem.rows["mkt.flow_bars_1m"][storageKey]
	if row["complete"] != true || row["gap_reason"] != nil {
		t.Fatalf("a late print downgraded a complete bar: %v", row)
	}
	if row["bar_version"] != int64(2) || row["revised"] != true || row["n_buy"] != int64(3) {
		t.Fatalf("revision fields: %v", row)
	}
	rev := pyval.Map(pyval.Map(row["capture_proof"])["revision"])
	late := pyval.Map(rev["late_print"])
	if rev["reason"] != "LATE_PRINT_AFTER_SEAL" || late["trade_id"] != "911" || late["qty"] != "0.25" ||
		pyval.Map(row["capture_proof"])["sealed_by"] != sourceStream {
		t.Fatalf("revision proof: %v", row["capture_proof"])
	}
	if version, ok := r.s.seal.Version(key); !ok || version != 2 {
		t.Fatalf("the sealer keeps the new version for a later revision: %d %v", version, ok)
	}
}

func TestFlushWaitsForTheWriterLock(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	held := false
	r.s.opt.CanWrite = func() bool { return held }
	r.s.points.Put("mkt.flow_bars_1m", r.inst.ID+":"+pytime.ISO(r.base), map[string]any{
		"instrument_id": r.inst.ID, "minute_start": pytime.ISO(r.base), "complete": true,
	})
	r.s.flush(context.Background())
	if r.mem.writes["mkt.flow_bars_1m"] != 0 || r.s.LastFlush() != (time.Time{}) {
		t.Fatal("a flush wrote without the writer lock")
	}
	held = true
	r.s.flush(context.Background())
	if r.mem.writes["mkt.flow_bars_1m"] != 1 {
		t.Fatal("the queued row was not written once the lock was held again")
	}
}
