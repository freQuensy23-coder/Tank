package capture

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

// A restart repairs only the minutes it was down for: the incomplete run that ends at its
// start. An older hole of the window is not paged again.
func TestDowntimeGapIsOnlyTheTrailingRun(t *testing.T) {
	st := newMemStore()
	iid := "binance:spot:BTCUSDT"
	lo := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	hi := lo.Add(60 * time.Minute)
	st.rows["mkt.flow_bars_1m"] = map[string]map[string]any{}
	for m := 0; m < 60; m++ {
		if m >= 20 && m < 25 || m >= 57 { // an old hole and the restart's downtime
			continue
		}
		at := lo.Add(time.Duration(m) * time.Minute)
		st.rows["mkt.flow_bars_1m"][iid+":"+pytime.ISO(at)] = map[string]any{
			"instrument_id": iid, "minute_start": pytime.ISO(at), "complete": true,
		}
	}
	s := &Service{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	gap, ok := s.downtimeGap(context.Background(), iid, lo, hi)
	if !ok || !gap.start.Equal(lo.Add(57*time.Minute)) || !gap.end.Equal(hi) {
		t.Fatalf("gap = %v %v, %v", gap.start, gap.end, ok)
	}
	// Written up to the start: nothing to repair.
	if _, ok := s.downtimeGap(context.Background(), iid, lo, lo.Add(20*time.Minute)); ok {
		t.Fatal("complete up to start, yet a gap")
	}
}

// A minute a repair could not prove gets its gap reason written on the stored row, identity
// included (r128 wrote the repair projection back and every market flush failed).
func TestRepairRecordsReasonOnFullStoredBar(t *testing.T) {
	r := newRESTRig(t, "hyperliquid") // no REST trade history: nothing can be repaired
	minute := r.base.Add(5 * time.Minute)
	key := r.inst.ID + ":" + pytime.ISO(minute)
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{key: {
		"instrument_id": r.inst.ID, "minute_start": pytime.ISO(minute), "complete": false, "buy_qty": "1",
	}}
	r.s.repairInstrument(context.Background(), r.inst, minute, minute.Add(time.Minute), true)
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	bar := r.bar(minute)
	if bar["gap_reason"] == nil || bar["instrument_id"] != r.inst.ID || bar["buy_qty"] != "1" {
		t.Fatalf("bar = %v", bar)
	}
}
