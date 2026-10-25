package capture

import (
	"context"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func TestUnrepairedSkipsOnlyTriedMinutes(t *testing.T) {
	st := newMemStore()
	iid := "binance:spot:BTCUSDT"
	lo := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	hi := lo.Add(60 * time.Minute)
	st.rows["mkt.flow_bars_1m"] = map[string]map[string]any{}
	for m := 0; m < 60; m++ {
		at := lo.Add(time.Duration(m) * time.Minute)
		row := map[string]any{"instrument_id": iid, "minute_start": pytime.ISO(at), "complete": true}
		switch {
		case m >= 20 && m < 25:
			continue
		case m == 40:
			row["complete"], row["gap_reason"] = false, BybitLimit
		case m == 41:
			row["complete"], row["gap_reason"] = false, "REST_REPAIR_INCOMPLETE"
		case m >= 57:
			continue
		}
		st.rows["mkt.flow_bars_1m"][iid+":"+pytime.ISO(at)] = row
	}
	s := &Service{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	got := s.unrepaired(context.Background(), iid, lo, hi)
	want := []span{{lo.Add(20 * time.Minute), lo.Add(25 * time.Minute)}, {lo.Add(41 * time.Minute), lo.Add(42 * time.Minute)}, {lo.Add(57 * time.Minute), hi}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if !got[i].start.Equal(want[i].start) || !got[i].end.Equal(want[i].end) {
			t.Fatalf("run %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestRepairRecordsReasonOnFullStoredBar(t *testing.T) {
	r := newRESTRig(t, "hyperliquid")
	minute := r.base.Add(5 * time.Minute)
	key := r.inst.ID + ":" + pytime.ISO(minute)
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{key: {
		"instrument_id": r.inst.ID, "minute_start": pytime.ISO(minute), "complete": false, "buy_qty": "1",
	}}
	if _, err := r.s.repairInstrument(context.Background(), r.inst, minute, minute.Add(time.Minute), true); err != nil {
		t.Fatal(err)
	}
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	bar := r.bar(minute)
	if bar["gap_reason"] == nil || bar["instrument_id"] != r.inst.ID || bar["buy_qty"] != "1" {
		t.Fatalf("bar = %v", bar)
	}
}

type countingRaw struct{ adds, flushes int }

func (c *countingRaw) Add(*normalize.Trade)  { c.adds++ }
func (c *countingRaw) Flush(time.Time) error { c.flushes++; return nil }

func TestStandbyWritesNothingUntilLeading(t *testing.T) {
	r := newRESTRig(t, "binance")
	leading := make(chan struct{})
	holding := false
	raw := &countingRaw{}
	r.s.opt.Leading, r.s.opt.CanWrite, r.s.opt.Raw = leading, func() bool { return holding }, raw
	r.mem.requests = []requeststate.Record{{StorageKey: "R1", RequestID: "R1", Kind: requestText("HTTP_FETCH"), Status: requestText("PENDING")}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r.s.leading() || r.s.waitLeading(ctx) {
		t.Fatal("leading before the lock")
	}
	r.s.flushRaw()
	if raw.flushes != 0 {
		t.Fatal("standby wrote trade files")
	}
	holding = true
	close(leading)
	if !r.s.leading() {
		t.Fatal("not leading after the lock")
	}
	r.s.flushRaw()
	if raw.flushes != 1 {
		t.Fatal("leader did not write trade files")
	}
}
