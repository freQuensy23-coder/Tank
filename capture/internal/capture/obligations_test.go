package capture

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

func TestObligationRunsCoreRowToTerminalResult(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	anchor := r.base.Add(30 * time.Minute)
	r.prints(1, anchor.Add(-25*time.Hour), anchor.Add(5*time.Minute), 20*time.Second)
	r.clock.set(anchor.Add(10 * time.Minute))
	r.mem.runs = []map[string]any{{
		"backfill_id": "BF-test", "event_id": "EVT-1", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(anchor), "started_at": pytime.ISO(anchor), "status": "PENDING",
		"reason": "PRE_EVENT_CAPTURE_PENDING",
	}}
	r.s.obligations(context.Background())
	r.s.wg.Wait()
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	row := r.mem.rows["mkt.backfill_runs"]["BF-test"]
	if row == nil || (row["status"] != "COMPLETE" && row["status"] != "PARTIAL") || row["finished_at"] == nil {
		t.Fatalf("the obligation ends COMPLETE or PARTIAL with its finish time: %v", row)
	}
	result := pyval.Map(row["result"])
	if len(pyval.List(result["instruments"])) != 1 {
		t.Fatalf("coverage per instrument: %v", result)
	}
	coverage := pyval.Map(pyval.List(result["instruments"])[0])
	flow := pyval.Map(pyval.Map(pyval.Map(coverage["windows"])["W1H"])["flow"])
	if flow["complete"] != true {
		t.Fatalf("the last hour before the anchor is complete from REST: %v", flow)
	}

	r.s.obligations(context.Background())
	r.s.wg.Wait()
}

func TestCaptureRequestRepairsWithinBudget(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.prints(1, r.base.Add(-2*time.Hour), r.base.Add(40*time.Minute), 30*time.Second)
	now := r.base.Add(40 * time.Minute)
	r.clock.set(now)
	r.mem.requests = []map[string]any{{
		"request_id": "CR-1", "kind": "REPAIR_WINDOWS", "token_id": r.inst.TokenID,
		"start": pytime.ISO(r.base), "end": pytime.ISO(r.base.Add(30 * time.Minute)),
		"deadline": pytime.ISO(now.Add(30 * time.Second)), "status": "PENDING",
	}, {
		"request_id": "CR-2", "kind": "REPAIR_WINDOWS", "token_id": r.inst.TokenID,
		"start": pytime.ISO(r.base), "end": pytime.ISO(r.base.Add(30 * time.Minute)),
		"deadline": pytime.ISO(now.Add(-time.Second)), "status": "PENDING",
	}}
	r.s.requests(context.Background())
	r.s.wg.Wait()
	done := map[string]map[string]any{}
	for _, row := range r.mem.requests {
		done[text(row["request_id"])] = row
	}
	if done["CR-1"]["status"] != "COMPLETE" {
		t.Fatalf("request finished: %v", done["CR-1"])
	}
	result := pyval.Map(pyval.Map(done["CR-1"]["result"])["instruments"])[r.inst.ID]
	counts := pyval.Map(result)
	if counts["incomplete_before"] != int64(30) || counts["incomplete_after"] != int64(0) {
		t.Fatalf("30 missing minutes repaired: %v", counts)
	}
	if done["CR-2"]["status"] != "EXPIRED" {
		t.Fatalf("a request past its deadline is expired, not run: %v", done["CR-2"])
	}
}

func TestKlineAuditsSpreadAcrossTheHour(t *testing.T) {
	order := make([]string, 51)
	for i := range order {
		order[i] = "venue:spot:I" + strconv.Itoa(i)
	}
	s := &Service{opt: Options{Order: order}}
	seen := map[time.Duration]bool{}
	var last time.Duration
	for _, iid := range order {
		d := s.auditOffset(iid)
		if d < time.Minute || d >= time.Minute+time.Hour || seen[d] {
			t.Fatalf("every instrument audits at its own point inside the first hour: %s at %v", iid, d)
		}
		if d-last > 0 && last > 0 && d-last < time.Minute {
			t.Fatalf("neighbouring audits start at least a minute apart: %v after %v", d, last)
		}
		seen[d], last = true, d
	}
}

func TestKlineAuditWaitsForNoRepairSlot(t *testing.T) {
	s := &Service{slots: make(chan struct{}, 1)}
	s.slots <- struct{}{}
	done := make(chan bool, 1)
	go func() { done <- s.auditKlines(context.Background(), &venue.Instrument{ID: "bybit:spot:BTCUSDT"}) }()
	select {
	case ran := <-done:
		if ran {
			t.Fatal("no audit runs without a free repair slot")
		}
	case <-time.After(time.Second):
		t.Fatal("the polling loop is not held while repairs hold every slot")
	}
}

func storedKlines(r *restRig, from, to time.Time) {
	table := map[string]map[string]any{}
	for m := pytime.MinuteFloor(from); m.Before(to); m = m.Add(time.Minute) {
		table[r.inst.ID+":"+pytime.ISO(m)] = map[string]any{"instrument_id": r.inst.ID, "open_time": pytime.ISO(m), "complete": true}
	}
	r.mem.rows["mkt.klines_1m"] = table
}

func runObligation(t *testing.T, r *restRig, id string, anchor time.Time) map[string]any {
	t.Helper()
	r.mem.runs = []map[string]any{{
		"backfill_id": id, "event_id": "EVT-" + id, "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(anchor), "started_at": pytime.ISO(anchor), "status": "PENDING",
		"reason": "PRE_EVENT_CAPTURE_PENDING",
	}}
	r.s.obligations(context.Background())
	r.s.wg.Wait()
	if err := r.mem.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r.mem.rows["mkt.backfill_runs"][id]
}

func TestSubMinuteAnchorBackfillWithCompleteMinutesIsComplete(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	anchor := r.base.Add(30*time.Minute + 3884*time.Millisecond)
	r.prints(1, anchor.Add(-25*time.Hour), anchor.Add(5*time.Minute), 20*time.Second)
	storedKlines(r, anchor.Add(-25*time.Hour), anchor.Add(time.Hour))
	r.clock.set(anchor.Add(10 * time.Minute))
	row := runObligation(t, r, "BF-subminute", anchor)
	if row["status"] != "COMPLETE" || row["reason"] != nil {
		t.Fatalf("complete minutes around a sub-minute anchor make a COMPLETE backfill: %v %v", row["status"], row["reason"])
	}
	coverage := pyval.Map(pyval.List(pyval.Map(row["result"])["instruments"])[0])
	w1h := pyval.Map(pyval.Map(coverage["windows"])["W1H"])
	if w1h["unresolved_boundary_seconds"] != float64(60) || w1h["timestamp_precision"] != "BUCKET" {
		t.Fatalf("the sub-minute boundary stays reported as bucket precision: %v", w1h)
	}
	if pre := pyval.Map(coverage["pre24_reference"]); pre["available"] != true {
		t.Fatalf("the pre-24h reference is the last REST trade before the boundary: %v", pre)
	}
}

func TestBackfillPersistsPre24ReferenceIntoLiveSealedBar(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	anchor := r.base.Add(30 * time.Minute)
	boundary := anchor.Add(-24 * time.Hour)
	r.prints(1, anchor.Add(-25*time.Hour), anchor.Add(5*time.Minute), 20*time.Second)
	storedKlines(r, anchor.Add(-25*time.Hour), anchor.Add(time.Hour))
	r.clock.set(anchor.Add(10 * time.Minute))
	if row := runObligation(t, r, "BF-first", anchor); row["status"] != "COMPLETE" {
		t.Fatalf("a fully backfilled exact-minute anchor is COMPLETE: %v %v", row["status"], row["reason"])
	}
	before := map[string]any{}
	for _, minute := range []time.Time{boundary.Add(-time.Minute), boundary} {
		key := r.inst.ID + ":" + pytime.ISO(minute)
		bar := r.mem.rows["mkt.flow_bars_1m"][key]
		if bar == nil {
			continue
		}
		before[key] = bar["bar_version"]
		bar["reference_points"], bar["boundary_segments"] = []any{}, []any{}
	}
	r.s.obliged = map[string]bool{}
	row := runObligation(t, r, "BF-second", anchor)
	if row["status"] != "COMPLETE" {
		t.Fatalf("a bar sealed without the boundary still gets the backfilled reference: %v %v", row["status"], row["reason"])
	}
	coverage := pyval.Map(pyval.List(pyval.Map(row["result"])["instruments"])[0])
	if pre := pyval.Map(coverage["pre24_reference"]); pre["available"] != true {
		t.Fatalf("pre-24h reference persisted: %v", pre)
	}
	revised := 0
	for key, version := range before {
		bar := r.mem.rows["mkt.flow_bars_1m"][key]
		if bar["bar_version"] == version {
			continue
		}
		revised++
		proof := pyval.Map(bar["capture_proof"])
		if pyval.Map(proof["revision"])["reason"] != "BOUNDARY_EVIDENCE" {
			t.Fatalf("the revision names its reason: %v", proof)
		}
	}
	if revised == 0 {
		t.Fatalf("no boundary bar was revised with the reference point")
	}

	writes := r.mem.writes["mkt.flow_bars_1m"]
	r.s.obliged = map[string]bool{}
	runObligation(t, r, "BF-third", anchor)
	for key := range before {
		bar := r.mem.rows["mkt.flow_bars_1m"][key]
		if len(pyval.List(bar["reference_points"])) == 0 {
			t.Fatalf("stored evidence survives: %v", bar)
		}
	}
	if r.mem.writes["mkt.flow_bars_1m"] != writes {
		t.Fatalf("a rebuild that adds no evidence rewrites nothing: %d -> %d", writes, r.mem.writes["mkt.flow_bars_1m"])
	}
}
