package capture

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
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
		"anchor": pytime.ISO(anchor), "started_at": nil, "started_at_prec": nil,
		"started_at_prec_reason": "PENDING", "status": "PENDING",
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
	if row["started_at"] != pytime.ISO(r.clock.Now()) || row["started_at_prec"] != "COLLECTION_ONLY" {
		t.Fatalf("worker must stamp actual start rather than preserve missing queued clock: %v", row)
	}
	result := pyval.Map(row["result"])
	if len(pyval.List(result["instruments"])) != 1 {
		t.Fatalf("coverage per instrument: %v", result)
	}
	coverage := pyval.Map(pyval.List(result["instruments"])[0])
	saveProducerEvidence(t, "backfill-terminal", row)
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
	saveProducerEvidence(t, "repair-complete", done["CR-1"])
	saveProducerEvidence(t, "repair-expired", done["CR-2"])
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

func saveProducerEvidence(t *testing.T, name string, row map[string]any) {
	t.Helper()
	directory := os.Getenv("TANK_CAPTURE_PRODUCER_EVIDENCE_DIR")
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	body, err := pyjson.Dumps(row, false)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name+".json")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("refusing to overwrite producer evidence at %s", path)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
