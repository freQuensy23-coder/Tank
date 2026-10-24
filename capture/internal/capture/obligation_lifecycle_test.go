package capture

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func TestFailedObligationCanRetrySameIdentityWithoutConcurrentDuplicate(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	ctx, cancel := context.WithCancel(context.Background())
	r.s.ctx = ctx
	t.Cleanup(func() { cancel(); r.s.wg.Wait() })
	anchor := r.base.Add(30 * time.Minute)
	r.prints(1, anchor.Add(-25*time.Hour), anchor.Add(5*time.Minute), 20*time.Second)
	r.clock.set(anchor.Add(10 * time.Minute))
	original := map[string]any{
		"backfill_id": "BF-retry", "event_id": "EVT-retry", "token_id": r.inst.TokenID,
		"anchor": nil, "anchor_prec": nil, "anchor_prec_reason": "INCOMPLETE",
		"started_at": nil, "started_at_prec": nil, "started_at_prec_reason": "PENDING",
		"finished_at": nil, "finished_at_prec": nil, "finished_at_prec_reason": "PENDING",
		"status": "PENDING", "reason": "PRE_EVENT_CAPTURE_PENDING",
	}
	r.mem.runs = []map[string]any{copyRow(original)}
	r.s.obligations(ctx)
	r.s.wg.Wait()
	r.mem.mu.Lock()
	failed := copyRow(r.mem.rows["mkt.backfill_runs"]["BF-retry"])
	r.mem.mu.Unlock()
	if failed["status"] != "FAILED" {
		t.Fatalf("first invalid input must produce explicit failed evidence: %v", failed)
	}
	pending := copyRow(original)
	pending["anchor"], pending["anchor_prec"], pending["anchor_prec_reason"] = pytime.ISO(anchor), "EXACT", nil
	r.mem.mu.Lock()
	r.mem.runs[0] = copyRow(pending)
	r.mem.rows["mkt.backfill_runs"]["BF-retry"] = copyRow(pending)
	before := r.mem.writes["mkt.backfill_runs"]
	r.mem.mu.Unlock()
	for range cap(r.s.slots) {
		r.s.slots <- struct{}{}
	}
	var callers sync.WaitGroup
	for range 8 {
		callers.Add(1)
		go func() { defer callers.Done(); r.s.obligations(ctx) }()
	}
	callers.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mem.mu.Lock()
		row := copyRow(r.mem.rows["mkt.backfill_runs"]["BF-retry"])
		writes := r.mem.writes["mkt.backfill_runs"]
		r.mem.mu.Unlock()
		if row["status"] == "RUNNING" {
			if writes != before+1 {
				t.Fatalf("concurrent workers duplicated the running transition: before=%d now=%d", before, writes)
			}
			if row["started_at"] != pytime.ISO(r.clock.Now()) || row["started_at_prec"] != "COLLECTION_ONLY" {
				t.Fatalf("retry did not record actual worker start: %v", row)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("same identity remained suppressed after failure: %v", row)
		}
		time.Sleep(time.Millisecond)
	}
	for range cap(r.s.slots) {
		<-r.s.slots
	}
	done := make(chan struct{})
	go func() { r.s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retried obligation did not finish")
	}
	r.mem.mu.Lock()
	terminal := copyRow(r.mem.rows["mkt.backfill_runs"]["BF-retry"])
	after := r.mem.writes["mkt.backfill_runs"]
	r.mem.mu.Unlock()
	if terminal["status"] != "COMPLETE" && terminal["status"] != "PARTIAL" {
		t.Fatalf("same identity did not rerun capture after corrected input: %v", terminal)
	}
	if terminal["result"] == nil || after != before+2 {
		t.Fatalf("retry must produce exactly one actual running-to-terminal capture: %v writes=%d", terminal, after-before)
	}
	r.s.obligations(ctx)
	r.s.wg.Wait()
	r.mem.mu.Lock()
	final := r.mem.writes["mkt.backfill_runs"]
	r.mem.mu.Unlock()
	if final != after {
		t.Fatalf("terminal durable row was captured again: %d to %d writes", after, final)
	}
}
