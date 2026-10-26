package capture

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type refusedBackfillStart struct{ Store }

func (refusedBackfillStart) StartBackfill(context.Context, backfillstate.Start) (backfillstate.Acknowledgement, error) {
	return backfillstate.Acknowledgement{}, errors.New("observed start write failure")
}

type refusedBackfillCompletion struct{ Store }

func (refusedBackfillCompletion) FinishBackfill(context.Context, backfillstate.Completion) (backfillstate.Acknowledgement, error) {
	return backfillstate.Acknowledgement{}, errors.New("observed completion write failure")
}

func TestBackfillStartFailureCannotReachCaptureEffects(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	observation := queuedBackfill(t, r.mem, "BF-start-unacknowledged", map[string]any{
		"backfill_id": "BF-start-unacknowledged", "event_id": "EVT-start", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	})
	r.s.store = refusedBackfillStart{Store: r.mem}
	r.s.runObligation(context.Background(), observation)
	row := r.mem.rows[memoryBackfills]["BF-start-unacknowledged"]
	if row["status"] != "PENDING" || row["started_at"] != nil || row["finished_at"] != nil || row["result"] != nil || r.mem.writes[memoryBackfills] != 0 || r.v.calls != 0 {
		t.Fatalf("unacknowledged start acquired processing or capture effects: %v", row)
	}
}

func TestBackfillCompletionFailureRemainsRunningAndRetryable(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.opt.Order = nil
	r.mem.seedBackfill("BF-finish-unacknowledged", map[string]any{
		"backfill_id": "BF-finish-unacknowledged", "event_id": "EVT-finish", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	})
	r.s.store = refusedBackfillCompletion{Store: r.mem}
	r.s.obligations(context.Background())
	r.s.wg.Wait()
	row := r.mem.rows[memoryBackfills]["BF-finish-unacknowledged"]
	if row["status"] != "RUNNING" || row["started_at"] == nil || row["finished_at"] != nil || row["result"] != nil || r.mem.writes[memoryBackfills] != 1 {
		t.Fatalf("failed terminal persistence claimed a completed capture: %v", row)
	}
	r.s.store = r.mem
	r.s.obligations(context.Background())
	r.s.wg.Wait()
	row = r.mem.rows[memoryBackfills]["BF-finish-unacknowledged"]
	if row["status"] != "PARTIAL" || row["finished_at"] == nil || row["result"] == nil || r.mem.writes[memoryBackfills] != 3 {
		t.Fatalf("unacknowledged completion suppressed a later actual retry: %v", row)
	}
}

type observedBackfillStart struct {
	Store
	started chan struct{}
}

func (s observedBackfillStart) StartBackfill(ctx context.Context, start backfillstate.Start) (backfillstate.Acknowledgement, error) {
	acknowledgement, err := s.Store.StartBackfill(ctx, start)
	if err == nil && acknowledgement.Disposition() == backfillstate.WriteApplied {
		s.started <- struct{}{}
	}
	return acknowledgement, err
}

func TestBackfillCancellationAfterActualStartPreservesRunningEvidence(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	ctx, cancel := context.WithCancel(context.Background())
	r.s.ctx = ctx
	t.Cleanup(func() { cancel(); r.s.wg.Wait() })
	r.mem.seedBackfill("BF-cancelled", map[string]any{
		"backfill_id": "BF-cancelled", "event_id": "EVT-cancelled", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	})
	started := make(chan struct{}, 1)
	r.s.store = observedBackfillStart{Store: r.mem, started: started}
	for range cap(r.s.slots) {
		r.s.slots <- struct{}{}
	}
	r.s.obligations(ctx)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("actual running transition did not finish before cancellation")
	}
	cancel()
	r.s.wg.Wait()
	row := r.mem.rows[memoryBackfills]["BF-cancelled"]
	if row["status"] != "RUNNING" || row["started_at"] != pytime.ISO(r.clock.Now()) || row["finished_at"] != nil || row["result"] != nil || r.mem.writes[memoryBackfills] != 1 || r.v.calls != 0 {
		t.Fatalf("cancellation invented a terminal capture observation: %v", row)
	}
}
