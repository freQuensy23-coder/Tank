package capture

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

type unavailableRepairStore struct {
	Store
	failAt int32
	reads  atomic.Int32
}

func (s *unavailableRepairStore) Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, ids []string) ([]map[string]any, error) {
	if collection == "mkt.flow_bars_1m" && s.reads.Add(1) >= s.failAt {
		return nil, errors.New("coverage backend unavailable")
	}
	return s.Store.Projection(ctx, collection, fields, field, start, end, ids)
}

func TestRepairRequestReadFailureCannotInventCompleteCoverage(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.store = &unavailableRepairStore{Store: r.mem, failAt: 1}
	row := map[string]any{
		"request_id": "CR-read-failed", "kind": "REPAIR_WINDOWS", "token_id": r.inst.TokenID,
		"start": pytime.ISO(r.base), "end": pytime.ISO(r.base.Add(30 * time.Minute)),
		"deadline": pytime.ISO(r.clock.Now().Add(time.Minute)), "status": "RUNNING",
	}
	r.mem.requests = []map[string]any{row}
	r.s.runRequest(context.Background(), row)
	stored := r.mem.requests[0]
	if stored["status"] != "FAILED" || stored["reason"] != "REPAIR_FAILED" || stored["finished_at"] == nil {
		t.Fatalf("coverage backend failure became a successful repair: %v", stored)
	}
	result := pyval.Map(stored["result"])
	instruments, ok := result["instruments"].(map[string]any)
	if !ok || len(instruments) != 0 {
		t.Fatalf("unavailable instrument observations became fabricated counts: %v", stored)
	}
}

func TestRepairFinalReadFailureDoesNotReuseInitialCounts(t *testing.T) {
	r := newRESTRig(t, "hyperliquid")
	r.s.store = &unavailableRepairStore{Store: r.mem, failAt: 2}
	coverage, err := r.s.repairInstrument(context.Background(), r.inst, r.base, r.base.Add(time.Minute), true)
	if err == nil || !strings.Contains(err.Error(), "final repair coverage unavailable") {
		t.Fatalf("final observation failure was hidden behind stale initial counts: coverage=%v error=%v", coverage, err)
	}
}

func TestBackfillReadFailureCannotPersistCompleteOrPartialCoverage(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.store = &unavailableRepairStore{Store: r.mem, failAt: 1}
	r.s.runObligation(context.Background(), map[string]any{
		"backfill_id": "BF-read-failed", "event_id": "EVT-read-failed", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	})
	stored := r.mem.rows["mkt.backfill_runs"]["BF-read-failed"]
	if stored["status"] != "FAILED" || stored["reason"] != "REPAIR_FAILED" || stored["finished_at"] == nil || stored["result"] != nil {
		t.Fatalf("backfill invented availability after a failed repair read: %v", stored)
	}
}

type unavailableCoverageStore struct {
	Store
	failAt    int32
	reads     atomic.Int32
	benchmark bool
}

func (s *unavailableCoverageStore) Range(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error) {
	if s.reads.Add(1) >= s.failAt {
		return nil, errors.New("range backend unavailable")
	}
	return s.Store.Range(ctx, collection, field, start, end, filters)
}

func (s *unavailableCoverageStore) Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, ids []string) ([]map[string]any, error) {
	if s.benchmark && collection == "mkt.klines_1m" {
		return nil, errors.New("benchmark backend unavailable")
	}
	return s.Store.Projection(ctx, collection, fields, field, start, end, ids)
}

func TestCaptureCoverageReadFailuresRemainUnavailable(t *testing.T) {
	for _, test := range []struct {
		name    string
		read    int32
		message string
	}{
		{"flow", 1, "flow coverage unavailable"},
		{"klines", 2, "klines coverage unavailable"},
		{"pre24", 7, "pre24 reference unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := newRESTRig(t, "coinbase")
			r.s.store = &unavailableCoverageStore{Store: r.mem, failAt: test.read}
			coverage, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
			if err == nil || !strings.Contains(err.Error(), test.message) || coverage != nil {
				t.Fatalf("failed observation became invented counters: coverage=%v error=%v", coverage, err)
			}
		})
	}
}

func TestBackfillCoverageFailureCannotInventPartialResult(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.store = &unavailableCoverageStore{Store: r.mem, failAt: 1}
	r.s.runObligation(context.Background(), map[string]any{
		"backfill_id": "BF-range-failed", "event_id": "EVT-range-failed", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	})
	stored := r.mem.rows["mkt.backfill_runs"]["BF-range-failed"]
	if stored["status"] != "FAILED" || stored["reason"] != "COVERAGE_READ_FAILED" || stored["result"] != nil {
		t.Fatalf("unavailable coverage became invented terminal counters: %v", stored)
	}
}

func TestBackfillBenchmarkFailureCannotSilentlyOmitObservation(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.opt.Tokens = map[string]map[string]any{r.inst.TokenID: {"symbol": "BTC"}}
	r.s.store = &unavailableCoverageStore{Store: r.mem, failAt: 1, benchmark: true}
	r.s.runObligation(context.Background(), map[string]any{
		"backfill_id": "BF-benchmark-failed", "event_id": "EVT-benchmark-failed", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	})
	stored := r.mem.rows["mkt.backfill_runs"]["BF-benchmark-failed"]
	if stored["status"] != "FAILED" || stored["reason"] != "BENCHMARK_CAPTURE_FAILED" || stored["result"] != nil {
		t.Fatalf("failed benchmark was silently omitted from terminal coverage: %v", stored)
	}
}
