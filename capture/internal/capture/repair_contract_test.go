package capture

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/coverage"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
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
	row := pendingRequest("CR-read-failed", r.inst.TokenID, r.base, r.base.Add(30*time.Minute), r.clock.Now().Add(time.Minute))
	r.mem.requests = []requeststate.Record{row}
	claimed, err := r.mem.ClaimRequest(context.Background(), "repair-test", r.clock.Now())
	if err != nil || claimed == nil {
		t.Fatalf("claim failed: %v", err)
	}
	r.s.runRequest(context.Background(), *claimed)
	stored := r.mem.requests[0]
	if stored.Status == nil || *stored.Status != "FAILED" || stored.Reason == nil || *stored.Reason != "REPAIR_FAILED" || stored.FinishedAt == nil {
		t.Fatalf("coverage backend failure became successful repair: %+v", stored)
	}
	if stored.ResultJSON == nil {
		t.Fatal("partial repair observations absent")
	}
	var result requeststate.RepairResult
	if err := json.Unmarshal([]byte(*stored.ResultJSON), &result); err != nil {
		t.Fatal(err)
	}
	if result.Instruments == nil || len(result.Instruments) != 0 {
		t.Fatalf("unavailable observations became fabricated counts: %+v", result)
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
	r.s.runObligation(context.Background(), queuedBackfill(t, r.mem, "BF-read-failed", map[string]any{
		"backfill_id": "BF-read-failed", "event_id": "EVT-read-failed", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	}))
	stored := r.mem.rows["mkt.backfill_runs"]["BF-read-failed"]
	if stored["status"] != "FAILED" || stored["reason"] != "REPAIR_FAILED" || stored["finished_at"] == nil || stored["result"] != nil {
		t.Fatalf("backfill invented availability after a failed repair read: %v", stored)
	}
}

type unavailableCoverageStore struct {
	Store
	failAt          int32
	reads           atomic.Int32
	benchmark       bool
	projectionReads atomic.Int32
}

func (s *unavailableCoverageStore) CoverageMinutes(ctx context.Context, q coverage.Query) ([]coverage.Minute, error) {
	if s.reads.Add(1) >= s.failAt {
		return nil, errors.New("range backend unavailable")
	}
	return s.Store.CoverageMinutes(ctx, q)
}

func (s *unavailableCoverageStore) Range(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error) {
	if s.reads.Add(1) >= s.failAt {
		return nil, errors.New("range backend unavailable")
	}
	return s.Store.Range(ctx, collection, field, start, end, filters)
}

func (s *unavailableCoverageStore) Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, ids []string) ([]map[string]any, error) {
	s.projectionReads.Add(1)
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
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("failed observation became invented counters: coverage=%v error=%v", coverage, err)
			}
		})
	}
}

func TestBackfillCoverageFailureCannotInventPartialResult(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.store = &unavailableCoverageStore{Store: r.mem, failAt: 1}
	r.s.runObligation(context.Background(), queuedBackfill(t, r.mem, "BF-range-failed", map[string]any{
		"backfill_id": "BF-range-failed", "event_id": "EVT-range-failed", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	}))
	stored := r.mem.rows["mkt.backfill_runs"]["BF-range-failed"]
	if stored["status"] != "FAILED" || stored["reason"] != "COVERAGE_READ_FAILED" || stored["result"] != nil {
		t.Fatalf("unavailable coverage became invented terminal counters: %v", stored)
	}
}

func TestBackfillBenchmarkFailureCannotSilentlyOmitObservation(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.opt.Tokens = map[string]map[string]any{r.inst.TokenID: {"symbol": "BTC"}}
	r.s.store = &unavailableCoverageStore{Store: r.mem, failAt: 1, benchmark: true}
	r.s.runObligation(context.Background(), queuedBackfill(t, r.mem, "BF-benchmark-failed", map[string]any{
		"backfill_id": "BF-benchmark-failed", "event_id": "EVT-benchmark-failed", "token_id": r.inst.TokenID,
		"anchor": pytime.ISO(r.base), "status": "PENDING",
	}))
	stored := r.mem.rows["mkt.backfill_runs"]["BF-benchmark-failed"]
	if stored["status"] != "FAILED" || stored["reason"] != "BENCHMARK_CAPTURE_FAILED" || stored["result"] != nil {
		t.Fatalf("failed benchmark was silently omitted from terminal coverage: %v", stored)
	}
}

func TestKlineInvertedIntervalRejectsBeforeStorageEffects(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	store := &unavailableCoverageStore{Store: r.mem}
	r.s.store = store
	_, err := r.s.backfillKlines(context.Background(), r.inst, r.base.Add(time.Minute), r.base, false, "TEST")
	if err == nil || !strings.Contains(err.Error(), "interval end precedes start") {
		t.Fatalf("inverted interval was silently converted to zero expected minutes: %v", err)
	}
	if calls := store.projectionReads.Load(); calls != 0 {
		t.Fatalf("invalid interval reached storage before validation: %d", calls)
	}
}

func TestCoverageRejectsTruthyNonBooleanCompletion(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{
		"malformed-completion": {
			"instrument_id": r.inst.ID, "minute_start": pytime.ISO(r.base.Add(-time.Minute)), "complete": "true",
		},
	}
	_, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
	if err == nil || !strings.Contains(err.Error(), "requires boolean completion") {
		t.Fatalf("truthy wire text became proven complete coverage: %v", err)
	}
}

func TestCoverageReferenceSelectsLatestAndPreservesIntegerIdentity(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	boundary := r.base.Add(-24 * time.Hour)
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{
		"reference": {
			"instrument_id": r.inst.ID, "minute_start": pytime.ISO(boundary.Add(-time.Minute)), "complete": false,
			"reference_points": []any{
				map[string]any{"boundary": pytime.ISO(boundary), "complete": true, "last_trade": map[string]any{
					"trade_id": "older-trade", "ts": pytime.ISO(boundary.Add(-2 * time.Second)), "ts_prec": "EXACT",
				}},
				map[string]any{"boundary": pytime.ISO(boundary), "complete": true, "last_trade": map[string]any{
					"trade_id": json.Number("9007199254740993"), "ts": pytime.ISO(boundary.Add(-time.Second)), "ts_prec": "EXACT",
				}},
			},
		},
	}
	coverage, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(coverage)
	if err != nil {
		t.Fatal(err)
	}
	wire := string(data)
	if !strings.Contains(wire, "\"trade_id\":9007199254740993") || strings.Contains(wire, "older-trade") {
		t.Fatalf("latest reference selection or integer wire identity lost precision: %s", wire)
	}
}

func TestCoverageCountsUniqueBucketsAndRejectsConflicts(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	rows := map[string]map[string]any{}
	for minute := 0; minute < 59; minute++ {
		rows[strconv.Itoa(minute)] = map[string]any{
			"instrument_id": r.inst.ID, "minute_start": pytime.ISO(r.base.Add(time.Duration(minute-60) * time.Minute)), "complete": true,
		}
	}
	rows["duplicate"] = copyRow(rows["58"])
	r.mem.rows["mkt.flow_bars_1m"] = rows
	coverage, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(coverage)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Windows struct {
			One struct {
				Flow struct {
					Complete                   *bool `json:"complete"`
					CompleteMinutes            int64 `json:"complete_minutes"`
					MissingOrIncompleteMinutes int64 `json:"missing_or_incomplete_minutes"`
				} `json:"flow"`
			} `json:"W1H"`
		} `json:"windows"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	window := wire.Windows.One.Flow
	if window.Complete == nil || *window.Complete || window.CompleteMinutes != 59 || window.MissingOrIncompleteMinutes != 1 {
		t.Fatalf("duplicate observation hid a genuinely missing minute: %+v", window)
	}
	rows["duplicate"]["complete"] = false
	if _, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base); err == nil || !strings.Contains(err.Error(), "conflicting observations") {
		t.Fatalf("conflicting duplicate completion flags were resolved arbitrarily: %v", err)
	}
}

func TestCoverageRejectsUnalignedBucket(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{
		"unaligned": {"instrument_id": r.inst.ID, "minute_start": pytime.ISO(r.base.Add(-30 * time.Second)), "complete": true},
	}
	if _, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base); err == nil || !strings.Contains(err.Error(), "aligned bucket") {
		t.Fatalf("non-minute evidence was counted as a complete minute: %v", err)
	}
}

func TestReferenceTradeIdentityRejectsNonIntegerJSONNumbers(t *testing.T) {
	for _, raw := range []json.Number{"null", "true", "[]", "1.5", "2e4", ""} {
		t.Run(string(raw), func(t *testing.T) {
			if _, err := decodeReferenceTradeIdentity(raw); err == nil {
				t.Fatalf("non-integer JSON value entered an integer trade identity: %q", raw)
			}
		})
	}
}
