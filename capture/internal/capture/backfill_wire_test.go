package capture

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func TestMalformedStoredKlineCompletionRefusesBeforeAnyProviderFetch(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.mem.rows["mkt.klines_1m"] = map[string]map[string]any{
		"actual-malformed-row": {"instrument_id": r.inst.ID, "open_time": pytime.ISO(r.base.Add(-time.Minute)), "complete": "true"},
	}
	_, err := r.s.backfillKlines(context.Background(), r.inst, r.base.Add(-time.Minute), r.base, true, "PRE_EVENT_KLINES")
	if err == nil || !strings.Contains(err.Error(), "boolean or unreported completion") || r.v.klineCalls.Load() != 0 || r.mem.writes["mkt.klines_1m"] != 0 {
		t.Fatalf("malformed persisted completion became missing coverage and triggered a fetch: %v", err)
	}
}

func TestMalformedBenchmarkSymbolCannotBecomeAnEmptySuccessfulObservation(t *testing.T) {
	for _, raw := range []any{nil, 123, true, "", " "} {
		r := newRESTRig(t, "coinbase")
		r.s.opt.Tokens = map[string]map[string]any{r.inst.TokenID: {"symbol": raw}}
		if _, err := r.s.backfillBenchmarkKlines(context.Background(), r.base); err == nil || !strings.Contains(err.Error(), "requires actual nonblank") || r.v.klineCalls.Load() != 0 {
			t.Fatalf("malformed symbol was converted to an absent benchmark: %v", err)
		}
	}
}

func TestStoredKlineUnknownCompletionRemainsUnreportedAndConflictsRefuse(t *testing.T) {
	at := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	unknown := map[string]any{"open_time": pytime.ISO(at), "complete": nil}
	observed, err := decodeCompletedKlineMinutes([]map[string]any{unknown}, at, at.Add(time.Minute), at.Add(time.Minute))
	if err != nil || observed.contains(at) {
		t.Fatal("uncaptured completion became a verified complete minute")
	}
	if _, err := decodeCompletedKlineMinutes([]map[string]any{unknown, {"open_time": pytime.ISO(at), "complete": true}}, at, at.Add(time.Minute), at.Add(time.Minute)); err == nil {
		t.Fatal("conflicting duplicate evidence arbitrarily selected a complete minute")
	}
	for _, raw := range []any{at.UnixMilli(), pytime.ISO(at)[:19], "malformed"} {
		if _, err := decodeCompletedKlineMinutes([]map[string]any{{"open_time": raw, "complete": true}}, at, at.Add(time.Minute), at.Add(time.Minute)); err == nil {
			t.Fatalf("stored clock acquired an inferred unit or zone: %v", raw)
		}
	}
}

func TestMalformedReferenceArrayIsAReadFailureInsteadOfAnUnavailableWitness(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	boundary := r.base.Add(-24 * time.Hour)
	r.mem.rows["mkt.flow_bars_1m"] = map[string]map[string]any{
		"actual-malformed-reference": {
			"instrument_id": r.inst.ID, "minute_start": pytime.ISO(boundary), "complete": false,
			"reference_points": "malformed retained JSON shape",
		},
	}
	if _, err := r.s.captureWindowCoverage(context.Background(), r.inst, r.base); err == nil || !strings.Contains(err.Error(), "require a JSON array") {
		t.Fatalf("malformed reference became an ordinary partial result: %v", err)
	}
	if r.mem.rows["mkt.flow_bars_1m"]["actual-malformed-reference"]["reference_points"] != "malformed retained JSON shape" {
		t.Fatal("read refusal sanitized stored evidence")
	}
}
