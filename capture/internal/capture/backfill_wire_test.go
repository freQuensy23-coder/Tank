package capture

import (
	"context"
	"encoding/json"
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

func TestPre24ReferenceDistinguishesUnreportedWitnessFromMalformedConsumedFacts(t *testing.T) {
	boundary := time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC)
	for _, rows := range [][]map[string]any{
		nil,
		{{"reference_points": nil}},
		{{"reference_points": []any{map[string]any{"complete": nil}}}},
		{{"reference_points": []any{map[string]any{"complete": true, "boundary": pytime.ISO(boundary), "last_trade": nil}}}},
	} {
		reference, err := decodePre24Reference(rows, boundary)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := json.Marshal(reference)
		if err != nil || !strings.Contains(string(wire), `"available":false`) || !strings.Contains(string(wire), `"reason":"INCOMPLETE"`) {
			t.Fatal("unreported source witness acquired availability or a guessed clock")
		}
	}
	for _, points := range []any{
		"malformed",
		[]any{nil},
		[]any{map[string]any{"complete": "true"}},
		[]any{map[string]any{"complete": true, "boundary": "malformed"}},
		[]any{map[string]any{"complete": true, "boundary": pytime.ISO(boundary), "last_trade": "malformed"}},
	} {
		if _, err := decodePre24Reference([]map[string]any{{"reference_points": points}}, boundary); err == nil {
			t.Fatal("malformed consumed source facts acquired an ordinary unavailable-witness result")
		}
	}
}

func TestPre24LatestClockRequiresConsistentActualTradeEvidence(t *testing.T) {
	boundary := time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC)
	point := func(id string, at time.Time) map[string]any {
		return map[string]any{"complete": true, "boundary": pytime.ISO(boundary), "last_trade": map[string]any{
			"trade_id": id, "ts": pytime.ISO(at), "ts_prec": nil,
		}}
	}
	a, b := point("trade-a", boundary.Add(-time.Second)), point("trade-b", boundary.Add(-time.Second))
	if _, err := decodePre24Reference([]map[string]any{{"reference_points": []any{a, b}}}, boundary); err == nil {
		t.Fatal("equal-clock conflicting identities acquired an invented ordering")
	}
	reference, err := decodePre24Reference([]map[string]any{{"reference_points": []any{a, a}}}, boundary)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(reference)
	if err != nil || !strings.Contains(string(wire), `"trade_id":"trade-a"`) || !strings.Contains(string(wire), `"ts_prec":null`) {
		t.Fatal("equivalent observed witness changed or acquired invented precision")
	}
	latest := point("observed-latest", boundary)
	reference, err = decodePre24Reference([]map[string]any{{"reference_points": []any{a, b, latest}}}, boundary)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = json.Marshal(reference)
	if err != nil || !strings.Contains(string(wire), `"trade_id":"observed-latest"`) || strings.Contains(string(wire), "trade-a") {
		t.Fatal("unconsumed older evidence displaced the actual latest clock witness")
	}
}
