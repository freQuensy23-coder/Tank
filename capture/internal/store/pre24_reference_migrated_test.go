package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pre24ref"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func referenceProjectionFixture(rows []map[string]any, boundary time.Time) (backfillstate.Pre24Reference, error) {
	q, err := pre24ref.NewQuery("synthetic:pre24-wire", boundary)
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	projection := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		var points any
		if row["reference_points"] != nil {
			wire, err := json.Marshal(row["reference_points"])
			if err != nil {
				return backfillstate.Pre24Reference{}, err
			}
			points = string(wire)
		}
		projection = append(projection, map[string]any{"instrument_id": q.InstrumentID(), "minute_start": q.Start(), "reference_points": points})
	}
	observation, err := AdmitPre24Projection(q, projection)
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	return observation.Reference(q)
}

func TestPre24ReferenceDistinguishesUnreportedWitnessFromMalformedConsumedFacts(t *testing.T) {
	boundary := time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC)
	for _, rows := range [][]map[string]any{
		nil,
		{{"reference_points": nil}},
		{{"reference_points": []any{map[string]any{"complete": nil}}}},
		{{"reference_points": []any{map[string]any{"complete": true, "boundary": pytime.ISO(boundary), "last_trade": nil}}}},
	} {
		reference, err := referenceProjectionFixture(rows, boundary)
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
		if _, err := referenceProjectionFixture([]map[string]any{{"reference_points": points}}, boundary); err == nil {
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
	for _, at := range []time.Time{boundary, boundary.Add(time.Microsecond)} {
		if _, err := referenceProjectionFixture([]map[string]any{{"reference_points": []any{point("invalid-predecessor", at)}}}, boundary); err == nil {
			t.Fatal("equal-boundary or future trade became a retained pre24 predecessor")
		}
	}
	if _, err := referenceProjectionFixture([]map[string]any{{"reference_points": []any{a, b}}}, boundary); err == nil {
		t.Fatal("equal-clock conflicting identities acquired an invented ordering")
	}
	reference, err := referenceProjectionFixture([]map[string]any{{"reference_points": []any{a, a}}}, boundary)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(reference)
	if err != nil || !strings.Contains(string(wire), `"trade_id":"trade-a"`) || !strings.Contains(string(wire), `"ts_prec":null`) {
		t.Fatal("equivalent observed witness changed or acquired invented precision")
	}
	latest := point("observed-latest", boundary.Add(-500*time.Millisecond))
	reference, err = referenceProjectionFixture([]map[string]any{{"reference_points": []any{a, b, latest}}}, boundary)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = json.Marshal(reference)
	if err != nil || !strings.Contains(string(wire), `"trade_id":"observed-latest"`) || strings.Contains(string(wire), "trade-a") {
		t.Fatal("unconsumed older evidence displaced the actual latest clock witness")
	}
}
func TestReferenceTradeIdentityRejectsNonIntegerJSONNumbers(t *testing.T) {
	for _, raw := range []json.Number{"null", "true", "[]", "1.5", "2e4", ""} {
		t.Run(string(raw), func(t *testing.T) {
			if _, err := referenceTradeIdentity(raw); err == nil {
				t.Fatalf("non-integer JSON value entered an integer trade identity: %q", raw)
			}
		})
	}
}
