package backfillstate

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func observedWindows(t *testing.T, anchor time.Time) CaptureWindows {
	t.Helper()
	var windows [3]WindowCoverage
	for index, hours := range []int{1, 4, 24} {
		start := anchor.Add(-time.Duration(hours) * time.Hour)
		lo := pytime.MinuteFloor(start)
		if start.After(lo) {
			lo = lo.Add(time.Minute)
		}
		hi := pytime.MinuteFloor(anchor)
		expected := int64(hi.Sub(lo) / time.Minute)
		minutes, err := NewMinuteCoverage(expected, expected)
		if err != nil {
			t.Fatal(err)
		}
		windows[index], err = NewWindowCoverage(start, anchor, lo, hi, lo.Sub(start).Seconds()+anchor.Sub(hi).Seconds(), minutes, minutes)
		if err != nil {
			t.Fatal(err)
		}
	}
	value, err := NewCaptureWindows(windows[0], windows[1], windows[2])
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func observedBenchmarks(t *testing.T, anchor time.Time) []BenchmarkCoverage {
	t.Helper()
	start := pytime.MinuteFloor(anchor.Add(-24 * time.Hour)).Add(-time.Minute)
	end := pytime.MinuteFloor(anchor)
	expected := int64(end.Sub(start) / time.Minute)
	values := make([]BenchmarkCoverage, 2)
	for index, id := range []string{"BTC-fixture", "ETH-fixture"} {
		value, err := NewBenchmarkCoverage(id, start, end, expected, expected, true)
		if err != nil {
			t.Fatal(err)
		}
		values[index] = value
	}
	return values
}

func TestNamedBackfillResultBindsActualScopeAndCompleteCoverage(t *testing.T) {
	anchor := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	instrument, err := NonSpotInstrumentCoverage("INSTR-fixture", observedWindows(t, anchor))
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewResult("TOKEN-fixture", anchor, []InstrumentCoverage{instrument}, observedBenchmarks(t, anchor))
	if err != nil {
		t.Fatal(err)
	}
	ready, err := result.Ready()
	if err != nil || !ready {
		t.Fatalf("observed complete scope was not ready: %v", err)
	}
	if err := result.ValidateScope("TOKEN-fixture", anchor); err != nil {
		t.Fatal(err)
	}
	if err := result.ValidateScope("OTHER", anchor); err == nil {
		t.Fatal("cross-token result was admitted")
	}
	if err := result.ValidateScope("TOKEN-fixture", anchor.Add(time.Minute)); err == nil {
		t.Fatal("cross-anchor result was admitted")
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"unresolved_boundary_seconds":0.0`)) || bytes.Contains(data, []byte(`"pre24_reference"`)) || bytes.Contains(data, []byte(`"tokenID"`)) {
		t.Fatalf("wire shape or Python float kind changed: %s", data)
	}
}

func TestNamedBackfillWireCannotSilentlyTruncateObservedNanoseconds(t *testing.T) {
	anchor := time.Date(2026, 10, 26, 1, 0, 0, 123456789, time.UTC)
	if _, err := NewResult("TOKEN-fixture", anchor, nil, nil); err == nil {
		t.Fatal("named result silently admitted an anchor its wire formatter truncates")
	}
	minutes, err := NewMinuteCoverage(59, 59)
	if err != nil {
		t.Fatal(err)
	}
	start := anchor.Add(-time.Hour)
	lo, hi := pytime.MinuteFloor(start).Add(time.Minute), pytime.MinuteFloor(anchor)
	if _, err := NewWindowCoverage(start, anchor, lo, hi, lo.Sub(start).Seconds()+anchor.Sub(hi).Seconds(), minutes, minutes); err == nil {
		t.Fatal("window silently admitted bounds its wire formatter truncates")
	}
	if _, err := NewBenchmarkCoverage("BENCH-fixture", start, anchor, 60, 60, true); err == nil {
		t.Fatal("benchmark silently admitted bounds its wire formatter truncates")
	}
}

func TestSpotReferenceMustActuallyPrecedeItsDeclaredPre24Origin(t *testing.T) {
	anchor := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	identity, err := TextTradeIdentity("observed-predecessor")
	if err != nil {
		t.Fatal(err)
	}
	boundary := anchor.Add(-24 * time.Hour)
	for _, at := range []time.Time{boundary, boundary.Add(time.Microsecond)} {
		reference, err := AvailablePre24Reference(identity, pytime.ISO(at), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := SpotInstrumentCoverage("SPOT-fixture", observedWindows(t, anchor), reference); err == nil {
			t.Fatal("equal-boundary or future trade became a valid pre24 predecessor")
		}
	}
	reference, err := AvailablePre24Reference(identity, pytime.ISO(boundary.Add(-time.Microsecond)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SpotInstrumentCoverage("SPOT-fixture", observedWindows(t, anchor), reference); err != nil {
		t.Fatal(err)
	}
}

func TestSpotMissingReferenceAndUnresolvedBoundaryNeverBecomeComplete(t *testing.T) {
	aligned := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	for _, anchor := range []time.Time{aligned, aligned.Add(17 * time.Second)} {
		coverage, err := SpotInstrumentCoverage("SPOT-fixture", observedWindows(t, anchor), MissingPre24Reference())
		if err != nil {
			t.Fatal(err)
		}
		result, err := NewResult("TOKEN-fixture", anchor, []InstrumentCoverage{coverage}, observedBenchmarks(t, anchor))
		if err != nil {
			t.Fatal(err)
		}
		ready, err := result.Ready()
		if err != nil || ready {
			t.Fatal("missing reference acquired completed coverage")
		}
		wire, err := json.Marshal(coverage)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(wire, []byte(`"available":false,"trade_id":null,"ts":null,"ts_prec":null,"reason":"INCOMPLETE"`)) {
			t.Fatalf("unavailable reference changed: %s", wire)
		}
	}
	instrument, err := NonSpotInstrumentCoverage("INSTR-fixture", observedWindows(t, aligned.Add(17*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if instrument.Complete() {
		t.Fatal("sixty unresolved boundary seconds were hidden by complete interior minutes")
	}
}

func TestReferenceIdentityPreservesTextIntegerAndUnknownPrecision(t *testing.T) {
	integer, err := IntegerTradeIdentity(json.Number("900719925474099312345678901"))
	if err != nil {
		t.Fatal(err)
	}
	textID, err := TextTradeIdentity("000.10")
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct {
		identity TradeIdentity
		expected string
	}{{integer, `900719925474099312345678901`}, {textID, `"000.10"`}} {
		reference, err := AvailablePre24Reference(input.identity, "2026-10-25T00:00:00Z", nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(reference)
		if err != nil {
			t.Fatal(err)
		}
		var wire struct {
			ID        json.RawMessage `json:"trade_id"`
			Precision *string         `json:"ts_prec"`
			Available bool            `json:"available"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			t.Fatal(err)
		}
		if string(wire.ID) != input.expected || wire.Precision != nil || !wire.Available {
			t.Fatalf("reference acquired fabricated identity or precision: %s", data)
		}
	}
}

func TestZeroDomainValuesAreNotDeclaredObservations(t *testing.T) {
	for _, value := range []json.Marshaler{MinuteCoverage{}, WindowCoverage{}, CaptureWindows{}, TradeIdentity{}, Pre24Reference{}, InstrumentCoverage{}, BenchmarkCoverage{}, Result{}} {
		if _, err := value.MarshalJSON(); err == nil {
			t.Fatalf("uninitialized domain value was serialized: %T", value)
		}
	}
	anchor := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	empty, err := NewResult("TOKEN-fixture", anchor, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := empty.Ready()
	if err != nil || ready {
		t.Fatal("explicit empty observed scope became ready")
	}
	data, err := json.Marshal(empty)
	if err != nil || string(data) != `{"instruments":[],"benchmark_klines":[]}` {
		t.Fatalf("explicit empty scope became missing fields: %s %v", data, err)
	}
}

func TestInvalidCoverageCannotAcquireACompleteOrPartialResult(t *testing.T) {
	for _, counts := range [][2]int64{{-1, 0}, {1, -1}, {1, 2}} {
		if _, err := NewMinuteCoverage(counts[0], counts[1]); err == nil {
			t.Fatal("invalid observed minute counts were admitted")
		}
	}
	anchor := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	minutes, err := NewMinuteCoverage(60, 60)
	if err != nil {
		t.Fatal(err)
	}
	for _, unresolved := range []float64{math.NaN(), math.Inf(1), -1, 60} {
		if _, err := NewWindowCoverage(anchor.Add(-time.Hour), anchor, anchor.Add(-time.Hour), anchor, unresolved, minutes, minutes); err == nil {
			t.Fatal("invalid unresolved boundary declaration was admitted")
		}
	}
	wrong := observedWindows(t, anchor.Add(time.Minute))
	instrument, err := NonSpotInstrumentCoverage("INSTR-fixture", wrong)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewResult("TOKEN-fixture", anchor, []InstrumentCoverage{instrument}, nil); err == nil {
		t.Fatal("cross-origin instrument was admitted")
	}
	correct, err := NonSpotInstrumentCoverage("INSTR-fixture", observedWindows(t, anchor))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewResult("TOKEN-fixture", anchor, []InstrumentCoverage{correct, correct}, nil); err == nil {
		t.Fatal("duplicate instrument silently counted twice")
	}
	short, err := NewBenchmarkCoverage("BTC-fixture", anchor.Add(-time.Minute), anchor, 1, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewResult("TOKEN-fixture", anchor, []InstrumentCoverage{correct}, []BenchmarkCoverage{short}); err == nil {
		t.Fatal("short benchmark was promoted to pre24 coverage")
	}
}
