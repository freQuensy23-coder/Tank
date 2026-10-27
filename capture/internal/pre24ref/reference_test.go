package pre24ref

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
)

func testQuery(t *testing.T) Query {
	t.Helper()
	q, err := NewQuery("synthetic.coin.spot", time.Date(2026, 10, 27, 1, 2, 3, 456789000, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func testWitness(t *testing.T, id string, at time.Time) Candidate {
	t.Helper()
	identity, err := backfillstate.TextTradeIdentity(id)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := backfillstate.AvailablePre24Reference(identity, at.Format(time.RFC3339Nano), nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := Witness(reference)
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}

func TestQueryPreservesActualBoundaryAndBoundedNeighborMinutes(t *testing.T) {
	q := testQuery(t)
	if q.InstrumentID() != "synthetic.coin.spot" || q.Boundary().Nanosecond() != 456789000 || !q.Start().Equal(time.Date(2026, 10, 27, 1, 1, 0, 0, time.UTC)) || !q.End().Equal(time.Date(2026, 10, 27, 1, 3, 0, 0, time.UTC)) {
		t.Fatal("query invented a minute-aligned boundary or changed the declared read interval")
	}
	for _, id := range []string{"", " ", "embedded\x00identity", string([]byte{0xff})} {
		if _, err := NewQuery(id, q.Boundary()); !errors.Is(err, ErrScope) {
			t.Fatal("invalid instrument scope admitted")
		}
	}
	if _, err := NewQuery(q.InstrumentID(), time.Time{}); !errors.Is(err, ErrScope) {
		t.Fatal("missing boundary admitted")
	}
}

func TestObservedEmptyIsMissingButZeroAndForeignObservationsRefuse(t *testing.T) {
	q := testQuery(t)
	observed, err := NewObservation(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := observed.Reference(q)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(reference)
	if err != nil || string(wire) != `{"available":false,"trade_id":null,"ts":null,"ts_prec":null,"reason":"INCOMPLETE"}` {
		t.Fatal("an observed empty read acquired a witness or invented source facts")
	}
	if _, err := (Observation{}).Reference(q); !errors.Is(err, ErrObservation) {
		t.Fatal("an unexecuted read became an observed empty result")
	}
	foreign, err := NewQuery("other-instrument", q.Boundary())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observed.Reference(foreign); !errors.Is(err, ErrObservation) {
		t.Fatal("observation escaped its actual instrument scope")
	}
	foreign, err = NewQuery(q.InstrumentID(), q.Boundary().Add(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observed.Reference(foreign); !errors.Is(err, ErrObservation) {
		t.Fatal("observation escaped its actual boundary")
	}
}

func TestOnlyLatestCandidatesConsumeTheirIdentityAdmission(t *testing.T) {
	q := testQuery(t)
	invalidIdentity := errors.New("reference trade identity requires text or an integer")
	older, err := Refusal(q.Boundary().Add(-2*time.Second), invalidIdentity)
	if err != nil {
		t.Fatal(err)
	}
	latest := testWitness(t, "actual-latest", q.Boundary().Add(-time.Second))
	for _, order := range [][]Candidate{{older, latest}, {latest, older}} {
		observation, err := NewObservation(q, order)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := observation.Reference(q)
		wire, encodeErr := json.Marshal(reference)
		if err != nil || encodeErr != nil || !strings.Contains(string(wire), `"trade_id":"actual-latest"`) {
			t.Fatal("unconsumed older identity was validated instead of selecting the actual latest clock")
		}
	}
	refusal, err := Refusal(q.Boundary().Add(-time.Second), invalidIdentity)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := NewObservation(q, []Candidate{older, refusal})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observation.Reference(q); !errors.Is(err, invalidIdentity) {
		t.Fatal("malformed latest identity acquired an available or ordinary missing result")
	}
}

func TestEqualClockEquivalentWitnessesAgreeAndConflictingIdentitiesRefuse(t *testing.T) {
	q := testQuery(t)
	at := q.Boundary().Add(-time.Second)
	first := testWitness(t, "actual-first", at)
	for _, order := range [][]Candidate{{first, testWitness(t, "actual-second", at)}, {testWitness(t, "actual-second", at), first}} {
		observation, err := NewObservation(q, order)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := observation.Reference(q); !errors.Is(err, ErrConflict) {
			t.Fatal("equal-clock conflict acquired an invented ordering")
		}
	}
	input := []Candidate{first, first}
	observation, err := NewObservation(q, input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = testWitness(t, "caller-mutated", at.Add(time.Millisecond))
	reference, err := observation.Reference(q)
	wire, encodeErr := json.Marshal(reference)
	if err != nil || encodeErr != nil || !strings.Contains(string(wire), `"trade_id":"actual-first"`) {
		t.Fatal("equivalent retained witnesses or owned input snapshot changed")
	}
}

func TestNonPredecessorsAndUnreportedCandidatesRefuse(t *testing.T) {
	q := testQuery(t)
	for _, at := range []time.Time{q.Boundary(), q.Boundary().Add(time.Microsecond)} {
		if _, err := NewObservation(q, []Candidate{testWitness(t, "not-a-predecessor", at)}); !errors.Is(err, ErrPredecessor) {
			t.Fatal("equal or future trade acquired predecessor status")
		}
	}
	if _, err := NewObservation(q, []Candidate{nil}); !errors.Is(err, ErrWitness) {
		t.Fatal("unreported candidate admitted")
	}
	if _, err := Witness(backfillstate.MissingPre24Reference()); !errors.Is(err, ErrWitness) {
		t.Fatal("ordinary missing observation acquired a candidate clock")
	}
	if _, err := Refusal(q.Boundary().Add(-time.Second), nil); !errors.Is(err, ErrWitness) {
		t.Fatal("a refusal without actual failed admission was invented")
	}
}

func TestExplicitYearOneClockRemainsObservedRatherThanMissing(t *testing.T) {
	q := testQuery(t)
	identity, err := backfillstate.TextTradeIdentity("actual-explicit-clock")
	if err != nil {
		t.Fatal(err)
	}
	reference, err := backfillstate.AvailablePre24Reference(identity, "0001-01-01T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	at, reported, err := reference.ObservationClock()
	if err != nil || !reported || !at.IsZero() {
		t.Fatal("the existing valid JSON timestamp was not admitted as an explicit clock")
	}
	candidate, err := Witness(reference)
	if err != nil {
		t.Fatal("Go's zero time representation erased an explicitly reported source timestamp")
	}
	observation, err := NewObservation(q, []Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := observation.Reference(q)
	wire, encodeErr := json.Marshal(selected)
	if err != nil || encodeErr != nil || !strings.Contains(string(wire), `"available":true`) || !strings.Contains(string(wire), `"ts":"0001-01-01T00:00:00Z"`) {
		t.Fatal("an explicit source timestamp became ordinary unavailable evidence")
	}
}
