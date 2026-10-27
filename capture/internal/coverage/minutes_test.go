package coverage

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func minuteQuery(t *testing.T, series Series, instrument string, start time.Time) Query {
	t.Helper()
	q, err := NewQuery(series, instrument, start, start.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func observedMinute(t *testing.T, q Query, at time.Time, complete bool) Minute {
	t.Helper()
	m, err := NewMinute(q, q.InstrumentID(), at, complete)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCountPreservesFalseMissingAndIdenticalDuplicates(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	q := minuteQuery(t, Flow, "synthetic:flow", at)
	first := observedMinute(t, q, at, true)
	second := observedMinute(t, q, at.Add(time.Minute), false)
	result, err := Count(q, []Minute{second, first, first})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Complete bool  `json:"complete"`
		Expected int64 `json:"expected_minutes"`
		Present  int64 `json:"complete_minutes"`
		Absent   int64 `json:"missing_or_incomplete_minutes"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Complete || wire.Expected != 3 || wire.Present != 1 || wire.Absent != 2 {
		t.Fatalf("missing/false/duplicate evidence changed counts: %s", data)
	}
}

func TestCountRefusesConflictingOrForeignEvidence(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	q := minuteQuery(t, Flow, "synthetic:flow", at)
	a := observedMinute(t, q, at, true)
	b := observedMinute(t, q, at, false)
	if _, err := Count(q, []Minute{a, b}); err == nil || !strings.Contains(err.Error(), "conflicting observations") {
		t.Fatalf("conflict was resolved arbitrarily: %v", err)
	}
	for _, foreign := range []Query{minuteQuery(t, Klines, q.InstrumentID(), at), minuteQuery(t, Flow, "synthetic:other", at)} {
		m := observedMinute(t, foreign, at, true)
		if _, err := Count(q, []Minute{m}); err == nil {
			t.Fatal("foreign query evidence counted")
		}
	}
	if _, err := Count(q, []Minute{{}}); err == nil {
		t.Fatal("uninitialized observation counted")
	}
}

func TestMinuteRefusesUnalignedAndOutsideClocks(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	q := minuteQuery(t, Flow, "synthetic:flow", at)
	for _, stamp := range []time.Time{at.Add(-time.Minute), at.Add(time.Nanosecond), q.End()} {
		if _, err := NewMinute(q, q.InstrumentID(), stamp, true); err == nil || err.Error() != "capture coverage requires an aligned bucket within its declared interval" {
			t.Fatalf("invalid bucket admitted: %v", err)
		}
	}
	if _, err := NewMinute(q, "foreign", at, true); err == nil {
		t.Fatal("foreign instrument admitted")
	}
}

func TestQueryRefusesUninitializedScopeAndPreservesEmptyWindow(t *testing.T) {
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		series     Series
		iid        string
		start, end time.Time
	}{
		{0, "synthetic:flow", at, at.Add(time.Minute)},
		{Series(99), "synthetic:flow", at, at.Add(time.Minute)},
		{Flow, " ", at, at.Add(time.Minute)},
		{Flow, "synthetic:flow", time.Time{}, at},
		{Flow, "synthetic:flow", at, at.Add(-time.Minute)},
		{Flow, "synthetic:flow", at.Add(time.Second), at.Add(time.Minute)},
	} {
		if _, err := NewQuery(test.series, test.iid, test.start, test.end); err == nil {
			t.Fatal("invalid required query facts admitted")
		}
	}
	q, err := NewQuery(Flow, "synthetic:flow", at, at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Count(q, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Count(Query{}, nil); err == nil {
		t.Fatal("uninitialized query fabricated empty coverage")
	}
}
