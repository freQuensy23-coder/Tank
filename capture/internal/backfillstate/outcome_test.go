package backfillstate

import (
	"testing"
	"time"
)

func TestCapturedStatusAndReasonAreDerivedFromActualCoverage(t *testing.T) {
	anchor := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	complete, err := NonSpotInstrumentCoverage("INSTR-observed", observedWindows(t, anchor))
	if err != nil {
		t.Fatal(err)
	}
	for _, instruments := range [][]InstrumentCoverage{{complete}, {}} {
		result, err := NewResult("TOKEN-observed", anchor, instruments, nil)
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := Captured(result)
		if err != nil {
			t.Fatal(err)
		}
		if len(instruments) == 1 {
			if outcome.Status() != "COMPLETE" || outcome.Reason() != nil {
				t.Fatal("observed complete coverage acquired a partial reason")
			}
		} else if outcome.Status() != "PARTIAL" || outcome.Reason() == nil || *outcome.Reason() != "INCOMPLETE_VENUE_WINDOWS_EXCLUDED" {
			t.Fatal("observed empty coverage acquired a complete status")
		}
		if _, err := outcome.JSONResult(); err != nil {
			t.Fatal(err)
		}
		outcome.complete = !outcome.complete
		if err := outcome.Validate(); err == nil {
			t.Fatal("mismatched complete/partial status was admitted")
		}
	}
	if _, err := Captured(Result{}); err == nil {
		t.Fatal("zero result acquired a terminal outcome")
	}
}

func TestLiveFailureKindsAreClosedAndCannotBorrowAnotherReason(t *testing.T) {
	for _, item := range []struct {
		kind   FailureKind
		reason string
	}{{BenchmarkCaptureFailed, "BENCHMARK_CAPTURE_FAILED"}, {RepairFailed, "REPAIR_FAILED"}, {CoverageReadFailed, "COVERAGE_READ_FAILED"}} {
		failure, err := Failed(item.kind)
		if err != nil || failure.Status() != "FAILED" || failure.Reason() == nil || *failure.Reason() != item.reason {
			t.Fatalf("public failure reason changed: %v", err)
		}
		failure.detail = "COVERAGE_READ_FAILED"
		if err := failure.Validate(); err == nil {
			t.Fatal("fixed failure borrowed another reason")
		}
	}
	for _, kind := range []FailureKind{0, 99, WorkerPanicked, CommandRefused} {
		if _, err := Failed(kind); err == nil {
			t.Fatal("unknown or unobserved failure was admitted")
		}
	}
	if _, err := Panicked(""); err == nil {
		t.Fatal("unobserved panic acquired a reason")
	}
	if _, err := Panicked(string([]byte{0xff})); err == nil {
		t.Fatal("invalid panic text was silently replaced during persistence")
	}
	failure, err := Panicked("actual panic observation")
	if err != nil || *failure.Reason() != "actual panic observation" {
		t.Fatal("actual panic observation changed")
	}
}

func TestCommandFailureRequiresItsActualClosedRefusal(t *testing.T) {
	for _, refusal := range []Refused{{}, {kind: refusalKind(99)}, {kind: tokenIdentityInvalid, detail: "REPAIR_FAILED"}, {kind: anchorUnreadable, detail: "REPAIR_FAILED"}, {kind: anchorUnreadable, detail: `anchor unreadable: unparseable timestamp: "2026-10-01T12:00:00Z"`}} {
		if _, err := RefusalOutcome(refusal); err == nil {
			t.Fatal("unknown or mismatched refusal acquired a terminal outcome")
		}
	}
	row := queuedRecord()
	row.Anchor = nil
	observation, err := Observe(observedReference(t), row)
	if err != nil {
		t.Fatal(err)
	}
	refusal, ok := observation.Selection().(Refused)
	if !ok {
		t.Fatal("missing anchor did not produce a refusal")
	}
	failure, err := RefusalOutcome(refusal)
	if err != nil || failure.Status() != "FAILED" || failure.Reason() == nil || *failure.Reason() != "anchor unreadable: unparseable timestamp: missing" {
		t.Fatalf("actual anchor refusal changed: %v", err)
	}
	failure.detail = "different failure"
	if err := failure.Validate(); err == nil {
		t.Fatal("command refusal borrowed unrelated error detail")
	}
}
