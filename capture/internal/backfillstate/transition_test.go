package backfillstate

import (
	"testing"
	"time"
)

func admittedObservation(t *testing.T) Observation {
	t.Helper()
	observation, err := Observe(observedReference(t), queuedRecord())
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func observedWriter(t *testing.T) WriterIdentity {
	t.Helper()
	writer, err := NewWriterIdentity("synthetic-config-captured", "synthetic-code-captured")
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func TestTransitionsRequireActualWriterAndCollectionClockFacts(t *testing.T) {
	observation := admittedObservation(t)
	clock := time.Date(2026, 10, 26, 1, 2, 3, 123456000, time.UTC)
	if _, err := NewWriterIdentity("", "synthetic-code"); err == nil {
		t.Fatal("missing captured configuration acquired a writer identity")
	}
	if _, err := NewWriterIdentity("synthetic-config", ""); err == nil {
		t.Fatal("missing captured code acquired a writer identity")
	}
	if _, err := Begin(observation, clock, clock, WriterIdentity{}); err == nil {
		t.Fatal("zero writer acquired a processing identity")
	}
	for _, missing := range []time.Time{{}, clock.Add(time.Nanosecond)} {
		if _, err := Begin(observation, missing, clock, observedWriter(t)); err == nil {
			t.Fatal("missing or unpersistable start clock was silently replaced")
		}
		if _, err := Begin(observation, clock, missing, observedWriter(t)); err == nil {
			t.Fatal("missing or unpersistable recorded clock was silently replaced")
		}
	}
	start, err := Begin(observation, clock, clock.Add(time.Microsecond), observedWriter(t))
	if err != nil || !start.StartedAt().Equal(clock) || !start.RecordedAt().Equal(clock.Add(time.Microsecond)) {
		t.Fatalf("distinct actual processing clocks changed: %v", err)
	}
}

func TestStartAcknowledgementAdvancesOnlyTheSameActualRowRevision(t *testing.T) {
	observation := admittedObservation(t)
	for _, identity := range [][2]string{{"other-row", "48124"}, {"BF-record", "48123"}} {
		reference, err := NewReference(identity[0], identity[1])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := observation.AcknowledgeStart(reference); err == nil {
			t.Fatal("another row or unchanged revision acknowledged a write")
		}
	}
	reference, err := NewReference("BF-record", "48124")
	if err != nil {
		t.Fatal(err)
	}
	running, err := observation.AcknowledgeStart(reference)
	if err != nil || running.Reference().Revision() != "48124" {
		t.Fatalf("actual revised row was not acknowledged: %v", err)
	}
	command, ok := running.Selection().(Obligation)
	if !ok || command.Phase() != Running || observation.Selection().(Obligation).Phase() != Pending {
		t.Fatal("start acknowledgement changed the old observation or failed to record running phase")
	}
}

func TestRefusalCannotStartOrBorrowAnotherObservedFailure(t *testing.T) {
	row := queuedRecord()
	row.Anchor = nil
	observation, err := Observe(observedReference(t), row)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 10, 26, 1, 2, 3, 0, time.UTC)
	if _, err := Begin(observation, clock, clock, observedWriter(t)); err == nil {
		t.Fatal("missing anchor started capture")
	}
	other, err := Failed(RepairFailed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Finish(observation, other, clock, clock, observedWriter(t)); err == nil {
		t.Fatal("missing anchor borrowed a repair failure")
	}
	failure, err := RefusalOutcome(observation.Selection().(Refused))
	if err != nil {
		t.Fatal(err)
	}
	completion, err := Finish(observation, failure, clock, clock, observedWriter(t))
	if err != nil || completion.Outcome().Status() != "FAILED" || *completion.Outcome().Reason() != "anchor unreadable: unparseable timestamp: missing" {
		t.Fatalf("actual refusal could not finish explicitly: %v", err)
	}
	if _, err := Finish(admittedObservation(t), failure, clock, clock, observedWriter(t)); err == nil {
		t.Fatal("another command borrowed the malformed anchor refusal")
	}
}

func TestCompletionBindsCoverageToTheActualCommandAndRejectsTypedNil(t *testing.T) {
	observation := admittedObservation(t)
	reference, err := NewReference("BF-record", "48124")
	if err != nil {
		t.Fatal(err)
	}
	observation, err = observation.AcknowledgeStart(reference)
	if err != nil {
		t.Fatal(err)
	}
	command := observation.Selection().(Obligation)
	clock := time.Date(2026, 10, 26, 1, 2, 3, 0, time.UTC)
	result, err := NewResult("OTHER-TOKEN", command.Anchor(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := Captured(result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Finish(observation, outcome, clock, clock, observedWriter(t)); err == nil {
		t.Fatal("cross-token coverage acquired a valid completion")
	}
	var missing *CaptureOutcome
	if _, err := Finish(observation, missing, clock, clock, observedWriter(t)); err == nil {
		t.Fatal("typed-nil outcome acquired a completion")
	}
	result, err = NewResult(command.TokenID(), command.Anchor(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err = Captured(result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Finish(admittedObservation(t), outcome, clock, clock, observedWriter(t)); err == nil {
		t.Fatal("unacknowledged pending command acquired a captured terminal result")
	}
	completion, err := Finish(observation, outcome, clock, clock, observedWriter(t))
	if err != nil || completion.Outcome().Status() != "PARTIAL" {
		t.Fatalf("actual observed empty coverage did not remain partial: %v", err)
	}
}
