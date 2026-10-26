package backfillstate

import (
	"bytes"
	"testing"
	"time"
)

func observedReference(t *testing.T) Reference {
	t.Helper()
	value, err := NewReference("BF-record", "48123")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func queuedRecord() Record {
	return Record{
		StorageKey: "BF-record", BackfillID: "BF-record", EventID: text("EVT-observed"),
		TokenID: text("TKN-observed"), Status: text("PENDING"), Anchor: stamp("2026-10-01T12:00:00.123456+02:00"),
		AnchorPrec: text("EXACT"), StartedAtPrecReason: text("PENDING"),
	}
}

func TestObligationRequiresAnActuallyObservedReference(t *testing.T) {
	if err := (Obligation{}).Validate(); err == nil {
		t.Fatal("zero command acquired actual obligation facts")
	}
	for _, pair := range [][2]string{{"", "48123"}, {"BF-record", ""}, {"BF-record", " "}} {
		if _, err := NewReference(pair[0], pair[1]); err == nil {
			t.Fatal("missing actual reference acquired a replacement")
		}
	}
	if _, err := Observe(Reference{}, queuedRecord()); err == nil {
		t.Fatal("zero reference admitted an obligation")
	}
	row := queuedRecord()
	row.StorageKey = "another-row"
	if _, err := Observe(observedReference(t), row); err == nil {
		t.Fatal("projection was detached from its storage identity")
	}
}

func TestQueuedAndRestartedObligationsKeepTheirActualAnchorWithoutAddingClocks(t *testing.T) {
	for _, status := range []string{"PENDING", "RUNNING"} {
		row := queuedRecord()
		row.Status = &status
		prior := physicalDocument(t, row)
		observation, err := Observe(observedReference(t), row)
		if err != nil {
			t.Fatal(err)
		}
		command, ok := observation.Selection().(Obligation)
		if !ok || command.BackfillID() != "BF-record" || command.EventID() != "EVT-observed" || command.TokenID() != "TKN-observed" {
			t.Fatalf("actual identities were not admitted: %#v", observation.Selection())
		}
		if err := command.Validate(); err != nil {
			t.Fatal(err)
		}
		want := time.Date(2026, 10, 1, 10, 0, 0, 123456000, time.UTC)
		if !command.Anchor().Equal(want) || observation.Reference().Revision() != "48123" {
			t.Fatal("actual anchor or observed revision changed")
		}
		if (status == "PENDING" && command.Phase() != Pending) || (status == "RUNNING" && command.Phase() != Running) {
			t.Fatal("actual pending/running phase was replaced")
		}
		if !bytes.Equal(prior, physicalDocument(t, row)) || row.StartedAt != nil || row.FinishedAt != nil {
			t.Fatal("intake manufactured worker clocks or changed retained provenance")
		}
	}
}

func TestMissingAnchorRefusesBeforeOtherMalformedCommandFacts(t *testing.T) {
	row := queuedRecord()
	row.Anchor, row.TokenID, row.EventID = nil, nil, nil
	observation, err := Observe(observedReference(t), row)
	if err != nil {
		t.Fatal(err)
	}
	refusal, ok := observation.Selection().(Refused)
	if !ok || refusal.Reason() != "anchor unreadable: unparseable timestamp: missing" {
		t.Fatalf("anchor precedence or public reason changed: %#v", observation.Selection())
	}
}

func TestUnverifiedAnchorTextCannotAcquireAnImplicitUnitOrZone(t *testing.T) {
	for _, raw := range []string{"0", "1727784000000", "2026-10-01T12:00:00", "malformed"} {
		row := queuedRecord()
		row.Anchor = stamp(raw)
		prior := physicalDocument(t, row)
		observation, err := Observe(observedReference(t), row)
		if err != nil {
			t.Fatal(err)
		}
		refusal, ok := observation.Selection().(Refused)
		if !ok || refusal.Reason() == "" || !bytes.Equal(prior, physicalDocument(t, row)) {
			t.Fatalf("unverified anchor was guessed or rewritten: %q", raw)
		}
	}
}

func TestMalformedNullableIdentityRemainsARefusalInsteadOfAValidFailedRecord(t *testing.T) {
	for _, field := range []string{"backfill_id", "token_id", "event_id", "status"} {
		row := queuedRecord()
		var expected string
		switch field {
		case "backfill_id":
			row.BackfillID, expected = "other-row", "BACKFILL_ID_INVALID"
		case "token_id":
			row.TokenID, expected = nil, "TOKEN_ID_INVALID"
		case "event_id":
			row.EventID, expected = nil, "EVENT_ID_INVALID"
		case "status":
			row.Status, expected = nil, "BACKFILL_STATUS_INVALID"
		}
		prior := physicalDocument(t, row)
		observation, err := Observe(observedReference(t), row)
		if err != nil {
			t.Fatal(err)
		}
		refusal, ok := observation.Selection().(Refused)
		if !ok || refusal.Reason() != expected || !bytes.Equal(prior, physicalDocument(t, row)) {
			t.Fatalf("malformed %s was repaired or mislabeled: %#v", field, observation.Selection())
		}
	}
}
