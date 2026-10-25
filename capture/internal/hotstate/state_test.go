package hotstate

import (
	"encoding/json"
	"testing"
	"time"
)

func text(value string) *string { return &value }
func fixture() Record {
	since := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	return Record{StorageKey: "physical-hot", TokenID: "token", EventID: "event", Since: &since, SinceReason: text("LEGACY_PRECISION_UNREPORTED")}
}
func anchor(hot Record, id string, at time.Time, started *time.Time) AnchorRecord {
	return AnchorRecord{StorageKey: "storage-" + id, BackfillID: id, TokenID: &hot.TokenID, EventID: &hot.EventID, Anchor: &at, AnchorPrec: text("BUCKET"), StartedAt: started, Status: text("COMPLETE")}
}
func resolved(t *testing.T, hot Record, rows []AnchorRecord) Selection {
	t.Helper()
	values, err := ResolveOrigins([]Record{hot}, rows)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := hot.Scope()
	if err != nil {
		t.Fatal(err)
	}
	return values[scope]
}

func TestHotPhysicalColumnsRequirePresenceAndRetainDeclaredNulls(t *testing.T) {
	data, err := json.Marshal(fixture())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 20 {
		t.Fatalf("Physical HOT schema changed: %d", len(fields))
	}
	original, err := DecodeRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	if original.ExpiresAt != nil || original.Priority != nil || original.SincePrec != nil || original.SinceReason == nil || *original.SinceReason != "LEGACY_PRECISION_UNREPORTED" {
		t.Fatal("Nullable historical metadata was inferred or discarded")
	}
	for name, value := range fields {
		t.Run(name, func(t *testing.T) {
			delete(fields, name)
			missing, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			fields[name] = value
			if _, err := DecodeRecord(missing); err == nil {
				t.Fatalf("Missing physical key accepted: %s", name)
			}
		})
	}
	for _, name := range []string{"storage_key", "event_id", "token_id"} {
		value := fields[name]
		fields[name] = json.RawMessage("null")
		invalid, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		fields[name] = value
		if _, err := DecodeRecord(invalid); err == nil {
			t.Fatalf("Nonnullable physical key accepted NULL: %s", name)
		}
	}
}

func TestSuccessfulAbsenceUsesRecordedSinceWithoutVerifiedAnchorClaim(t *testing.T) {
	hot := fixture()
	selection := resolved(t, hot, nil)
	origin, ok := selection.Origin()
	if !ok || origin.Kind != RecordedHotSince || !origin.Clock.Value.Equal(*hot.Since) ||
		origin.Clock.Precision != nil || origin.Clock.Reason == nil || *origin.Clock.Reason != "LEGACY_PRECISION_UNREPORTED" ||
		origin.BackfillID != nil || origin.StartedClock != nil {
		t.Fatalf("Recorded since was relabeled or its uncertainty inferred: %+v", origin)
	}
}

func TestLatestRecordedBackfillStartPreservesAnchorClockEvidence(t *testing.T) {
	hot := fixture()
	earlier := hot.Since.Add(time.Hour)
	later := earlier.Add(time.Hour)
	first := anchor(hot, "first", hot.Since.Add(-time.Hour), &earlier)
	latest := anchor(hot, "latest", hot.Since.Add(time.Minute), &later)
	latest.StartedAtReason = text("RETAINED_START_PRECISION_UNREPORTED")
	for _, rows := range [][]AnchorRecord{{first, latest}, {latest, first}} {
		origin, ok := resolved(t, hot, rows).Origin()
		if !ok || origin.Kind != RecordedBackfillAnchor || !origin.Clock.Value.Equal(*latest.Anchor) ||
			origin.Clock.Precision == nil || *origin.Clock.Precision != "BUCKET" || origin.BackfillID == nil || *origin.BackfillID != "latest" ||
			origin.StartedClock == nil || origin.StartedClock.Precision != nil || origin.StartedClock.Reason == nil ||
			*origin.StartedClock.Reason != "RETAINED_START_PRECISION_UNREPORTED" {
			t.Fatalf("Recorded anchor ordering or uncertainty changed: %+v", origin)
		}
	}
}

func TestMalformedOrUnavailableMatchingEvidenceCannotBecomeAbsence(t *testing.T) {
	hot := fixture()
	bad := anchor(hot, "malformed", *hot.Since, nil)
	bad.Anchor = nil
	bad.Status = text("RUNNING")
	failed := bad
	failed.BackfillID = "failed-input"
	failed.Status = text("FAILED")
	unknown := anchor(hot, "unsupported", *hot.Since, nil)
	unknown.Status = text("UNKNOWN")
	for _, candidate := range []struct {
		record AnchorRecord
		reason RefusalReason
	}{
		{bad, BackfillMalformed}, {failed, BackfillUnavailable}, {unknown, BackfillMalformed},
	} {
		selection := resolved(t, hot, []AnchorRecord{candidate.record})
		refusal, ok := selection.Refusal()
		if _, usable := selection.Origin(); usable || !ok || refusal.Reason != candidate.reason {
			t.Fatalf("Matching unavailable evidence selected since: %+v", selection)
		}
	}
	valid := anchor(hot, "observed", *hot.Since, nil)
	selected := resolved(t, hot, []AnchorRecord{failed, valid})
	origin, ok := selected.Origin()
	ids := selected.UnavailableBackfillIDs()
	if !ok || origin.BackfillID == nil || *origin.BackfillID != "observed" || len(ids) != 1 || ids[0] != "failed-input" {
		t.Fatalf("Actual declared anchor or rejected-input evidence lost: %+v", selected)
	}
}

func TestConflictingUnorderedAnchorsRefuseInsteadOfGuessing(t *testing.T) {
	hot := fixture()
	rows := []AnchorRecord{anchor(hot, "one", *hot.Since, nil), anchor(hot, "two", hot.Since.Add(time.Hour), nil)}
	selection := resolved(t, hot, rows)
	refused, ok := selection.Refusal()
	if _, usable := selection.Origin(); usable || !ok || refused.Reason != BackfillAmbiguous {
		t.Fatal("Unordered conflicting anchors were selected arbitrarily")
	}
	later := hot.Since.Add(time.Minute)
	rows = append(rows, anchor(hot, "started", *hot.Since, &later))
	selection = resolved(t, hot, rows)
	refused, ok = selection.Refusal()
	if _, usable := selection.Origin(); usable || !ok || refused.Reason != BackfillAmbiguous {
		t.Fatal("An observed start incorrectly ordered conflicting unobserved starts")
	}
}

func TestOpaqueEventAndTokenIdentitiesDoNotCollide(t *testing.T) {
	one, two := fixture(), fixture()
	one.TokenID, one.EventID = "a|b", "c"
	two.TokenID, two.EventID = "a", "b|c"
	other := one.Since.Add(time.Hour)
	two.Since = &other
	selections, err := ResolveOrigins([]Record{one, two}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, firstOK := selections[Scope{TokenID: "a|b", EventID: "c"}].Origin()
	second, secondOK := selections[Scope{TokenID: "a", EventID: "b|c"}].Origin()
	if len(selections) != 2 || !firstOK || !secondOK || !first.Clock.Value.Equal(*one.Since) || !second.Clock.Value.Equal(other) {
		t.Fatal("Opaque identity fields were joined into a colliding string key")
	}
}

func TestAnchorProjectionRequiresAllKeysAndPreservesUnobservedStart(t *testing.T) {
	hot := fixture()
	original := anchor(hot, "legacy-running", *hot.Since, nil)
	original.Status = text("RUNNING")
	original.StartedAtReason = text("PENDING")
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 11 {
		t.Fatalf("Minimal anchor projection changed: %d", len(fields))
	}
	decoded, err := DecodeAnchorRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	selection := resolved(t, hot, []AnchorRecord{decoded})
	origin, ok := selection.Origin()
	if !ok || origin.StartedClock == nil || origin.StartedClock.Value != nil || origin.StartedClock.Precision != nil ||
		origin.StartedClock.Reason == nil || *origin.StartedClock.Reason != "PENDING" {
		t.Fatalf("Retained missing worker-start provenance was inferred or lost: %+v", origin)
	}
	for name, value := range fields {
		t.Run(name, func(t *testing.T) {
			delete(fields, name)
			missing, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			fields[name] = value
			if _, err := DecodeAnchorRecord(missing); err == nil {
				t.Fatalf("Missing anchor projection key accepted: %s", name)
			}
		})
	}
	for _, name := range []string{"storage_key", "backfill_id"} {
		value := fields[name]
		fields[name] = json.RawMessage("null")
		invalid, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		fields[name] = value
		if _, err := DecodeAnchorRecord(invalid); err == nil {
			t.Fatalf("Nonnullable anchor identity accepted NULL: %s", name)
		}
	}
}

func TestMixedObservedAndUnobservedStartsRequireEquivalentOrigins(t *testing.T) {
	hot := fixture()
	started := hot.Since.Add(time.Minute)
	observed := anchor(hot, "observed", *hot.Since, &started)
	unknown := anchor(hot, "unobserved", hot.Since.Add(time.Hour), nil)
	unknown.StartedAtReason = text("PENDING")
	for _, rows := range [][]AnchorRecord{{observed, unknown}, {unknown, observed}} {
		selection := resolved(t, hot, rows)
		refusal, ok := selection.Refusal()
		if _, usable := selection.Origin(); usable || !ok || refusal.Reason != BackfillAmbiguous {
			t.Fatal("Complete backfill with unobserved start was assumed older")
		}
	}
	unknown.Anchor = observed.Anchor
	for _, rows := range [][]AnchorRecord{{observed, unknown}, {unknown, observed}} {
		origin, ok := resolved(t, hot, rows).Origin()
		if !ok || origin.BackfillID == nil || *origin.BackfillID != "unobserved" || origin.StartedClock == nil ||
			origin.StartedClock.Value != nil || origin.StartedClock.Precision != nil ||
			origin.StartedClock.Reason == nil || *origin.StartedClock.Reason != "PENDING" {
			t.Fatalf("Equivalent origin lost its recorded missing-start evidence: %+v", origin)
		}
	}
}
