package capture

import (
	"context"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func TestInvalidRepairRequestRemainsFailedEvidence(t *testing.T) {
	for _, change := range []struct {
		field  string
		value  any
		reason string
	}{
		{"deadline", nil, "CAPTURE_REQUEST_DEADLINE_INVALID"},
		{"deadline", "not-a-timestamp", "CAPTURE_REQUEST_DEADLINE_INVALID"},
		{"budget_s", "not-a-number", "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"budget_s", "NaN", "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"budget_s", "Infinity", "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"budget_s", 0, "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"budget_s", -1, "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"budget_s", true, "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"budget_s", "0.0000000001", "CAPTURE_REQUEST_BUDGET_INVALID"},
		{"token_id", nil, "CAPTURE_REQUEST_TOKEN_INVALID"},
		{"token_id", "", "CAPTURE_REQUEST_TOKEN_INVALID"},
	} {
		t.Run(change.field+change.reason, func(t *testing.T) {
			r := newRESTRig(t, "coinbase")
			row := map[string]any{
				"request_id": "CR-invalid", "kind": "REPAIR_WINDOWS", "token_id": r.inst.TokenID,
				"start": pytime.ISO(r.base), "end": pytime.ISO(r.base.Add(time.Minute)),
				"deadline": pytime.ISO(r.clock.Now().Add(time.Minute)), "status": "RUNNING",
			}
			row[change.field] = change.value
			r.mem.requests = []map[string]any{row}
			r.s.runRequest(context.Background(), row)
			stored := r.mem.requests[0]
			if stored["status"] != "FAILED" || stored["reason"] != change.reason || stored["result"] != nil || stored["finished_at"] == nil {
				t.Fatalf("invalid request was not retained as explicit failed evidence: %v", stored)
			}
			if stored[change.field] != change.value {
				t.Fatalf("invalid evidence was silently rewritten: %v", stored)
			}
		})
	}
}

func TestDeclaredDeadlineBoundsLegacyRequestWithoutBudget(t *testing.T) {
	now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	row := map[string]any{"deadline": pytime.ISO(now.Add(7 * time.Second))}
	limits, invalid := decodeRequestLimits(row, now)
	if invalid != nil || limits.budget != 7*time.Second {
		t.Fatalf("explicit deadline did not bound legacy request: limits=%+v error=%v", limits, invalid)
	}
	row["budget_s"] = "3.5"
	limits, invalid = decodeRequestLimits(row, now)
	if invalid != nil || limits.budget != 3500*time.Millisecond {
		t.Fatalf("declared numeric-column budget did not narrow deadline: limits=%+v error=%v", limits, invalid)
	}
	row["budget_s"] = "1e100"
	limits, invalid = decodeRequestLimits(row, now)
	if invalid != nil || limits.budget != 7*time.Second {
		t.Fatalf("large finite budget overflowed the earlier explicit deadline: limits=%+v error=%v", limits, invalid)
	}
}
