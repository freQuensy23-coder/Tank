package capture

import (
	"context"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

func TestInvalidRepairRequestRemainsFailedEvidence(t *testing.T) {
	changes := []struct {
		name   string
		change func(*requeststate.Record)
		reason requeststate.Reason
	}{
		{"missing-deadline", func(row *requeststate.Record) { row.Deadline = nil }, requeststate.DeadlineInvalid},
		{"invalid-budget-text", func(row *requeststate.Record) { row.BudgetS = requestText("not-a-number") }, requeststate.BudgetInvalid},
		{"nan", func(row *requeststate.Record) { row.BudgetS = requestText("NaN") }, requeststate.BudgetInvalid},
		{"infinity", func(row *requeststate.Record) { row.BudgetS = requestText("Infinity") }, requeststate.BudgetInvalid},
		{"zero", func(row *requeststate.Record) { row.BudgetS = requestText("0") }, requeststate.BudgetInvalid},
		{"negative", func(row *requeststate.Record) { row.BudgetS = requestText("-1") }, requeststate.BudgetInvalid},
		{"below-timer-resolution", func(row *requeststate.Record) { row.BudgetS = requestText("0.0000000001") }, requeststate.BudgetInvalid},
		{"missing-token", func(row *requeststate.Record) { row.TokenID = nil }, requeststate.TokenInvalid},
		{"empty-token", func(row *requeststate.Record) { row.TokenID = requestText("") }, requeststate.TokenInvalid},
		{"unsupported-kind", func(row *requeststate.Record) { row.Kind = requestText("UNKNOWN") }, requeststate.Unsupported},
		{"invalid-interval", func(row *requeststate.Record) { row.End = row.Start }, requeststate.Unsupported},
	}
	for _, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			r := newRESTRig(t, "coinbase")
			row := pendingRequest("CR-invalid", r.inst.TokenID, r.base, r.base.Add(time.Minute), r.clock.Now().Add(time.Minute))
			change.change(&row)
			r.mem.requests = []requeststate.Record{row}
			claimed, err := r.mem.ClaimRequest(context.Background(), "invalid-test", r.clock.Now())
			if err != nil || claimed == nil {
				t.Fatalf("malformed facts blocked physical claim: %v", err)
			}
			r.s.runRequest(context.Background(), *claimed)
			stored := r.mem.requests[0]
			if stored.Status == nil || *stored.Status != "FAILED" || stored.Reason == nil || *stored.Reason != string(change.reason) || stored.ResultJSON != nil || stored.FinishedAt == nil {
				t.Fatalf("invalid request did not become terminal evidence: %+v", stored)
			}
			if stored.BudgetS != row.BudgetS || stored.Deadline != row.Deadline || stored.Kind != row.Kind || stored.TokenID != row.TokenID || stored.Start != row.Start || stored.End != row.End {
				t.Fatalf("consumed rejection rewrote original evidence: %+v", stored)
			}
		})
	}
}

func TestDeclaredDeadlineBoundsLegacyRequestWithoutBudget(t *testing.T) {
	now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	deadline := now.Add(7 * time.Second)
	for _, candidate := range []struct {
		budget   *string
		duration time.Duration
	}{
		{nil, 7 * time.Second}, {requestText("3.5"), 3500 * time.Millisecond}, {requestText("1e100"), 7 * time.Second},
	} {
		limits, invalid := requeststate.DecodeLimits(&deadline, candidate.budget, now)
		if invalid != nil || limits.Budget != candidate.duration {
			t.Fatalf("declared deadline/budget lost bounds: %+v %v", limits, invalid)
		}
	}
}

func TestMalformedRequestCannotBlockFollowingCommandOrOverrideExpiry(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	now := r.clock.Now()
	r.s.opt.Writer = "queue-progress-test"
	fetcher := &recordedFetch{response: &rest.Fetched{Status: 200, ContentType: "text/plain", Body: []byte("actual response")}}
	r.s.opt.Fetcher = fetcher
	unsupported := pendingRequest("unsupported", r.inst.TokenID, r.base, r.base.Add(time.Minute), now.Add(time.Minute))
	unsupported.Kind = requestText("UNKNOWN")
	valid := pendingRequest("valid", r.inst.TokenID, r.base, r.base.Add(time.Minute), now.Add(time.Minute))
	valid.Kind = requestText("HTTP_FETCH")
	valid.RequestJSON = requestText(`{"method":"GET","url":"https://api.binance.com/data","source_id":null}`)
	expired := pendingRequest("expired", r.inst.TokenID, r.base, r.base.Add(time.Minute), now.Add(-time.Second))
	expired.Kind = requestText("UNKNOWN")
	expired.BudgetS = requestText("NaN")
	expired.RequestJSON = requestText("true")
	r.mem.requests = []requeststate.Record{unsupported, valid, expired}
	r.s.requests(context.Background())
	r.s.wg.Wait()
	failed, completed, deadline := r.mem.requests[0], r.mem.requests[1], r.mem.requests[2]
	if failed.Status == nil || *failed.Status != "FAILED" || failed.Reason == nil || *failed.Reason != "UNSUPPORTED_CAPTURE_REQUEST" {
		t.Fatalf("Unsupported command did not become terminal refusal: %+v", failed)
	}
	if completed.Status == nil || *completed.Status != "COMPLETE" || completed.ResultJSON == nil || fetcher.calls != 1 {
		t.Fatalf("Malformed predecessor blocked valid command or caused extra effects: %+v calls=%d", completed, fetcher.calls)
	}
	if deadline.Status == nil || *deadline.Status != "EXPIRED" || deadline.Reason == nil || *deadline.Reason != "REQUEST_DEADLINE_PASSED" ||
		deadline.BudgetS == nil || *deadline.BudgetS != "NaN" || deadline.RequestJSON == nil || *deadline.RequestJSON != "true" {
		t.Fatalf("Expired malformed facts were executed or sanitized: %+v", deadline)
	}
	r.s.requests(context.Background())
	r.s.wg.Wait()
	if fetcher.calls != 1 {
		t.Fatal("Terminal queue entries were executed again")
	}
}
