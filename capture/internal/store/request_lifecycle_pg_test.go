package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"os"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

type lifecycleSnapshot struct {
	Stage  string         `json:"stage"`
	Record map[string]any `json:"record"`
}

func TestSparseRequestLifecyclePreservesInvalidNumericEvidence(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "sparse-request-" + suffix()
	cleanup(t, s, "request_id", id+"%", requests)
	now := time.Now().UTC().Truncate(time.Microsecond)
	body := map[string]any{
		"method": "GET", "url": "https://api.binance.com/data", "source_id": nil,
		"body": map[string]any{"execution_id": "opaque-user-input", "source_position": map[string]any{"revision": "opaque"}},
	}
	request := func(number string, deadline time.Time) map[string]any {
		row := map[string]any{
			"request_id": id + number, "kind": "HTTP_FETCH", "status": "PENDING",
			"budget_s": "20", "request": body,
			"config_version": "retained-config", "code_version": "retained-code",
		}
		pytime.CollectionClock(now).Into(row, "requested_at")
		pytime.CollectionClock(deadline).Into(row, "deadline")
		pytime.Timestamp{Reason: "PENDING"}.Into(row, "started_at")
		pytime.Timestamp{Reason: "PENDING"}.Into(row, "finished_at")
		return row
	}
	firstKey, expiredKey := "storage-"+id+"live", "storage-"+id+"expired"
	if _, err := s.PutMarketBatch(ctx, requests, []map[string]any{
		request("-live", now.Add(time.Minute)), request("-expired", now.Add(-time.Minute)),
	}, []string{firstKey, expiredKey}); err != nil {
		t.Fatal(err)
	}
	queued, err := s.Get(ctx, requests, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []lifecycleSnapshot{{Stage: "queued", Record: queued}}
	if _, err := s.Pool.Exec(ctx, "UPDATE "+liveTable(requests)+" SET budget_s='NaN'::numeric WHERE request_id LIKE $1", id+"%"); err != nil {
		t.Fatal(err)
	}
	poisoned, err := s.Get(ctx, requests, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	snapshots = append(snapshots, lifecycleSnapshot{Stage: "poisoned_pending", Record: poisoned})
	claimed, err := s.ClaimRequest(ctx, "sparse-test-writer", now)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != id+"-live" || claimed.BudgetS == nil || *claimed.BudgetS != "NaN" {
		t.Fatalf("invalid persisted numeric evidence could not enter explicit lifecycle handling: %v", claimed)
	}
	if !claimed.StartedAt.Equal(now) {
		t.Fatalf("claim did not persist the actual collection start clock: %v", claimed)
	}
	running, err := s.Get(ctx, requests, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	snapshots = append(snapshots, lifecycleSnapshot{Stage: "claimed_running", Record: running})
	if string(claimed.Request) == "" || pyjson.Canonical(running["request"]) != pyjson.Canonical(body) {
		t.Fatalf("claim rewrote opaque request evidence: %v", claimed)
	}
	expired, err := s.Get(ctx, requests, expiredKey)
	if err != nil {
		t.Fatal(err)
	}
	if expired["status"] != "EXPIRED" || expired["budget_s"] != "NaN" || expired["reason"] != "REQUEST_DEADLINE_PASSED" {
		t.Fatalf("expiry could not retain invalid numeric evidence: %v", expired)
	}
	snapshots = append(snapshots, lifecycleSnapshot{Stage: "expired", Record: expired})
	if err := s.FinishRequest(ctx, requeststate.Completion{ID: claimed.ID, Outcome: requeststate.Failure(requeststate.BudgetInvalid, nil), FinishedAt: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Get(ctx, requests, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	if stored["status"] != "FAILED" || stored["reason"] != "CAPTURE_REQUEST_BUDGET_INVALID" || stored["budget_s"] != "NaN" {
		t.Fatalf("invalid input did not become explicit terminal evidence: %v", stored)
	}
	if pyjson.Canonical(stored["request"]) != pyjson.Canonical(body) || stored["config_version"] != "retained-config" || stored["code_version"] != "retained-code" {
		t.Fatalf("terminal update changed original request provenance: %v", stored)
	}
	if stored["writer"] != "sparse-test-writer" || stored["started_at"] != pytime.ISO(claimed.StartedAt) ||
		stored["finished_at"] != pytime.ISO(now.Add(time.Second)) || stored["finished_at_prec"] != pytime.CollectionOnly ||
		stored["finished_at_prec_reason"] != nil || stored["result"] != nil {
		t.Fatalf("terminal update lost claim identity or exact clock/null bindings: %v", stored)
	}
	snapshots = append(snapshots, lifecycleSnapshot{Stage: "failed", Record: stored})
	if stored["semantic_fingerprint"] != schema.Fingerprint(stored, requests) {
		t.Fatalf("sparse update fingerprint does not describe persisted raw evidence: %v", stored)
	}
	next, err := s.ClaimRequest(ctx, "sparse-test-writer", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatalf("terminal invalid request was retried: %v", next)
	}
	completedKey := "storage-" + id + "complete"
	if _, err := s.PutMarketBatch(ctx, requests, []map[string]any{request("-complete", now.Add(time.Minute))}, []string{completedKey}); err != nil {
		t.Fatal(err)
	}
	completing, err := s.ClaimRequest(ctx, "complete-test-writer", now)
	if err != nil {
		t.Fatal(err)
	}
	bodyText := `{"quoted":"a'b","nullable":null}`
	digest := sha256.Sum256([]byte(bodyText))
	result := requeststate.FetchResult{StatusCode: 200, ContentType: "application/json", Body: &bodyText, Bytes: int64(len(bodyText)), SHA256: hex.EncodeToString(digest[:]), FetchedAt: pytime.ISO(now)}
	outcome := requeststate.Completed(result)
	if err := s.FinishRequest(ctx, requeststate.Completion{ID: completing.ID, Outcome: outcome, FinishedAt: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := outcome.JSONResult()
	if err != nil {
		t.Fatal(err)
	}
	var expectedResult map[string]json.RawMessage
	if err := json.Unmarshal(expectedJSON, &expectedResult); err != nil {
		t.Fatal(err)
	}
	completed, err := s.Get(ctx, requests, completedKey)
	if err != nil {
		t.Fatal(err)
	}
	expectedCanonical, err := json.Marshal(expectedResult)
	if err != nil {
		t.Fatal(err)
	}
	actualCanonical, err := json.Marshal(completed["result"])
	if err != nil {
		t.Fatal(err)
	}
	if completed["status"] != "COMPLETE" || completed["reason"] != nil || string(actualCanonical) != string(expectedCanonical) ||
		completed["writer"] != "complete-test-writer" || completed["semantic_fingerprint"] != schema.Fingerprint(completed, requests) {
		t.Fatalf("successful result JSON/null reason lost typed SQL binding parity: %v", completed)
	}
	if err := s.FinishRequest(ctx, requeststate.Completion{ID: id + "-missing", Outcome: requeststate.Failure(requeststate.BudgetInvalid, nil), FinishedAt: now}); err == nil {
		t.Fatal("finishing an unqueued identity must not create a guessed storage key")
	}
	if path := os.Getenv("TANK_CAPTURE_LIFECYCLE_EVIDENCE"); path != "" {
		data, err := json.MarshalIndent(snapshots, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}

}
