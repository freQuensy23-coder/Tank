package store

import (
	"context"
	"encoding/json"
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
	if claimed == nil || claimed["request_id"] != id+"-live" || claimed["status"] != "RUNNING" || claimed["budget_s"] != "NaN" {
		t.Fatalf("invalid persisted numeric evidence could not enter explicit lifecycle handling: %v", claimed)
	}
	if claimed["started_at"] != pytime.ISO(now) || claimed["started_at_prec"] != pytime.CollectionOnly || claimed["started_at_prec_reason"] != nil {
		t.Fatalf("claim did not persist the actual collection start clock: %v", claimed)
	}
	snapshots = append(snapshots, lifecycleSnapshot{Stage: "claimed_running", Record: claimed})
	if pyjson.Canonical(claimed["request"]) != pyjson.Canonical(body) {
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
	claimed = copyRequestRecord(claimed)
	claimed["status"], claimed["reason"], claimed["result"] = "FAILED", "CAPTURE_REQUEST_BUDGET_INVALID", nil
	pytime.CollectionClock(now.Add(time.Second)).Into(claimed, "finished_at")
	if err := s.FinishRequest(ctx, claimed); err != nil {
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
	if stored["writer"] != "sparse-test-writer" || stored["started_at"] != claimed["started_at"] ||
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
	result := map[string]any{"status_code": int64(200), "body": map[string]any{"quoted": "a'b", "nullable": nil}, "bytes": int64(17)}
	completing["status"], completing["reason"], completing["result"] = "COMPLETE", nil, result
	pytime.CollectionClock(now.Add(time.Second)).Into(completing, "finished_at")
	if err := s.FinishRequest(ctx, completing); err != nil {
		t.Fatal(err)
	}
	completed, err := s.Get(ctx, requests, completedKey)
	if err != nil {
		t.Fatal(err)
	}
	if completed["status"] != "COMPLETE" || completed["reason"] != nil || pyjson.Canonical(completed["result"]) != pyjson.Canonical(result) ||
		completed["writer"] != "complete-test-writer" || completed["semantic_fingerprint"] != schema.Fingerprint(completed, requests) {
		t.Fatalf("successful result JSON/null reason lost typed SQL binding parity: %v", completed)
	}
	missing := map[string]any{"request_id": id + "-missing", "status": "FAILED", "reason": "not queued"}
	pytime.CollectionClock(now).Into(missing, "finished_at")
	if err := s.FinishRequest(ctx, missing); err == nil {
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

func copyRequestRecord(row map[string]any) map[string]any {
	result := make(map[string]any, len(row))
	for key, value := range row {
		result[key] = value
	}
	return result
}
