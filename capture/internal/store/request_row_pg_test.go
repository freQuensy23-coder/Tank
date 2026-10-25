package store

import (
	"context"
	"encoding/json"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

func TestPhysicalRequestProjectionPreservesSQLNumericAndJSONNullEvidence(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "typed-physical-" + suffix()
	cleanup(t, s, "request_id", id, requests)
	const payload = `{"exact":0.123456789012345678901,"integer":123456789012345678901234567890,"text":"0.123456789012345678901"}`
	_, err := s.Pool.Exec(ctx, "INSERT INTO "+liveTable(requests)+
		" (storage_key,request_id,budget_s,request,result) VALUES($1,$1,'NaN'::numeric,$2::jsonb,'null'::jsonb)", id, payload)
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodePhysicalRequest(s.Pool.QueryRow(ctx,
		"SELECT "+requestProjection+" FROM "+liveTable(requests)+" request_row WHERE request_id=$1", id))
	if err != nil {
		t.Fatal(err)
	}
	if record.StorageKey != id || record.RequestID != id || record.BudgetS == nil || *record.BudgetS != "NaN" {
		t.Fatalf("Physical identity or rejected numeric evidence changed: %+v", record)
	}
	if record.ResultJSON == nil || *record.ResultJSON != "null" || record.Deadline != nil {
		t.Fatal("SQL NULL and JSONB null collapsed")
	}
	if record.RequestJSON == nil {
		t.Fatal("Opaque HTTP payload disappeared")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(*record.RequestJSON), &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["exact"]) != "0.123456789012345678901" || string(fields["integer"]) != "123456789012345678901234567890" ||
		string(fields["text"]) != `"0.123456789012345678901"` {
		t.Fatalf("Physical JSON numeric/text values changed: %s", *record.RequestJSON)
	}
	document, err := record.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := schema.FingerprintDocument(document, requests); err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool.Exec(ctx, "UPDATE "+liveTable(requests)+" SET result=NULL,budget_s='Infinity'::numeric WHERE request_id=$1", id)
	if err != nil {
		t.Fatal(err)
	}
	record, err = decodePhysicalRequest(s.Pool.QueryRow(ctx,
		"SELECT "+requestProjection+" FROM "+liveTable(requests)+" request_row WHERE request_id=$1", id))
	if err != nil {
		t.Fatal(err)
	}
	if record.ResultJSON != nil || record.BudgetS == nil || *record.BudgetS != "Infinity" {
		t.Fatal("SQL NULL or positive infinity evidence changed")
	}
}

func TestRequestLifecycleReplacesRoundedFingerprintWithoutRewritingEvidence(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "exact-transition-" + suffix()
	cleanup(t, s, "request_id", id, requests)
	now := time.Now().UTC().Truncate(time.Microsecond)
	payload := `{"value":0.123456789012345678901,"integer":123456789012345678901234567890}`
	prior := `{"retained":0.987654321098765432109}`
	_, err := s.Pool.Exec(ctx, "INSERT INTO "+liveTable(requests)+
		" (storage_key,request_id,kind,status,budget_s,requested_at,deadline,request,result,code_version,config_version) VALUES($1,$1,'UNKNOWN','PENDING','NaN'::numeric,$2,$3,$4::jsonb,$5::jsonb,'retained-code','retained-config')",
		id, now, now.Add(time.Minute), payload, prior)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := s.Get(ctx, requests, id)
	if err != nil {
		t.Fatal(err)
	}
	oldHash := schema.Fingerprint(legacy, requests)
	if _, err := s.Pool.Exec(ctx, "UPDATE "+liveTable(requests)+" SET semantic_fingerprint=$2 WHERE request_id=$1", id, oldHash); err != nil {
		t.Fatal(err)
	}
	physical := func() requeststate.Record {
		t.Helper()
		row, err := decodePhysicalRequest(s.Pool.QueryRow(ctx, "SELECT "+requestProjection+" FROM "+liveTable(requests)+" request_row WHERE request_id=$1", id))
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	before := physical()
	beforeDocument, err := before.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	exactBeforeHash, err := schema.FingerprintDocument(beforeDocument, requests)
	if err != nil {
		t.Fatal(err)
	}
	if exactBeforeHash == oldHash {
		t.Fatal("High precision fixture did not establish the old rounded fingerprint transition")
	}
	claimed, err := s.ClaimRequest(ctx, "exact-test", now)
	if err != nil || claimed == nil || claimed.ID != id {
		t.Fatalf("Legacy fingerprint blocked claim: %+v %v", claimed, err)
	}
	running := physical()
	if *running.RequestJSON != *before.RequestJSON || *running.ResultJSON != *before.ResultJSON || *running.BudgetS != "NaN" ||
		*running.CodeVersion != "retained-code" || *running.ConfigVersion != "retained-config" {
		t.Fatalf("Claim rewrote rejected or opaque evidence: %+v", running)
	}
	assertFingerprint := func(row requeststate.Record) {
		t.Helper()
		document, err := row.SemanticDocument()
		if err != nil {
			t.Fatal(err)
		}
		expected, err := schema.FingerprintDocument(document, requests)
		if err != nil {
			t.Fatal(err)
		}
		if row.SemanticFingerprint == nil || *row.SemanticFingerprint != expected {
			t.Fatal("Stored fingerprint does not bind exact persisted evidence")
		}
	}
	assertFingerprint(running)
	completion := requeststate.Completion{ID: id, Outcome: requeststate.Failure(requeststate.BudgetInvalid, nil), FinishedAt: now.Add(time.Second)}
	if err := s.FinishRequest(ctx, completion); err != nil {
		t.Fatal(err)
	}
	terminal := physical()
	if *terminal.RequestJSON != *before.RequestJSON || *terminal.BudgetS != "NaN" || terminal.ResultJSON != nil || *terminal.Status != "FAILED" {
		t.Fatalf("Finish rewrote original request or lost explicit failure: %+v", terminal)
	}
	assertFingerprint(terminal)
	if err := s.FinishRequest(ctx, completion); err != nil {
		t.Fatal(err)
	}
	unchanged := physical()
	if *unchanged.SemanticFingerprint != *terminal.SemanticFingerprint {
		t.Fatal("Identical completion oscillated fingerprint conventions")
	}
	again, err := s.ClaimRequest(ctx, "exact-test", now.Add(2*time.Second))
	if err != nil || again != nil {
		t.Fatalf("Terminal poison request retried: %+v %v", again, err)
	}
}
