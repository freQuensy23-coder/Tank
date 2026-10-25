package store

import (
	"context"
	"encoding/json"
	"testing"

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
