package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
)

func TestBackfillProjectionPreservesActualSQLNullJSONNullAndNumbers(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	prefix := "backfill-physical-" + suffix()
	cleanup(t, s, "backfill_id", prefix+"%", backfillRuns)
	at := time.Date(2026, 10, 26, 1, 2, 3, 123456000, time.UTC)
	jsonNull := "null"
	large := `{"integer":9007199254740993,"fraction":0.123456789012345678901,"label":"000.100"}`
	inputs := []struct {
		name   string
		result *string
	}{{"sql-null", nil}, {"json-null", &jsonNull}, {"exact", &large}}
	for _, input := range inputs {
		_, err := s.Pool.Exec(ctx, "INSERT INTO "+liveTable(backfillRuns)+
			` (storage_key,backfill_id,event_id,token_id,anchor,anchor_prec_reason,status,
 started_at_prec_reason,finished_at_prec_reason,result,code_version,config_version)
 VALUES($1,$1,$2,$3,$4,'RETAINED_ORIGIN_UNCAPTURED','PENDING','PENDING','PENDING',$5::jsonb,'retained-code','retained-config')`,
			prefix+input.name, prefix+"event", prefix+"token", at, input.result)
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.backfillRows(ctx, []string{"PENDING", "RUNNING"})
	if err != nil {
		t.Fatal(err)
	}
	owned := make(map[string]backfillSnapshot)
	for _, row := range rows {
		if strings.HasPrefix(row.record.BackfillID, prefix) {
			owned[row.record.BackfillID] = row
		}
	}
	if len(owned) != 3 {
		t.Fatalf("actual Store lost physical rows: %d", len(owned))
	}
	if owned[prefix+"sql-null"].record.ResultJSON != nil {
		t.Fatal("SQL NULL acquired a fabricated JSON value")
	}
	observedJSONNull := owned[prefix+"json-null"].record.ResultJSON
	if observedJSONNull == nil || *observedJSONNull != "null" {
		t.Fatal("JSON null lost its non-NULL physical text")
	}
	for _, row := range owned {
		if row.record.Anchor == nil || row.record.AnchorPrec != nil || row.record.AnchorPrecReason == nil || *row.record.AnchorPrecReason != "RETAINED_ORIGIN_UNCAPTURED" {
			t.Fatal("recorded anchor provenance was upgraded")
		}
		parsed, err := time.Parse(time.RFC3339Nano, string(*row.record.Anchor))
		if err != nil || !parsed.Equal(at) {
			t.Fatalf("actual stored anchor changed: %v", err)
		}
		if row.record.StartedAt != nil || row.record.RecordedAt != nil || row.record.ReceivedTS != nil || row.record.StartedAtPrecReason == nil || *row.record.StartedAtPrecReason != "PENDING" {
			t.Fatal("nullable physical metadata acquired processing facts")
		}
		var transaction string
		if err := s.Pool.QueryRow(ctx, "SELECT xmin::text FROM "+liveTable(backfillRuns)+" WHERE storage_key=$1", row.record.StorageKey).Scan(&transaction); err != nil {
			t.Fatal(err)
		}
		if row.transaction == "" || row.transaction != transaction {
			t.Fatal("observed revision differs from actual PostgreSQL tuple")
		}
	}
	exact := owned[prefix+"exact"].record
	if exact.ResultJSON == nil {
		t.Fatal("actual JSON evidence is unavailable")
	}
	var values struct {
		Integer  json.Number `json:"integer"`
		Fraction json.Number `json:"fraction"`
		Label    string      `json:"label"`
	}
	if err := json.Unmarshal([]byte(*exact.ResultJSON), &values); err != nil {
		t.Fatal(err)
	}
	if values.Integer != "9007199254740993" || values.Fraction != "0.123456789012345678901" || values.Label != "000.100" {
		t.Fatal("actual numeric/text evidence passed through a lossy conversion")
	}
}

func TestBackfillObservedRevisionChangesOnlyAfterActualStoreTupleMutation(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "backfill-revision-" + suffix()
	cleanup(t, s, "backfill_id", id+"%", backfillRuns)
	if _, err := s.Pool.Exec(ctx, "INSERT INTO "+liveTable(backfillRuns)+" (storage_key,backfill_id,status) VALUES($1,$1,'PENDING')", id); err != nil {
		t.Fatal(err)
	}
	read := func() backfillSnapshot {
		t.Helper()
		row, err := decodeBackfillRow(s.Pool.QueryRow(ctx, "SELECT "+backfillProjection+",backfill_row.xmin::text FROM "+liveTable(backfillRuns)+" backfill_row WHERE storage_key=$1", id))
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	before := read()
	if before.record.Anchor != nil || before.record.TokenID != nil || before.record.EventID != nil {
		t.Fatal("malformed consumed fields were silently repaired by physical read")
	}
	if _, err := s.Pool.Exec(ctx, "UPDATE "+liveTable(backfillRuns)+" SET reason='observed mutation' WHERE storage_key=$1", id); err != nil {
		t.Fatal(err)
	}
	after := read()
	if after.transaction == before.transaction || after.record.Reason == nil || *after.record.Reason != "observed mutation" || before.record.Reason != nil {
		t.Fatal("actual revision or prior immutable observation changed incorrectly")
	}
	if _, err := backfillstate.DecodeRecord([]byte(`{"storage_key":"` + id + `"}`)); err == nil {
		t.Fatal("sparse input was promoted to a complete SQL envelope")
	}
}
