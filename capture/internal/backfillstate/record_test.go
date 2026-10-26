package backfillstate

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

func text(value string) *string { return &value }

func physicalDocument(t *testing.T, row Record) []byte {
	t.Helper()
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func physicalObject(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(physicalDocument(t, Record{StorageKey: "BF-record", BackfillID: "BF-record"}), &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestPhysicalProjectionMatchesEveryActualSchemaColumn(t *testing.T) {
	fields := physicalObject(t)
	if len(fields) != len(schema.Columns["mkt.backfill_runs"])+1 || len(fields) != 26 {
		t.Fatalf("physical column count differs: %d", len(fields))
	}
	for _, column := range schema.Columns["mkt.backfill_runs"] {
		if _, exists := fields[column.Name]; !exists {
			t.Fatalf("physical projection omitted %s", column.Name)
		}
	}
}

func TestEveryNullableProjectionKeyIsRequired(t *testing.T) {
	for _, name := range physicalFields {
		t.Run(name, func(t *testing.T) {
			fields := physicalObject(t)
			delete(fields, name)
			data, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeRecord(data); err == nil || !strings.Contains(err.Error(), "missing "+name) {
				t.Fatalf("missing physical field did not refuse: %v", err)
			}
		})
	}
}

func TestPhysicalNullsAndRetainedUnknownMetadataAreNotUpgraded(t *testing.T) {
	row := Record{StorageKey: "BF-record", BackfillID: "BF-record", Status: text("RUNNING"), StartedAtPrecReason: text("PENDING"), AnchorPrec: text("UNCAPTURED_ORIGIN")}
	decoded, err := DecodeRecord(physicalDocument(t, row))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Anchor != nil || decoded.StartedAt != nil || decoded.FinishedAt != nil || decoded.TokenID != nil || decoded.RecordedAt != nil || decoded.ResultJSON != nil {
		t.Fatalf("physical absence acquired invented values: %v", decoded)
	}
	if decoded.StartedAtPrecReason == nil || *decoded.StartedAtPrecReason != "PENDING" || decoded.AnchorPrec == nil || *decoded.AnchorPrec != "UNCAPTURED_ORIGIN" {
		t.Fatal("retained metadata was normalized")
	}
}

func TestResultPreservesSQLNullVersusJSONNullAndExactNumberText(t *testing.T) {
	for _, raw := range []string{"null", `{"number":9007199254740993.000100,"text":"000.10"}`, `"opaque prior evidence"`} {
		row := Record{StorageKey: "BF-record", BackfillID: "BF-record", ResultJSON: &raw}
		decoded, err := DecodeRecord(physicalDocument(t, row))
		if err != nil {
			t.Fatal(err)
		}
		if decoded.ResultJSON == nil || *decoded.ResultJSON != raw {
			t.Fatalf("JSON text lost exact evidence: %v", decoded.ResultJSON)
		}
	}
}

func TestNonNullablePhysicalIdentitiesRefuseExplicitNull(t *testing.T) {
	for _, name := range []string{"storage_key", "backfill_id"} {
		fields := physicalObject(t)
		fields[name] = json.RawMessage("null")
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeRecord(data); err == nil || !strings.Contains(err.Error(), "null "+name) {
			t.Fatalf("null required identity did not refuse: %v", err)
		}
	}
}

func TestPhysicalDecoderRefusesDuplicateUnknownAndTrailingDocuments(t *testing.T) {
	data := physicalDocument(t, Record{StorageKey: "BF-record", BackfillID: "BF-record"})
	for _, malformed := range [][]byte{
		append(append([]byte{}, data...), []byte(` {}`)...),
		bytes.Replace(data, []byte(`"status":null`), []byte(`"status":"PENDING","status":"RUNNING"`), 1),
		bytes.Replace(data, []byte(`"status":null`), []byte(`"status":null,"unexpected":true`), 1),
		[]byte(`[]`),
		[]byte(`null`),
	} {
		if _, err := DecodeRecord(malformed); err == nil {
			t.Fatalf("malformed physical object was admitted: %s", malformed)
		}
	}
}

func TestPhysicalClockPreservesActualInstantWithoutAddingOrigin(t *testing.T) {
	at := time.Date(2026, 10, 26, 11, 12, 13, 123456000, time.FixedZone("declared-offset", 3*3600))
	row := Record{StorageKey: "BF-record", BackfillID: "BF-record", Anchor: &at}
	decoded, err := DecodeRecord(physicalDocument(t, row))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Anchor == nil || !decoded.Anchor.Equal(at) || decoded.AnchorPrec != nil || decoded.AnchorPrecReason != nil {
		t.Fatal("physical clock or unavailable provenance changed")
	}
}

func TestMalformedConsumedFactsRemainPhysicalEvidence(t *testing.T) {
	row := Record{StorageKey: "", BackfillID: "", Status: text("UNSUPPORTED"), TokenID: nil, ResultJSON: text(`{"prior":false}`)}
	decoded, err := DecodeRecord(physicalDocument(t, row))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.StorageKey != "" || decoded.BackfillID != "" || decoded.TokenID != nil || decoded.Status == nil || *decoded.Status != "UNSUPPORTED" {
		t.Fatal("physical decoding silently repaired consumed facts")
	}
}

func TestSQLJsonTextMustBeACompleteValue(t *testing.T) {
	for _, raw := range []string{"", "NaN", "{", "null null"} {
		row := Record{StorageKey: "BF-record", BackfillID: "BF-record", ResultJSON: &raw}
		if _, err := DecodeRecord(physicalDocument(t, row)); err == nil {
			t.Fatalf("invalid physical JSON text was admitted: %q", raw)
		}
	}
}
