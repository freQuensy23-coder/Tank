package backfillstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type TimestampText string

type Record struct {
	StorageKey           string         `json:"storage_key"`
	Anchor               *TimestampText `json:"anchor"`
	AnchorPrec           *string        `json:"anchor_prec"`
	AnchorPrecReason     *string        `json:"anchor_prec_reason"`
	BackfillID           string         `json:"backfill_id"`
	CodeVersion          *string        `json:"code_version"`
	ConfigVersion        *string        `json:"config_version"`
	EventID              *string        `json:"event_id"`
	FinishedAt           *TimestampText `json:"finished_at"`
	FinishedAtPrec       *string        `json:"finished_at_prec"`
	FinishedAtPrecReason *string        `json:"finished_at_prec_reason"`
	InstrumentID         *string        `json:"instrument_id"`
	Reason               *string        `json:"reason"`
	ReceivedTS           *TimestampText `json:"received_ts"`
	ReceivedTSPrec       *string        `json:"received_ts_prec"`
	ReceivedTSPrecReason *string        `json:"received_ts_prec_reason"`
	RecordedAt           *TimestampText `json:"recorded_at"`
	RecordedAtPrec       *string        `json:"recorded_at_prec"`
	RecordedAtPrecReason *string        `json:"recorded_at_prec_reason"`
	StartedAt            *TimestampText `json:"started_at"`
	StartedAtPrec        *string        `json:"started_at_prec"`
	StartedAtPrecReason  *string        `json:"started_at_prec_reason"`
	Status               *string        `json:"status"`
	TokenID              *string        `json:"token_id"`
	ResultJSON           *string        `json:"result"`
	SemanticFingerprint  *string        `json:"semantic_fingerprint"`
}

var physicalFields = [...]string{
	"storage_key", "anchor", "anchor_prec", "anchor_prec_reason", "backfill_id",
	"code_version", "config_version", "event_id", "finished_at", "finished_at_prec",
	"finished_at_prec_reason", "instrument_id", "reason", "received_ts", "received_ts_prec",
	"received_ts_prec_reason", "recorded_at", "recorded_at_prec", "recorded_at_prec_reason",
	"started_at", "started_at_prec", "started_at_prec_reason", "status", "token_id", "result",
	"semantic_fingerprint",
}

func physicalKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("physical backfill row requires a JSON object")
	}
	seen := make(map[string]struct{}, len(physicalFields))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, valid := token.(string)
		if !valid {
			return errors.New("physical backfill row requires text field names")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("physical backfill row repeats %s", name)
		}
		seen[name] = struct{}{}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return err
		}
		if (name == "storage_key" || name == "backfill_id") && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("physical backfill row has null %s", name)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	for _, name := range physicalFields {
		if _, present := seen[name]; !present {
			return fmt.Errorf("physical backfill row is missing %s", name)
		}
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("physical backfill row requires one JSON document")
	}
	return nil
}

func DecodeRecord(data []byte) (Record, error) {
	if !utf8.Valid(data) {
		return Record{}, errors.New("physical backfill row requires UTF-8 JSON")
	}
	if err := physicalKeys(data); err != nil {
		return Record{}, err
	}
	var record Record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return Record{}, err
	}
	if record.ResultJSON != nil && !json.Valid([]byte(*record.ResultJSON)) {
		return Record{}, errors.New("physical backfill result JSON text is invalid")
	}
	return record, nil
}

func clockText(value *TimestampText) *string {
	if value == nil {
		return nil
	}
	text := string(*value)
	if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
		text = pytime.ISO(parsed)
	}
	return &text
}

func (r Record) SemanticDocument() ([]byte, error) {
	type fields Record
	var result json.RawMessage
	if r.ResultJSON != nil {
		if !json.Valid([]byte(*r.ResultJSON)) {
			return nil, errors.New("physical backfill result JSON text is invalid")
		}
		result = json.RawMessage(*r.ResultJSON)
	}
	wire := struct {
		StorageKey *string         `json:"storage_key,omitempty"`
		Anchor     *string         `json:"anchor"`
		FinishedAt *string         `json:"finished_at"`
		ReceivedTS *string         `json:"received_ts"`
		RecordedAt *string         `json:"recorded_at"`
		StartedAt  *string         `json:"started_at"`
		Result     json.RawMessage `json:"result"`
		*fields
	}{
		Anchor: clockText(r.Anchor), FinishedAt: clockText(r.FinishedAt),
		ReceivedTS: clockText(r.ReceivedTS), RecordedAt: clockText(r.RecordedAt),
		StartedAt: clockText(r.StartedAt), Result: result, fields: (*fields)(&r),
	}
	return json.Marshal(wire)
}
