package backfillstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

type Record struct {
	StorageKey           string     `json:"storage_key"`
	Anchor               *time.Time `json:"anchor"`
	AnchorPrec           *string    `json:"anchor_prec"`
	AnchorPrecReason     *string    `json:"anchor_prec_reason"`
	BackfillID           string     `json:"backfill_id"`
	CodeVersion          *string    `json:"code_version"`
	ConfigVersion        *string    `json:"config_version"`
	EventID              *string    `json:"event_id"`
	FinishedAt           *time.Time `json:"finished_at"`
	FinishedAtPrec       *string    `json:"finished_at_prec"`
	FinishedAtPrecReason *string    `json:"finished_at_prec_reason"`
	InstrumentID         *string    `json:"instrument_id"`
	Reason               *string    `json:"reason"`
	ReceivedTS           *time.Time `json:"received_ts"`
	ReceivedTSPrec       *string    `json:"received_ts_prec"`
	ReceivedTSPrecReason *string    `json:"received_ts_prec_reason"`
	RecordedAt           *time.Time `json:"recorded_at"`
	RecordedAtPrec       *string    `json:"recorded_at_prec"`
	RecordedAtPrecReason *string    `json:"recorded_at_prec_reason"`
	StartedAt            *time.Time `json:"started_at"`
	StartedAtPrec        *string    `json:"started_at_prec"`
	StartedAtPrecReason  *string    `json:"started_at_prec_reason"`
	Status               *string    `json:"status"`
	TokenID              *string    `json:"token_id"`
	ResultJSON           *string    `json:"result"`
	SemanticFingerprint  *string    `json:"semantic_fingerprint"`
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
