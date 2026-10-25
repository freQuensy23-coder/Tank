package requeststate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type Record struct {
	StorageKey            string     `json:"storage_key"`
	BudgetS               *string    `json:"budget_s"`
	CodeVersion           *string    `json:"code_version"`
	ConfigVersion         *string    `json:"config_version"`
	Deadline              *time.Time `json:"deadline"`
	DeadlinePrec          *string    `json:"deadline_prec"`
	DeadlinePrecReason    *string    `json:"deadline_prec_reason"`
	End                   *time.Time `json:"end"`
	EndPrec               *string    `json:"end_prec"`
	EndPrecReason         *string    `json:"end_prec_reason"`
	EventID               *string    `json:"event_id"`
	FinishedAt            *time.Time `json:"finished_at"`
	FinishedAtPrec        *string    `json:"finished_at_prec"`
	FinishedAtPrecReason  *string    `json:"finished_at_prec_reason"`
	Kind                  *string    `json:"kind"`
	Reason                *string    `json:"reason"`
	RecordedAt            *time.Time `json:"recorded_at"`
	RecordedAtPrec        *string    `json:"recorded_at_prec"`
	RecordedAtPrecReason  *string    `json:"recorded_at_prec_reason"`
	RequestID             string     `json:"request_id"`
	RequestedAt           *time.Time `json:"requested_at"`
	RequestedAtPrec       *string    `json:"requested_at_prec"`
	RequestedAtPrecReason *string    `json:"requested_at_prec_reason"`
	Start                 *time.Time `json:"start"`
	StartPrec             *string    `json:"start_prec"`
	StartPrecReason       *string    `json:"start_prec_reason"`
	StartedAt             *time.Time `json:"started_at"`
	StartedAtPrec         *string    `json:"started_at_prec"`
	StartedAtPrecReason   *string    `json:"started_at_prec_reason"`
	Status                *string    `json:"status"`
	TokenID               *string    `json:"token_id"`
	Writer                *string    `json:"writer"`
	RequestJSON           *string    `json:"request"`
	ResultJSON            *string    `json:"result"`
	SemanticFingerprint   *string    `json:"semantic_fingerprint"`
}

func DecodeRecord(data []byte) (Record, error) {
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return Record{}, err
	}
	for _, name := range []string{
		"storage_key",
		"budget_s",
		"code_version",
		"config_version",
		"deadline",
		"deadline_prec",
		"deadline_prec_reason",
		"end",
		"end_prec",
		"end_prec_reason",
		"event_id",
		"finished_at",
		"finished_at_prec",
		"finished_at_prec_reason",
		"kind",
		"reason",
		"recorded_at",
		"recorded_at_prec",
		"recorded_at_prec_reason",
		"request_id",
		"requested_at",
		"requested_at_prec",
		"requested_at_prec_reason",
		"start",
		"start_prec",
		"start_prec_reason",
		"started_at",
		"started_at_prec",
		"started_at_prec_reason",
		"status",
		"token_id",
		"writer",
		"request",
		"result",
		"semantic_fingerprint",
	} {
		if _, present := keys[name]; !present {
			return Record{}, fmt.Errorf("physical capture request row is missing %s", name)
		}
	}
	for _, name := range []string{"storage_key", "request_id"} {
		if bytes.Equal(bytes.TrimSpace(keys[name]), []byte("null")) {
			return Record{}, fmt.Errorf("physical capture request row has null %s", name)
		}
	}
	var value Record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return Record{}, err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Record{}, errors.New("capture request row requires one JSON document")
	}
	for _, raw := range []*string{value.RequestJSON, value.ResultJSON} {
		if raw != nil && !json.Valid([]byte(*raw)) {
			return Record{}, errors.New("physical capture request JSON text is invalid")
		}
	}
	return value, nil
}

func (r Record) Claimed() (Claimed, error) {
	if r.RequestID == "" {
		return Claimed{}, errors.New("capture request without request_id")
	}
	if r.Status == nil || *r.Status != "RUNNING" || r.Writer == nil || *r.Writer == "" || r.StartedAt == nil || r.StartedAtPrec == nil || *r.StartedAtPrec != pytime.CollectionOnly || r.StartedAtPrecReason != nil {
		return Claimed{}, errors.New("capture request requires an actual running claim and collection start clock")
	}
	var request json.RawMessage
	if r.RequestJSON != nil {
		request = json.RawMessage(*r.RequestJSON)
	}
	return Claimed{ID: r.RequestID, Writer: *r.Writer, StartedAt: r.StartedAt.UTC(), Kind: r.Kind, TokenID: r.TokenID, Start: r.Start, End: r.End, Deadline: r.Deadline, BudgetS: r.BudgetS, Request: request}, nil
}

func clockText(value *time.Time) *string {
	if value == nil {
		return nil
	}
	text := pytime.ISO(*value)
	return &text
}

func (r Record) SemanticDocument() ([]byte, error) {
	type fields Record
	if r.BudgetS != nil {
		if parsed, err := pydec.Parse(*r.BudgetS); err == nil {
			text := parsed.String()
			r.BudgetS = &text
		}
	}
	var request, result json.RawMessage
	if r.RequestJSON != nil {
		request = json.RawMessage(*r.RequestJSON)
	}
	if r.ResultJSON != nil {
		result = json.RawMessage(*r.ResultJSON)
	}
	wire := struct {
		StorageKey  *string         `json:"storage_key,omitempty"`
		Request     json.RawMessage `json:"request,omitempty"`
		Result      json.RawMessage `json:"result,omitempty"`
		Deadline    *string         `json:"deadline"`
		End         *string         `json:"end"`
		FinishedAt  *string         `json:"finished_at"`
		RecordedAt  *string         `json:"recorded_at"`
		RequestedAt *string         `json:"requested_at"`
		Start       *string         `json:"start"`
		StartedAt   *string         `json:"started_at"`
		*fields
	}{
		Request: request, Result: result,
		Deadline:    clockText(r.Deadline),
		End:         clockText(r.End),
		FinishedAt:  clockText(r.FinishedAt),
		RecordedAt:  clockText(r.RecordedAt),
		RequestedAt: clockText(r.RequestedAt),
		Start:       clockText(r.Start),
		StartedAt:   clockText(r.StartedAt),
		fields:      (*fields)(&r),
	}
	return json.Marshal(wire)
}
