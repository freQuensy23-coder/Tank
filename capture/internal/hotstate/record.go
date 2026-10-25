package hotstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Record struct {
	StorageKey          string     `json:"storage_key"`
	CodeVersion         *string    `json:"code_version"`
	ConfigVersion       *string    `json:"config_version"`
	EventID             string     `json:"event_id"`
	ExpiresAt           *time.Time `json:"expires_at"`
	ExpiresAtPrec       *string    `json:"expires_at_prec"`
	ExpiresAtReason     *string    `json:"expires_at_prec_reason"`
	InstrumentID        *string    `json:"instrument_id"`
	Priority            *int64     `json:"priority"`
	ReceivedTS          *time.Time `json:"received_ts"`
	ReceivedTSPrec      *string    `json:"received_ts_prec"`
	ReceivedTSReason    *string    `json:"received_ts_prec_reason"`
	RecordedAt          *time.Time `json:"recorded_at"`
	RecordedAtPrec      *string    `json:"recorded_at_prec"`
	RecordedAtReason    *string    `json:"recorded_at_prec_reason"`
	Since               *time.Time `json:"since"`
	SincePrec           *string    `json:"since_prec"`
	SinceReason         *string    `json:"since_prec_reason"`
	TokenID             string     `json:"token_id"`
	SemanticFingerprint *string    `json:"semantic_fingerprint"`
}

type Scope struct{ TokenID, EventID string }

type OriginKind string

const (
	RecordedHotSince       OriginKind = "RECORDED_HOT_SINCE"
	RecordedBackfillAnchor OriginKind = "RECORDED_BACKFILL_ANCHOR"
)

type ClockEvidence struct {
	Value     time.Time
	Precision *string
	Reason    *string
}

type OptionalClockEvidence struct {
	Value     *time.Time
	Precision *string
	Reason    *string
}

type WindowOrigin struct {
	Kind         OriginKind
	Clock        ClockEvidence
	BackfillID   *string
	StartedClock *OptionalClockEvidence
}

func (r Record) Scope() (Scope, error) {
	if r.TokenID == "" || r.EventID == "" {
		return Scope{}, errors.New("HOT membership requires explicit token and event identities")
	}
	return Scope{TokenID: r.TokenID, EventID: r.EventID}, nil
}
func (r Record) RecordedSince() (WindowOrigin, error) {
	if r.Since == nil {
		return WindowOrigin{}, errors.New("HOT membership has no recorded since clock")
	}
	return WindowOrigin{Kind: RecordedHotSince, Clock: ClockEvidence{Value: *r.Since, Precision: r.SincePrec, Reason: r.SinceReason}}, nil
}

type AnchorRecord struct {
	StorageKey      string     `json:"storage_key"`
	BackfillID      string     `json:"backfill_id"`
	EventID         *string    `json:"event_id"`
	TokenID         *string    `json:"token_id"`
	Anchor          *time.Time `json:"anchor"`
	AnchorPrec      *string    `json:"anchor_prec"`
	AnchorReason    *string    `json:"anchor_prec_reason"`
	StartedAt       *time.Time `json:"started_at"`
	StartedAtPrec   *string    `json:"started_at_prec"`
	StartedAtReason *string    `json:"started_at_prec_reason"`
	Status          *string    `json:"status"`
}

func (r AnchorRecord) Scope() (Scope, error) {
	if r.EventID == nil || *r.EventID == "" || r.TokenID == nil || *r.TokenID == "" {
		return Scope{}, errors.New("backfill window origin requires explicit token and event identities")
	}
	return Scope{TokenID: *r.TokenID, EventID: *r.EventID}, nil
}
func (r AnchorRecord) RecordedAnchor() (WindowOrigin, error) {
	if r.Anchor == nil || r.BackfillID == "" {
		return WindowOrigin{}, errors.New("matching backfill has no declared anchor evidence")
	}
	return WindowOrigin{Kind: RecordedBackfillAnchor, Clock: ClockEvidence{Value: *r.Anchor, Precision: r.AnchorPrec, Reason: r.AnchorReason}, BackfillID: &r.BackfillID}, nil
}

func requireProjection(data []byte, required, nonnull []string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, key := range required {
		if _, present := fields[key]; !present {
			return fmt.Errorf("physical HOT projection is missing %s", key)
		}
	}
	for _, key := range nonnull {
		if bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
			return fmt.Errorf("physical HOT projection has null %s", key)
		}
	}
	return nil
}
func DecodeRecord(data []byte) (Record, error) {
	if err := requireProjection(data, []string{
		"storage_key", "code_version", "config_version", "event_id", "expires_at", "expires_at_prec", "expires_at_prec_reason",
		"instrument_id", "priority", "received_ts", "received_ts_prec", "received_ts_prec_reason", "recorded_at", "recorded_at_prec",
		"recorded_at_prec_reason", "since", "since_prec", "since_prec_reason", "token_id", "semantic_fingerprint",
	}, []string{"storage_key", "event_id", "token_id"}); err != nil {
		return Record{}, err
	}
	var record Record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return Record{}, err
	}
	return record, nil
}
func DecodeAnchorRecord(data []byte) (AnchorRecord, error) {
	if err := requireProjection(data, []string{
		"storage_key", "backfill_id", "event_id", "token_id", "anchor", "anchor_prec", "anchor_prec_reason",
		"started_at", "started_at_prec", "started_at_prec_reason", "status",
	}, []string{"storage_key", "backfill_id"}); err != nil {
		return AnchorRecord{}, err
	}
	var record AnchorRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return AnchorRecord{}, err
	}
	return record, nil
}
