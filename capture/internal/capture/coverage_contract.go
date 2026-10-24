package capture

import (
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type CoverageMinutes struct {
	ExpectedMinutes            int64
	CompleteMinutes            int64
	MissingOrIncompleteMinutes int64
	Complete                   bool
}

func (r CoverageMinutes) storageValue() map[string]any {
	return map[string]any{
		"expected_minutes": r.ExpectedMinutes, "complete_minutes": r.CompleteMinutes,
		"missing_or_incomplete_minutes": r.MissingOrIncompleteMinutes, "complete": r.Complete,
	}
}

type CaptureWindowCoverage struct {
	Start                     string
	End                       string
	ObservedBucketStart       string
	ObservedBucketEnd         string
	UnresolvedBoundarySeconds float64
	Flow                      CoverageMinutes
	Klines                    CoverageMinutes
}

func (r CaptureWindowCoverage) storageValue() map[string]any {
	return map[string]any{
		"start": r.Start, "end": r.End, "observed_bucket_start": r.ObservedBucketStart,
		"observed_bucket_end": r.ObservedBucketEnd, "timestamp_precision": "BUCKET",
		"unresolved_boundary_seconds": r.UnresolvedBoundarySeconds,
		"flow":                        r.Flow.storageValue(), "klines": r.Klines.storageValue(),
	}
}

type CaptureWindows struct {
	W1H  CaptureWindowCoverage
	W4H  CaptureWindowCoverage
	W24H CaptureWindowCoverage
}

func (r CaptureWindows) storageValue() map[string]any {
	return map[string]any{"W1H": r.W1H.storageValue(), "W4H": r.W4H.storageValue(), "W24H": r.W24H.storageValue()}
}

type referenceTradeIdentity struct {
	text   *string
	number *json.Number
}

func decodeReferenceTradeIdentity(raw any) (referenceTradeIdentity, error) {
	var numeric string
	switch value := raw.(type) {
	case string:
		if value == "" {
			return referenceTradeIdentity{}, errors.New("reference trade identity is empty")
		}
		return referenceTradeIdentity{text: &value}, nil
	case int:
		numeric = strconv.Itoa(value)
	case int32:
		numeric = strconv.FormatInt(int64(value), 10)
	case int64:
		numeric = strconv.FormatInt(value, 10)
	case uint64:
		numeric = strconv.FormatUint(value, 10)
	case json.Number:
		numeric = string(value)
	default:
		return referenceTradeIdentity{}, errors.New("reference trade identity requires text or an integer")
	}
	if _, integer := new(big.Int).SetString(numeric, 10); !integer || !json.Valid([]byte(numeric)) {
		return referenceTradeIdentity{}, errors.New("reference trade identity requires an integer")
	}
	number := json.Number(numeric)
	return referenceTradeIdentity{number: &number}, nil
}

func (r referenceTradeIdentity) storageValue() any {
	if r.text != nil {
		return *r.text
	}
	if r.number != nil {
		return *r.number
	}
	return nil
}

type Pre24Reference struct {
	Available bool
	TradeID   *referenceTradeIdentity
	TS        *string
	Precision *string
	Reason    *string
}

func (r Pre24Reference) storageValue() map[string]any {
	out := map[string]any{"available": r.Available, "trade_id": nil, "ts": nil, "ts_prec": nil, "reason": nil}
	if r.TradeID != nil {
		out["trade_id"] = r.TradeID.storageValue()
	}
	if r.TS != nil {
		out["ts"] = *r.TS
	}
	if r.Precision != nil {
		out["ts_prec"] = *r.Precision
	}
	if r.Reason != nil {
		out["reason"] = *r.Reason
	}
	return out
}

type InstrumentCaptureCoverage struct {
	InstrumentID   string
	Windows        CaptureWindows
	Complete       bool
	Pre24Reference *Pre24Reference
}

func (r InstrumentCaptureCoverage) storageValue() map[string]any {
	out := map[string]any{"instrument_id": r.InstrumentID, "windows": r.Windows.storageValue(), "complete": r.Complete}
	if r.Pre24Reference != nil {
		out["pre24_reference"] = r.Pre24Reference.storageValue()
	}
	return out
}

func decodeCoverageMinutes(rows []map[string]any, field string, start, end time.Time) (CoverageMinutes, error) {
	expected := int64(end.Sub(start) / time.Minute)
	if expected < 0 {
		return CoverageMinutes{}, errors.New("capture coverage interval end precedes start")
	}
	buckets := make(map[int64]bool, len(rows))
	for _, row := range rows {
		at, err := pytime.Parse(row[field])
		if err != nil || !at.Equal(pytime.MinuteFloor(at)) || at.Before(start) || !at.Before(end) {
			return CoverageMinutes{}, errors.New("capture coverage requires an aligned bucket within its declared interval")
		}
		complete, ok := row["complete"].(bool)
		if !ok {
			return CoverageMinutes{}, errors.New("capture coverage requires boolean completion flags")
		}
		key := at.Unix()
		if previous, exists := buckets[key]; exists && previous != complete {
			return CoverageMinutes{}, errors.New("capture coverage contains conflicting observations for one bucket")
		}
		buckets[key] = complete
	}
	var complete int64
	for _, observedComplete := range buckets {
		if observedComplete {
			complete++
		}
	}
	return CoverageMinutes{
		ExpectedMinutes: expected, CompleteMinutes: complete,
		MissingOrIncompleteMinutes: expected - complete, Complete: complete == expected && expected > 0,
	}, nil
}
