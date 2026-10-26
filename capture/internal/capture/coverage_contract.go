package capture

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func decodeReferenceTradeIdentity(raw any) (backfillstate.TradeIdentity, error) {
	var numeric string
	switch value := raw.(type) {
	case string:
		if value == "" {
			return backfillstate.TradeIdentity{}, errors.New("reference trade identity is empty")
		}
		return backfillstate.TextTradeIdentity(value)
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
		return backfillstate.TradeIdentity{}, errors.New("reference trade identity requires text or an integer")
	}
	return backfillstate.IntegerTradeIdentity(json.Number(numeric))
}

func decodeCoverageMinutes(rows []map[string]any, field string, start, end time.Time) (backfillstate.MinuteCoverage, error) {
	expected := int64(end.Sub(start) / time.Minute)
	if expected < 0 {
		return backfillstate.MinuteCoverage{}, errors.New("capture coverage interval end precedes start")
	}
	buckets := make(map[int64]bool, len(rows))
	for _, row := range rows {
		at, err := pytime.Parse(row[field])
		if err != nil || !at.Equal(pytime.MinuteFloor(at)) || at.Before(start) || !at.Before(end) {
			return backfillstate.MinuteCoverage{}, errors.New("capture coverage requires an aligned bucket within its declared interval")
		}
		complete, ok := row["complete"].(bool)
		if !ok {
			return backfillstate.MinuteCoverage{}, errors.New("capture coverage requires boolean completion flags")
		}
		key := at.Unix()
		if previous, exists := buckets[key]; exists && previous != complete {
			return backfillstate.MinuteCoverage{}, errors.New("capture coverage contains conflicting observations for one bucket")
		}
		buckets[key] = complete
	}
	var complete int64
	for _, observedComplete := range buckets {
		if observedComplete {
			complete++
		}
	}
	return backfillstate.NewMinuteCoverage(expected, complete)
}
