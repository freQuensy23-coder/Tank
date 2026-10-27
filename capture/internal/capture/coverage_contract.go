package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func storedBackfillClock(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case string:
		at, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %q", pytime.ErrTime, value)
		}
		return at.UTC(), nil
	case time.Time:
		return value.UTC(), nil
	case nil:
		return time.Time{}, fmt.Errorf("%w: missing", pytime.ErrTime)
	default:
		return time.Time{}, fmt.Errorf("%w: %T", pytime.ErrTime, raw)
	}
}

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

type completedKlineMinutes map[int64]struct{}

func (m completedKlineMinutes) contains(at time.Time) bool {
	_, present := m[at.Unix()]
	return present
}

func decodeCompletedKlineMinutes(rows []map[string]any, start, end, cutoff time.Time) (completedKlineMinutes, error) {
	completed := make(completedKlineMinutes)
	observations := make(map[int64]*bool, len(rows))
	for _, row := range rows {
		at, err := storedBackfillClock(row["open_time"])
		if err != nil || !at.Equal(pytime.MinuteFloor(at)) || at.Before(start) || !at.Before(end) {
			return nil, errors.New("capture kline coverage requires an aligned bucket within its declared interval")
		}
		var complete *bool
		if raw := row["complete"]; raw != nil {
			value, valid := raw.(bool)
			if !valid {
				return nil, errors.New("capture kline coverage requires boolean or unreported completion flags")
			}
			complete = &value
		}
		key := at.Unix()
		if prior, present := observations[key]; present && ((prior == nil) != (complete == nil) || (prior != nil && *prior != *complete)) {
			return nil, errors.New("capture kline coverage contains conflicting observations for one bucket")
		}
		observations[key] = complete
		if complete != nil && *complete && !at.Add(time.Minute).After(cutoff) {
			completed[key] = struct{}{}
		}
	}
	return completed, nil
}

func decodePre24Trade(trade map[string]any) (backfillstate.Pre24Reference, error) {
	identity, err := decodeReferenceTradeIdentity(trade["trade_id"])
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	ts, valid := trade["ts"].(string)
	if !valid {
		return backfillstate.Pre24Reference{}, errors.New("reference trade requires a JSON timestamp string")
	}
	var precision *string
	if raw := trade["ts_prec"]; raw != nil {
		value, valid := raw.(string)
		if !valid {
			return backfillstate.Pre24Reference{}, errors.New("reference trade precision requires text or null")
		}
		precision = &value
	}
	return backfillstate.AvailablePre24Reference(identity, ts, precision)
}

func decodePre24Reference(rows []map[string]any, boundary time.Time) (backfillstate.Pre24Reference, error) {
	var latest time.Time
	var candidates []map[string]any
	for _, row := range rows {
		if row["reference_points"] == nil {
			continue
		}
		points, valid := row["reference_points"].([]any)
		if !valid {
			return backfillstate.Pre24Reference{}, errors.New("pre24 reference points require a JSON array or unreported value")
		}
		for _, raw := range points {
			point, valid := raw.(map[string]any)
			if !valid || point == nil {
				return backfillstate.Pre24Reference{}, errors.New("pre24 reference point requires a JSON object")
			}
			if point["complete"] == nil {
				continue
			}
			complete, valid := point["complete"].(bool)
			if !valid {
				return backfillstate.Pre24Reference{}, errors.New("pre24 reference completion requires a boolean or unreported value")
			}
			if !complete {
				continue
			}
			at, err := storedBackfillClock(point["boundary"])
			if err != nil {
				return backfillstate.Pre24Reference{}, err
			}
			if !at.Equal(boundary) || point["last_trade"] == nil {
				continue
			}
			trade, valid := point["last_trade"].(map[string]any)
			if !valid {
				return backfillstate.Pre24Reference{}, errors.New("pre24 reference trade requires a JSON object or unreported value")
			}
			if trade["ts"] == nil {
				continue
			}
			ts, err := storedBackfillClock(trade["ts"])
			if err != nil {
				return backfillstate.Pre24Reference{}, err
			}
			if !ts.Before(boundary) {
				return backfillstate.Pre24Reference{}, errors.New("pre24 reference trade must precede its declared boundary")
			}
			if len(candidates) == 0 || ts.After(latest) {
				latest, candidates = ts, []map[string]any{trade}
			} else if ts.Equal(latest) {
				candidates = append(candidates, trade)
			}
		}
	}
	if len(candidates) == 0 {
		return backfillstate.MissingPre24Reference(), nil
	}
	reference, err := decodePre24Trade(candidates[0])
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	encoded, err := json.Marshal(reference)
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	for _, trade := range candidates[1:] {
		other, err := decodePre24Trade(trade)
		if err != nil {
			return backfillstate.Pre24Reference{}, err
		}
		document, err := json.Marshal(other)
		if err != nil {
			return backfillstate.Pre24Reference{}, err
		}
		if !bytes.Equal(encoded, document) {
			return backfillstate.Pre24Reference{}, errors.New("pre24 reference has conflicting trade evidence at its latest observed clock")
		}
	}
	return reference, nil
}
