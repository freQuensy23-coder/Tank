package capture

import (
	"errors"
	"fmt"
	"time"

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
