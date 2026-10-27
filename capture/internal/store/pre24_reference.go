package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pre24ref"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type Pre24ProjectionProblem uint8

const (
	MissingPre24Instrument Pre24ProjectionProblem = iota + 1
	NullPre24Instrument
	MalformedPre24Instrument
	ForeignPre24Instrument
	MissingPre24Minute
	NullPre24Minute
	MalformedPre24Minute
	MissingPre24Points
)

type Pre24ProjectionError struct{ problem Pre24ProjectionProblem }

func (e *Pre24ProjectionError) Problem() Pre24ProjectionProblem { return e.problem }
func (e *Pre24ProjectionError) Error() string {
	switch e.problem {
	case MissingPre24Instrument, NullPre24Instrument, MalformedPre24Instrument, ForeignPre24Instrument:
		return "pre24 reference projection requires its actual instrument scope"
	case MissingPre24Minute, NullPre24Minute, MalformedPre24Minute:
		return "pre24 reference projection requires an aligned minute within its declared interval"
	case MissingPre24Points:
		return "pre24 reference projection is missing reference_points"
	default:
		return "pre24 reference projection error is uninitialized"
	}
}

func referenceClock(raw any) (time.Time, error) {
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

func referenceTradeIdentity(raw any) (backfillstate.TradeIdentity, error) {
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

func referenceTrade(trade map[string]any) (backfillstate.Pre24Reference, error) {
	identity, err := referenceTradeIdentity(trade["trade_id"])
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

func referenceCandidates(points any, q pre24ref.Query) ([]pre24ref.Candidate, error) {
	if points == nil {
		return nil, nil
	}
	list, valid := points.([]any)
	if !valid {
		return nil, errors.New("pre24 reference points require a JSON array or unreported value")
	}
	var candidates []pre24ref.Candidate
	for _, raw := range list {
		point, valid := raw.(map[string]any)
		if !valid || point == nil {
			return nil, errors.New("pre24 reference point requires a JSON object")
		}
		if point["complete"] == nil {
			continue
		}
		complete, valid := point["complete"].(bool)
		if !valid {
			return nil, errors.New("pre24 reference completion requires a boolean or unreported value")
		}
		if !complete {
			continue
		}
		at, err := referenceClock(point["boundary"])
		if err != nil {
			return nil, err
		}
		if !at.Equal(q.Boundary()) || point["last_trade"] == nil {
			continue
		}
		trade, valid := point["last_trade"].(map[string]any)
		if !valid {
			return nil, errors.New("pre24 reference trade requires a JSON object or unreported value")
		}
		if trade["ts"] == nil {
			continue
		}
		clock, err := referenceClock(trade["ts"])
		if err != nil {
			return nil, err
		}
		if !clock.Before(q.Boundary()) {
			return nil, pre24ref.ErrPredecessor
		}
		reference, err := referenceTrade(trade)
		var candidate pre24ref.Candidate
		if err != nil {
			candidate, err = pre24ref.Refusal(clock, err)
		} else {
			candidate, err = pre24ref.Witness(reference)
		}
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func referenceJSON(data []byte) (any, error) {
	if !utf8.Valid(data) || !json.Valid(data) {
		return nil, errors.New("pre24 reference requires valid UTF-8 JSON")
	}
	for index := 0; index < len(data); index++ {
		if data[index] != '"' {
			continue
		}
		start := index
		for index++; index < len(data); index++ {
			if data[index] == '\\' {
				index++
				continue
			}
			if data[index] == '"' {
				break
			}
		}
		if !validCoverageString(data[start : index+1]) {
			return nil, errors.New("pre24 reference requires valid Unicode scalar strings")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return referenceJSONValue(decoder)
}

func referenceJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, structured := token.(json.Delim)
	if !structured {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, valid := token.(string)
			if !valid {
				return nil, errors.New("pre24 reference JSON requires text field names")
			}
			if _, duplicate := object[name]; duplicate {
				return nil, errors.New("pre24 reference projection contains duplicate JSON fields")
			}
			value, err := referenceJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
			return nil, errors.New("pre24 reference JSON requires a complete object")
		}
		return object, nil
	case '[':
		list := make([]any, 0)
		for decoder.More() {
			value, err := referenceJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			list = append(list, value)
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim(']') {
			return nil, errors.New("pre24 reference JSON requires a complete array")
		}
		return list, nil
	default:
		return nil, errors.New("pre24 reference JSON requires an actual value")
	}
}

func AdmitPre24Projection(q pre24ref.Query, rows []map[string]any) (pre24ref.Observation, error) {
	if err := q.Validate(); err != nil {
		return pre24ref.Observation{}, err
	}
	var candidates []pre24ref.Candidate
	for _, row := range rows {
		for name := range row {
			if name != "instrument_id" && name != "minute_start" && name != "reference_points" {
				return pre24ref.Observation{}, errors.New("pre24 reference projection contains an unrequested field")
			}
		}
		rawInstrument, present := row["instrument_id"]
		if !present {
			return pre24ref.Observation{}, &Pre24ProjectionError{MissingPre24Instrument}
		}
		if rawInstrument == nil {
			return pre24ref.Observation{}, &Pre24ProjectionError{NullPre24Instrument}
		}
		instrument, valid := rawInstrument.(string)
		if !valid {
			return pre24ref.Observation{}, &Pre24ProjectionError{MalformedPre24Instrument}
		}
		if instrument != q.InstrumentID() {
			return pre24ref.Observation{}, &Pre24ProjectionError{ForeignPre24Instrument}
		}
		rawMinute, present := row["minute_start"]
		if !present {
			return pre24ref.Observation{}, &Pre24ProjectionError{MissingPre24Minute}
		}
		if rawMinute == nil {
			return pre24ref.Observation{}, &Pre24ProjectionError{NullPre24Minute}
		}
		minute, err := referenceClock(rawMinute)
		if err != nil || !minute.Equal(pytime.MinuteFloor(minute)) || minute.Before(q.Start()) || !minute.Before(q.End()) {
			return pre24ref.Observation{}, &Pre24ProjectionError{MalformedPre24Minute}
		}
		raw, present := row["reference_points"]
		if !present {
			return pre24ref.Observation{}, &Pre24ProjectionError{MissingPre24Points}
		}
		if raw == nil {
			continue
		}
		text, valid := raw.(string)
		if !valid {
			return pre24ref.Observation{}, fmt.Errorf("json column read as %T", raw)
		}
		value, err := referenceJSON([]byte(text))
		if err != nil {
			return pre24ref.Observation{}, err
		}
		points, err := referenceCandidates(pythonNumbers(value), q)
		if err != nil {
			return pre24ref.Observation{}, err
		}
		candidates = append(candidates, points...)
	}
	return pre24ref.NewObservation(q, candidates)
}

func decodePre24Body(q pre24ref.Query, body []byte) (pre24ref.Observation, error) {
	var rows []map[string]any
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		value, err := referenceJSON(line)
		if err != nil {
			return pre24ref.Observation{}, err
		}
		row, valid := value.(map[string]any)
		if !valid {
			return pre24ref.Observation{}, errors.New("pre24 reference projection requires a JSON object")
		}
		rows = append(rows, row)
	}
	return AdmitPre24Projection(q, rows)
}

func (s *Store) Pre24References(ctx context.Context, scope pre24ref.Query) (pre24ref.Observation, error) {
	if err := scope.Validate(); err != nil {
		return pre24ref.Observation{}, err
	}
	const collection = "mkt.flow_bars_1m"
	if err := s.series(collection); err != nil {
		return pre24ref.Observation{}, pre24ref.FailedRead(err)
	}
	query := newCHQuery()
	query.where("minute_start IS NOT NULL")
	chBound(query, "minute_start", ">=", scope.Start())
	chBound(query, "minute_start", "<", scope.End())
	query.where("instrument_id IN " + query.param(chArray([]string{scope.InstrumentID()}), "Array(String)"))
	statement := "SELECT instrument_id, minute_start, reference_points FROM " + collection + " FINAL WHERE " + query.predicate() + " ORDER BY minute_start, storage_key FORMAT JSONEachRow"
	body, err := s.CH.post(ctx, statement, query.params, nil)
	if err != nil {
		return pre24ref.Observation{}, pre24ref.FailedRead(fmt.Errorf("%s: %w", collection, err))
	}
	return decodePre24Body(scope, body)
}
