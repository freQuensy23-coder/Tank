package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/coverage"
)

type CoverageProjectionProblem uint8

const (
	MissingCoverageInstrument CoverageProjectionProblem = iota + 1
	NullCoverageInstrument
	MalformedCoverageInstrument
	MissingCoverageBucket
	NullCoverageBucket
	MalformedCoverageBucket
	MissingCoverageCompletion
	NullCoverageCompletion
	MalformedCoverageCompletion
)

type CoverageProjectionError struct{ problem CoverageProjectionProblem }

func (e *CoverageProjectionError) Problem() CoverageProjectionProblem { return e.problem }
func (e *CoverageProjectionError) Error() string {
	switch e.problem {
	case MissingCoverageInstrument, NullCoverageInstrument, MalformedCoverageInstrument:
		return "capture coverage requires its actual instrument scope"
	case MissingCoverageBucket, NullCoverageBucket, MalformedCoverageBucket:
		return "capture coverage requires an aligned bucket within its declared interval"
	case MissingCoverageCompletion, NullCoverageCompletion, MalformedCoverageCompletion:
		return "capture coverage requires boolean completion flags"
	default:
		return "capture coverage projection error is uninitialized"
	}
}

func coverageColumns(q coverage.Query) (string, string, error) {
	switch q.Series() {
	case coverage.Flow:
		return "mkt.flow_bars_1m", "minute_start", nil
	case coverage.Klines:
		return "mkt.klines_1m", "open_time", nil
	default:
		return "", "", errors.New("capture coverage requires a supported minute series")
	}
}

func AdmitCoverageProjection(q coverage.Query, rows []map[string]any) ([]coverage.Minute, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	_, field, err := coverageColumns(q)
	if err != nil {
		return nil, err
	}
	out := make([]coverage.Minute, 0, len(rows))
	for _, row := range rows {
		for name := range row {
			if name != "instrument_id" && name != field && name != "complete" {
				return nil, errors.New("capture coverage projection contains an unrequested field")
			}
		}
		rawInstrument, present := row["instrument_id"]
		if !present {
			return nil, &CoverageProjectionError{MissingCoverageInstrument}
		}
		if rawInstrument == nil {
			return nil, &CoverageProjectionError{NullCoverageInstrument}
		}
		instrument, valid := rawInstrument.(string)
		if !valid {
			return nil, &CoverageProjectionError{MalformedCoverageInstrument}
		}
		rawBucket, present := row[field]
		if !present {
			return nil, &CoverageProjectionError{MissingCoverageBucket}
		}
		if rawBucket == nil {
			return nil, &CoverageProjectionError{NullCoverageBucket}
		}
		var at time.Time
		switch raw := rawBucket.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return nil, &CoverageProjectionError{MalformedCoverageBucket}
			}
			at = parsed
		case time.Time:
			at = raw
		default:
			return nil, &CoverageProjectionError{MalformedCoverageBucket}
		}
		rawComplete, present := row["complete"]
		if !present {
			return nil, &CoverageProjectionError{MissingCoverageCompletion}
		}
		if rawComplete == nil {
			return nil, &CoverageProjectionError{NullCoverageCompletion}
		}
		complete, valid := rawComplete.(bool)
		if !valid {
			return nil, &CoverageProjectionError{MalformedCoverageCompletion}
		}
		minute, err := coverage.NewMinute(q, instrument, at, complete)
		if err != nil {
			return nil, err
		}
		out = append(out, minute)
	}
	return out, nil
}

func validCoverageString(raw []byte) bool {
	if len(raw) < 2 || raw[0] != '"' {
		return true
	}
	for i := 1; i < len(raw)-1; {
		if raw[i] != '\\' {
			i++
			continue
		}
		if i+1 >= len(raw)-1 {
			return false
		}
		if raw[i+1] != 'u' {
			i += 2
			continue
		}
		if i+6 > len(raw)-1 {
			return false
		}
		value, err := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
		if err != nil {
			return false
		}
		i += 6
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 > len(raw)-1 || raw[i] != '\\' || raw[i+1] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func decodeCoverageObject(field string, line []byte) (map[string]any, error) {
	if !utf8.Valid(line) {
		return nil, errors.New("capture coverage projection requires UTF-8 JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("capture coverage requires a valid JSON projection object")
	}
	row := make(map[string]any, 3)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("capture coverage requires a valid JSON projection object")
		}
		name, valid := token.(string)
		if !valid {
			return nil, errors.New("capture coverage requires a valid JSON projection object")
		}
		if name != "instrument_id" && name != field && name != "complete" {
			return nil, errors.New("capture coverage projection contains an unrequested field")
		}
		if _, duplicate := row[name]; duplicate {
			return nil, errors.New("capture coverage projection contains duplicate fields")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, errors.New("capture coverage requires a valid JSON projection object")
		}
		if !validCoverageString(raw) {
			return nil, errors.New("capture coverage projection requires valid Unicode scalar strings")
		}
		valueDecoder := json.NewDecoder(bytes.NewReader(raw))
		valueDecoder.UseNumber()
		var value any
		if err := valueDecoder.Decode(&value); err != nil {
			return nil, errors.New("capture coverage requires a valid JSON projection object")
		}
		row[name] = value
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return nil, errors.New("capture coverage requires a valid JSON projection object")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("capture coverage requires one JSON projection object per line")
	}
	return row, nil
}

func decodeCoverageBody(q coverage.Query, body []byte) ([]coverage.Minute, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	_, field, err := coverageColumns(q)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		row, err := decodeCoverageObject(field, line)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return AdmitCoverageProjection(q, rows)
}

func (s *Store) CoverageMinutes(ctx context.Context, scope coverage.Query) ([]coverage.Minute, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	collection, field, err := coverageColumns(scope)
	if err != nil {
		return nil, err
	}
	if err := s.series(collection); err != nil {
		return nil, err
	}
	query := newCHQuery()
	query.where(chColumn(field) + " IS NOT NULL")
	chBound(query, field, ">=", scope.Start())
	chBound(query, field, "<", scope.End())
	query.where("instrument_id IN " + query.param(chArray([]string{scope.InstrumentID()}), "Array(String)"))
	statement := "SELECT instrument_id, " + chColumn(field) + ", complete FROM " + collection + " FINAL WHERE " + query.predicate() + " ORDER BY " + chColumn(field) + ", storage_key FORMAT JSONEachRow"
	body, err := s.CH.post(ctx, statement, query.params, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", collection, err)
	}
	return decodeCoverageBody(scope, body)
}
