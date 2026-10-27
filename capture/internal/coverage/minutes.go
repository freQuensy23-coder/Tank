package coverage

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

var (
	ErrSeries               = errors.New("capture coverage requires a supported minute series")
	ErrInstrument           = errors.New("capture coverage requires its actual instrument scope")
	ErrWindow               = errors.New("capture coverage requires its actual bounded interval")
	ErrInvertedWindow       = errors.New("capture coverage interval end precedes start")
	ErrBucket               = errors.New("capture coverage requires an aligned bucket within its declared interval")
	ErrWindowRepresentation = errors.New("capture coverage interval cannot be represented")
	ErrObservedSeries       = errors.New("capture coverage requires its actual minute series")
	ErrConflict             = errors.New("capture coverage contains conflicting observations for one bucket")
	ErrCompletion           = errors.New("capture coverage requires boolean completion flags")
)

type Series uint8

const (
	Flow Series = iota + 1
	Klines
)

type Query struct {
	series     Series
	instrument string
	start      time.Time
	end        time.Time
}

func NewQuery(series Series, instrument string, start, end time.Time) (Query, error) {
	q := Query{series, instrument, start.UTC(), end.UTC()}
	return q, q.Validate()
}

func (q Query) Series() Series       { return q.series }
func (q Query) InstrumentID() string { return q.instrument }
func (q Query) Start() time.Time     { return q.start }
func (q Query) End() time.Time       { return q.end }

func (q Query) Validate() error {
	if q.series != Flow && q.series != Klines {
		return ErrSeries
	}
	if strings.TrimSpace(q.instrument) == "" || !utf8.ValidString(q.instrument) || strings.IndexByte(q.instrument, 0) >= 0 {
		return ErrInstrument
	}
	if q.start.IsZero() || q.end.IsZero() {
		return ErrWindow
	}
	if q.end.Before(q.start) {
		return ErrInvertedWindow
	}
	if !q.start.Equal(pytime.MinuteFloor(q.start)) || !q.end.Equal(pytime.MinuteFloor(q.end)) {
		return ErrBucket
	}
	if !q.start.Add(q.end.Sub(q.start)).Equal(q.end) {
		return ErrWindowRepresentation
	}
	return nil
}

type Minute struct {
	series     Series
	instrument string
	at         time.Time
	complete   bool
}

func NewMinute(q Query, instrument string, at time.Time, complete bool) (Minute, error) {
	if err := q.Validate(); err != nil {
		return Minute{}, err
	}
	if instrument != q.instrument {
		return Minute{}, ErrInstrument
	}
	if !at.Equal(pytime.MinuteFloor(at)) || at.Before(q.start) || !at.Before(q.end) {
		return Minute{}, ErrBucket
	}
	return Minute{q.series, instrument, at.UTC(), complete}, nil
}

func (m Minute) Series() Series       { return m.series }
func (m Minute) InstrumentID() string { return m.instrument }
func (m Minute) At() time.Time        { return m.at }
func (m Minute) Complete() bool       { return m.complete }

func Count(q Query, rows []Minute) (backfillstate.MinuteCoverage, error) {
	if err := q.Validate(); err != nil {
		return backfillstate.MinuteCoverage{}, err
	}
	buckets := make(map[int64]bool, len(rows))
	for _, row := range rows {
		if row.series != q.series {
			return backfillstate.MinuteCoverage{}, ErrObservedSeries
		}
		if _, err := NewMinute(q, row.instrument, row.at, row.complete); err != nil {
			return backfillstate.MinuteCoverage{}, err
		}
		key := row.at.Unix()
		if prior, exists := buckets[key]; exists && prior != row.complete {
			return backfillstate.MinuteCoverage{}, ErrConflict
		}
		buckets[key] = row.complete
	}
	var complete int64
	for _, value := range buckets {
		if value {
			complete++
		}
	}
	return backfillstate.NewMinuteCoverage(int64(q.end.Sub(q.start)/time.Minute), complete)
}
