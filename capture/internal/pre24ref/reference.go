package pre24ref

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

var (
	ErrScope       = errors.New("pre24 reference requires its actual instrument and boundary")
	ErrObservation = errors.New("pre24 reference requires an admitted observation for its actual scope")
	ErrWitness     = errors.New("pre24 reference candidate requires an actual predecessor observation")
	ErrPredecessor = errors.New("pre24 reference trade must precede its declared boundary")
	ErrConflict    = errors.New("pre24 reference has conflicting trade evidence at its latest observed clock")
)

type Query struct {
	instrument string
	boundary   time.Time
}

func NewQuery(instrument string, boundary time.Time) (Query, error) {
	q := Query{instrument: instrument, boundary: boundary.UTC()}
	return q, q.Validate()
}

func (q Query) InstrumentID() string { return q.instrument }
func (q Query) Boundary() time.Time  { return q.boundary }
func (q Query) Start() time.Time     { return pytime.MinuteFloor(q.boundary).Add(-time.Minute) }
func (q Query) End() time.Time       { return pytime.MinuteFloor(q.boundary).Add(time.Minute) }
func (q Query) Validate() error {
	if strings.TrimSpace(q.instrument) == "" || !utf8.ValidString(q.instrument) || strings.IndexByte(q.instrument, 0) >= 0 || q.boundary.IsZero() {
		return ErrScope
	}
	return nil
}

type Candidate interface {
	clock() time.Time
	result() (backfillstate.Pre24Reference, error)
	candidate()
}

type witness struct {
	at        time.Time
	reference backfillstate.Pre24Reference
}

func (w witness) clock() time.Time                              { return w.at }
func (w witness) result() (backfillstate.Pre24Reference, error) { return w.reference, nil }
func (w witness) candidate()                                    {}

type refused struct {
	at    time.Time
	cause error
}

func (r refused) clock() time.Time { return r.at }
func (r refused) result() (backfillstate.Pre24Reference, error) {
	return backfillstate.Pre24Reference{}, r.cause
}
func (r refused) candidate() {}

func Witness(reference backfillstate.Pre24Reference) (Candidate, error) {
	at, available, err := reference.ObservationClock()
	if err != nil {
		return nil, err
	}
	if !available {
		return nil, ErrWitness
	}
	return witness{at: at, reference: reference}, nil
}

func Refusal(at time.Time, cause error) (Candidate, error) {
	if cause == nil {
		return nil, ErrWitness
	}
	return refused{at: at.UTC(), cause: cause}, nil
}

type Observation struct {
	query      Query
	candidates []Candidate
	observed   bool
}

func NewObservation(q Query, candidates []Candidate) (Observation, error) {
	if err := q.Validate(); err != nil {
		return Observation{}, err
	}
	for _, candidate := range candidates {
		if candidate == nil {
			return Observation{}, ErrWitness
		}
		if !candidate.clock().Before(q.boundary) {
			return Observation{}, ErrPredecessor
		}
	}
	return Observation{query: q, candidates: append([]Candidate(nil), candidates...), observed: true}, nil
}

func (o Observation) Reference(q Query) (backfillstate.Pre24Reference, error) {
	if err := q.Validate(); err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	if !o.observed || o.query.instrument != q.instrument || !o.query.boundary.Equal(q.boundary) {
		return backfillstate.Pre24Reference{}, ErrObservation
	}
	var latest time.Time
	var selected []Candidate
	for _, candidate := range o.candidates {
		at := candidate.clock()
		if len(selected) == 0 || at.After(latest) {
			latest, selected = at, []Candidate{candidate}
		} else if at.Equal(latest) {
			selected = append(selected, candidate)
		}
	}
	if len(selected) == 0 {
		return backfillstate.MissingPre24Reference(), nil
	}
	reference, err := selected[0].result()
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	encoded, err := json.Marshal(reference)
	if err != nil {
		return backfillstate.Pre24Reference{}, err
	}
	for _, candidate := range selected[1:] {
		other, err := candidate.result()
		if err != nil {
			return backfillstate.Pre24Reference{}, err
		}
		document, err := json.Marshal(other)
		if err != nil {
			return backfillstate.Pre24Reference{}, err
		}
		if !bytes.Equal(encoded, document) {
			return backfillstate.Pre24Reference{}, ErrConflict
		}
	}
	return reference, nil
}

type ReadError struct{ cause error }

func FailedRead(cause error) error {
	if cause == nil {
		return errors.New("pre24 reference read failure requires its actual cause")
	}
	return ReadError{cause: cause}
}
func (e ReadError) Error() string {
	if e.cause == nil {
		return "pre24 reference read failure is uninitialized"
	}
	return e.cause.Error()
}
func (e ReadError) Unwrap() error { return e.cause }
