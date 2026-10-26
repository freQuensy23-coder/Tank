package backfillstate

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type MinuteCoverage struct {
	expected, complete int64
	observed           bool
}

func NewMinuteCoverage(expected, complete int64) (MinuteCoverage, error) {
	if expected < 0 || complete < 0 || complete > expected {
		return MinuteCoverage{}, errors.New("capture minute counts require a bounded nonnegative observation")
	}
	return MinuteCoverage{expected: expected, complete: complete, observed: true}, nil
}

func (m MinuteCoverage) Validate() error {
	if !m.observed || m.expected < 0 || m.complete < 0 || m.complete > m.expected {
		return errors.New("capture minute coverage is not an admitted observation")
	}
	return nil
}
func (m MinuteCoverage) Complete() bool {
	return m.observed && m.expected > 0 && m.complete == m.expected
}
func (m MinuteCoverage) ExpectedMinutes() int64 { return m.expected }
func (m MinuteCoverage) CompleteMinutes() int64 { return m.complete }
func (m MinuteCoverage) MissingMinutes() int64  { return m.expected - m.complete }

func (m MinuteCoverage) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Expected        int64 `json:"expected_minutes"`
		CompleteMinutes int64 `json:"complete_minutes"`
		Missing         int64 `json:"missing_or_incomplete_minutes"`
		Complete        bool  `json:"complete"`
	}{m.expected, m.complete, m.MissingMinutes(), m.Complete()})
}

type boundarySeconds float64

func (value boundarySeconds) MarshalJSON() ([]byte, error) {
	text, err := pyjson.FloatRepr(float64(value))
	return []byte(text), err
}

type WindowCoverage struct {
	start, end, observedStart, observedEnd time.Time
	unresolved                             float64
	flow, klines                           MinuteCoverage
	observed                               bool
}

func NewWindowCoverage(start, end, observedStart, observedEnd time.Time, unresolved float64, flow, klines MinuteCoverage) (WindowCoverage, error) {
	value := WindowCoverage{start: start, end: end, observedStart: observedStart, observedEnd: observedEnd, unresolved: unresolved, flow: flow, klines: klines, observed: true}
	if err := value.Validate(); err != nil {
		return WindowCoverage{}, err
	}
	return value, nil
}
func (w WindowCoverage) Validate() error {
	if !w.observed || !w.end.After(w.start) || math.IsNaN(w.unresolved) || math.IsInf(w.unresolved, 0) || w.unresolved < 0 {
		return errors.New("capture window requires actual bounds and finite unresolved time")
	}
	for _, at := range []time.Time{w.start, w.end, w.observedStart, w.observedEnd} {
		if at.Nanosecond()%1000 != 0 {
			return errors.New("capture window bounds exceed the supported microsecond wire resolution")
		}
	}
	lo := pytime.MinuteFloor(w.start)
	if w.start.After(lo) {
		lo = lo.Add(time.Minute)
	}
	hi := pytime.MinuteFloor(w.end)
	if !w.observedStart.Equal(lo) || !w.observedEnd.Equal(hi) || hi.Before(lo) || w.unresolved != lo.Sub(w.start).Seconds()+w.end.Sub(hi).Seconds() {
		return errors.New("capture window observations differ from declared bucket bounds")
	}
	if err := w.flow.Validate(); err != nil {
		return err
	}
	if err := w.klines.Validate(); err != nil {
		return err
	}
	expected := int64(hi.Sub(lo) / time.Minute)
	if w.flow.expected != expected || w.klines.expected != expected {
		return errors.New("capture window minute counts differ from declared interval")
	}
	return nil
}
func (w WindowCoverage) Complete() bool {
	return w.observed && w.flow.Complete() && w.klines.Complete() && w.unresolved == 0
}
func (w WindowCoverage) MarshalJSON() ([]byte, error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Start         string          `json:"start"`
		End           string          `json:"end"`
		ObservedStart string          `json:"observed_bucket_start"`
		ObservedEnd   string          `json:"observed_bucket_end"`
		Precision     string          `json:"timestamp_precision"`
		Unresolved    boundarySeconds `json:"unresolved_boundary_seconds"`
		Flow          MinuteCoverage  `json:"flow"`
		Klines        MinuteCoverage  `json:"klines"`
	}{pytime.ISO(w.start), pytime.ISO(w.end), pytime.ISO(w.observedStart), pytime.ISO(w.observedEnd), "BUCKET", boundarySeconds(w.unresolved), w.flow, w.klines})
}

type CaptureWindows struct {
	one, four, day WindowCoverage
	observed       bool
}

func NewCaptureWindows(one, four, day WindowCoverage) (CaptureWindows, error) {
	value := CaptureWindows{one: one, four: four, day: day, observed: true}
	if err := value.Validate(); err != nil {
		return CaptureWindows{}, err
	}
	return value, nil
}
func (w CaptureWindows) Validate() error {
	if !w.observed {
		return errors.New("capture windows are not an admitted observation")
	}
	for _, window := range []WindowCoverage{w.one, w.four, w.day} {
		if err := window.Validate(); err != nil {
			return err
		}
	}
	if w.one.end.Sub(w.one.start) != time.Hour || w.four.end.Sub(w.four.start) != 4*time.Hour || w.day.end.Sub(w.day.start) != 24*time.Hour || !w.one.end.Equal(w.four.end) || !w.one.end.Equal(w.day.end) {
		return errors.New("capture windows require the declared one four and twenty-four hour origins")
	}
	return nil
}
func (w CaptureWindows) Complete() bool {
	return w.observed && w.one.Complete() && w.four.Complete() && w.day.Complete()
}
func (w CaptureWindows) MarshalJSON() ([]byte, error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		One  WindowCoverage `json:"W1H"`
		Four WindowCoverage `json:"W4H"`
		Day  WindowCoverage `json:"W24H"`
	}{w.one, w.four, w.day})
}

type TradeIdentity struct {
	text    *string
	integer *json.Number
}

func TextTradeIdentity(value string) (TradeIdentity, error) {
	if value == "" || !utf8.ValidString(value) {
		return TradeIdentity{}, errors.New("reference trade identity requires nonempty UTF-8 text")
	}
	return TradeIdentity{text: &value}, nil
}
func IntegerTradeIdentity(value json.Number) (TradeIdentity, error) {
	if _, valid := new(big.Int).SetString(string(value), 10); !valid || !json.Valid([]byte(value)) {
		return TradeIdentity{}, errors.New("reference trade identity requires an integer")
	}
	return TradeIdentity{integer: &value}, nil
}
func (id TradeIdentity) Validate() error {
	if id.text != nil && id.integer == nil && *id.text != "" && utf8.ValidString(*id.text) {
		return nil
	}
	if id.integer != nil && id.text == nil {
		if _, valid := new(big.Int).SetString(string(*id.integer), 10); valid && json.Valid([]byte(*id.integer)) {
			return nil
		}
	}
	return errors.New("reference trade identity is not an admitted observation")
}
func (id TradeIdentity) MarshalJSON() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if id.text != nil && id.integer == nil {
		return json.Marshal(*id.text)
	}
	if id.integer != nil && id.text == nil {
		return []byte(*id.integer), nil
	}
	return nil, errors.New("reference trade identity is not an admitted observation")
}

type Pre24Reference struct {
	available        bool
	identity         *TradeIdentity
	stamp, precision *string
	observed         bool
}

func AvailablePre24Reference(identity TradeIdentity, stamp string, precision *string) (Pre24Reference, error) {
	if err := identity.Validate(); err != nil {
		return Pre24Reference{}, err
	}
	if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
		return Pre24Reference{}, errors.New("reference trade requires an offset-aware timestamp string")
	}
	var capturedPrecision *string
	if precision != nil {
		value := *precision
		capturedPrecision = &value
	}
	value := Pre24Reference{available: true, identity: &identity, stamp: &stamp, precision: capturedPrecision, observed: true}
	if err := value.Validate(); err != nil {
		return Pre24Reference{}, err
	}
	return value, nil
}
func MissingPre24Reference() Pre24Reference { return Pre24Reference{observed: true} }
func (r Pre24Reference) Validate() error {
	if !r.observed {
		return errors.New("pre24 reference is not an admitted observation")
	}
	if !r.available {
		if r.identity != nil || r.stamp != nil || r.precision != nil {
			return errors.New("missing pre24 reference carries unsupported observed facts")
		}
		return nil
	}
	if r.identity == nil || r.stamp == nil {
		return errors.New("available pre24 reference requires actual identity and timestamp")
	}
	if err := r.identity.Validate(); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, *r.stamp); err != nil {
		return errors.New("reference trade requires an offset-aware timestamp string")
	}
	if r.precision != nil && !utf8.ValidString(*r.precision) {
		return errors.New("reference trade precision requires UTF-8 text or null")
	}
	return nil
}
func (r Pre24Reference) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	var reason *string
	if !r.available {
		value := "INCOMPLETE"
		reason = &value
	}
	return json.Marshal(struct {
		Available bool           `json:"available"`
		Identity  *TradeIdentity `json:"trade_id"`
		Stamp     *string        `json:"ts"`
		Precision *string        `json:"ts_prec"`
		Reason    *string        `json:"reason"`
	}{r.available, r.identity, r.stamp, r.precision, reason})
}

type InstrumentCoverage struct {
	id       string
	windows  CaptureWindows
	pre24    *Pre24Reference
	observed bool
}

func NonSpotInstrumentCoverage(id string, windows CaptureWindows) (InstrumentCoverage, error) {
	if id == "" || !utf8.ValidString(id) {
		return InstrumentCoverage{}, errors.New("instrument capture coverage requires an identity")
	}
	if err := windows.Validate(); err != nil {
		return InstrumentCoverage{}, err
	}
	return InstrumentCoverage{id: id, windows: windows, observed: true}, nil
}
func SpotInstrumentCoverage(id string, windows CaptureWindows, pre24 Pre24Reference) (InstrumentCoverage, error) {
	value, err := NonSpotInstrumentCoverage(id, windows)
	if err != nil {
		return InstrumentCoverage{}, err
	}
	if err := pre24.Validate(); err != nil {
		return InstrumentCoverage{}, err
	}
	value.pre24 = &pre24
	if err := value.Validate(); err != nil {
		return InstrumentCoverage{}, err
	}
	return value, nil
}
func (r InstrumentCoverage) Complete() bool {
	return r.observed && r.windows.Complete() && (r.pre24 == nil || r.pre24.available)
}
func (r InstrumentCoverage) Validate() error {
	if !r.observed || r.id == "" || !utf8.ValidString(r.id) {
		return errors.New("instrument capture coverage is not an admitted observation")
	}
	if err := r.windows.Validate(); err != nil {
		return err
	}
	if r.pre24 != nil {
		if err := r.pre24.Validate(); err != nil {
			return err
		}
		if r.pre24.available {
			at, err := time.Parse(time.RFC3339Nano, *r.pre24.stamp)
			if err != nil {
				return err
			}
			if !at.Before(r.windows.day.start) {
				return errors.New("pre24 reference trade must precede its declared boundary")
			}
		}
	}
	return nil
}
func (r InstrumentCoverage) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		ID       string          `json:"instrument_id"`
		Windows  CaptureWindows  `json:"windows"`
		Complete bool            `json:"complete"`
		Pre24    *Pre24Reference `json:"pre24_reference,omitempty"`
	}{r.id, r.windows, r.Complete(), r.pre24})
}

type BenchmarkCoverage struct {
	id          string
	start, end  time.Time
	minutes     MinuteCoverage
	anchorClose bool
	observed    bool
}

func NewBenchmarkCoverage(id string, start, end time.Time, expected, complete int64, anchorClose bool) (BenchmarkCoverage, error) {
	if id == "" || !utf8.ValidString(id) || end.Before(start) || start.Nanosecond()%1000 != 0 || end.Nanosecond()%1000 != 0 || expected != int64(end.Sub(start)/time.Minute) {
		return BenchmarkCoverage{}, errors.New("benchmark capture coverage requires declared interval and counts")
	}
	minutes, err := NewMinuteCoverage(expected, complete)
	if err != nil {
		return BenchmarkCoverage{}, err
	}
	return BenchmarkCoverage{id: id, start: start, end: end, minutes: minutes, anchorClose: anchorClose, observed: true}, nil
}
func (r BenchmarkCoverage) Complete() bool { return r.observed && r.minutes.Complete() }
func (r BenchmarkCoverage) Validate() error {
	if !r.observed || r.id == "" || !utf8.ValidString(r.id) || r.end.Before(r.start) || r.start.Nanosecond()%1000 != 0 || r.end.Nanosecond()%1000 != 0 || r.minutes.expected != int64(r.end.Sub(r.start)/time.Minute) {
		return errors.New("benchmark capture coverage is not an admitted observation")
	}
	return r.minutes.Validate()
}
func (r BenchmarkCoverage) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		ID              string `json:"instrument_id"`
		Start           string `json:"start"`
		End             string `json:"end"`
		Expected        int64  `json:"expected_minutes"`
		CompleteMinutes int64  `json:"complete_minutes"`
		Complete        bool   `json:"complete"`
		AnchorClose     bool   `json:"includes_anchor_close"`
	}{r.id, pytime.ISO(r.start), pytime.ISO(r.end), r.minutes.expected, r.minutes.complete, r.Complete(), r.anchorClose})
}

type Result struct {
	tokenID     string
	anchor      time.Time
	instruments []InstrumentCoverage
	benchmarks  []BenchmarkCoverage
	observed    bool
}

func NewResult(tokenID string, anchor time.Time, instruments []InstrumentCoverage, benchmarks []BenchmarkCoverage) (Result, error) {
	value := Result{tokenID: tokenID, anchor: anchor, instruments: append([]InstrumentCoverage{}, instruments...), benchmarks: append([]BenchmarkCoverage{}, benchmarks...), observed: true}
	if err := value.Validate(); err != nil {
		return Result{}, err
	}
	return value, nil
}
func (r Result) Validate() error {
	if !r.observed || r.tokenID == "" || !utf8.ValidString(r.tokenID) {
		return errors.New("backfill result is not an admitted observation")
	}
	if r.anchor.Nanosecond()%1000 != 0 {
		return errors.New("backfill result anchor exceeds the supported microsecond wire resolution")
	}
	seen := map[string]struct{}{}
	for _, instrument := range r.instruments {
		if !instrument.windows.one.end.Equal(r.anchor) {
			return errors.New("instrument capture belongs to a different recorded anchor")
		}
		if err := instrument.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[instrument.id]; duplicate {
			return errors.New("backfill result repeats an instrument observation")
		}
		seen[instrument.id] = struct{}{}
	}
	seen = map[string]struct{}{}
	for _, benchmark := range r.benchmarks {
		if !benchmark.end.Equal(pytime.MinuteFloor(r.anchor)) || !benchmark.anchorClose || !benchmark.start.Equal(pytime.MinuteFloor(r.anchor.Add(-24*time.Hour)).Add(-time.Minute)) {
			return errors.New("benchmark capture differs from its declared pre24 reference interval")
		}
		if err := benchmark.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[benchmark.id]; duplicate {
			return errors.New("backfill result repeats a benchmark observation")
		}
		seen[benchmark.id] = struct{}{}
	}
	return nil
}
func (r Result) ValidateScope(tokenID string, anchor time.Time) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.tokenID != tokenID || !r.anchor.Equal(anchor) {
		return errors.New("backfill result differs from its claimed token and recorded anchor")
	}
	return nil
}
func (r Result) Ready() (bool, error) {
	if err := r.Validate(); err != nil {
		return false, err
	}
	ready := len(r.instruments) > 0
	for _, instrument := range r.instruments {
		ready = ready && instrument.Complete()
	}
	for _, benchmark := range r.benchmarks {
		ready = ready && benchmark.Complete()
	}
	return ready, nil
}
func (r Result) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Instruments []InstrumentCoverage `json:"instruments"`
		Benchmarks  []BenchmarkCoverage  `json:"benchmark_klines"`
	}{r.instruments, r.benchmarks})
}
