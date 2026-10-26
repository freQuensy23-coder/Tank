package backfillstate

import (
	"encoding/json"
	"errors"
	"unicode/utf8"
)

type Outcome interface {
	backfillOutcome()
	Validate() error
	Status() string
	Reason() *string
}

type CaptureOutcome struct {
	result   Result
	complete bool
}

func Captured(result Result) (CaptureOutcome, error) {
	complete, err := result.Ready()
	if err != nil {
		return CaptureOutcome{}, err
	}
	value := CaptureOutcome{result: result, complete: complete}
	if err := value.Validate(); err != nil {
		return CaptureOutcome{}, err
	}
	return value, nil
}

func (CaptureOutcome) backfillOutcome() {}
func (c CaptureOutcome) Validate() error {
	complete, err := c.result.Ready()
	if err != nil {
		return err
	}
	if c.complete != complete {
		return errors.New("backfill terminal status differs from actual captured coverage")
	}
	return nil
}
func (c CaptureOutcome) Result() Result { return c.result }
func (c CaptureOutcome) Status() string {
	if c.complete {
		return "COMPLETE"
	}
	return "PARTIAL"
}
func (c CaptureOutcome) Reason() *string {
	if c.complete {
		return nil
	}
	reason := "INCOMPLETE_VENUE_WINDOWS_EXCLUDED"
	return &reason
}
func (c CaptureOutcome) JSONResult() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(c.result)
}

type FailureKind uint8

const (
	BenchmarkCaptureFailed FailureKind = iota + 1
	RepairFailed
	CoverageReadFailed
	WorkerPanicked
	CommandRefused
)

type Failure struct {
	kind    FailureKind
	detail  string
	refusal *Refused
}

func Failed(kind FailureKind) (Failure, error) {
	value := Failure{kind: kind}
	if err := value.Validate(); err != nil {
		return Failure{}, err
	}
	return value, nil
}

func Panicked(observed string) (Failure, error) {
	value := Failure{kind: WorkerPanicked, detail: observed}
	if err := value.Validate(); err != nil {
		return Failure{}, err
	}
	return value, nil
}

func RefusalOutcome(refusal Refused) (Failure, error) {
	if err := refusal.Validate(); err != nil {
		return Failure{}, err
	}
	value := Failure{kind: CommandRefused, refusal: &refusal}
	if err := value.Validate(); err != nil {
		return Failure{}, err
	}
	return value, nil
}

func (Failure) backfillOutcome() {}
func (f Failure) Status() string { return "FAILED" }
func (f Failure) Validate() error {
	switch f.kind {
	case BenchmarkCaptureFailed, RepairFailed, CoverageReadFailed:
		if f.detail != "" || f.refusal != nil {
			return errors.New("backfill fixed failure cannot carry another failure's detail")
		}
	case WorkerPanicked:
		if f.detail == "" || !utf8.ValidString(f.detail) || f.refusal != nil {
			return errors.New("backfill failure requires its actual UTF-8 panic evidence")
		}
	case CommandRefused:
		if f.detail != "" || f.refusal == nil {
			return errors.New("backfill refusal requires its admitted command refusal")
		}
		return f.refusal.Validate()
	default:
		return errors.New("backfill failure kind is not supported")
	}
	return nil
}
func (f Failure) Reason() *string {
	var reason string
	switch f.kind {
	case BenchmarkCaptureFailed:
		reason = "BENCHMARK_CAPTURE_FAILED"
	case RepairFailed:
		reason = "REPAIR_FAILED"
	case CoverageReadFailed:
		reason = "COVERAGE_READ_FAILED"
	case WorkerPanicked:
		reason = f.detail
	case CommandRefused:
		if f.refusal != nil {
			reason = f.refusal.Reason()
		}
	}
	return &reason
}
