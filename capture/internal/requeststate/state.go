package requeststate

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
)

type Status string

const (
	Complete Status = "COMPLETE"
	Failed   Status = "FAILED"
	Expired  Status = "EXPIRED"
)

type Reason string

const (
	DeadlineInvalid       Reason = "CAPTURE_REQUEST_DEADLINE_INVALID"
	BudgetInvalid         Reason = "CAPTURE_REQUEST_BUDGET_INVALID"
	TokenInvalid          Reason = "CAPTURE_REQUEST_TOKEN_INVALID"
	DeadlinePassed        Reason = "REQUEST_DEADLINE_PASSED"
	Unsupported           Reason = "UNSUPPORTED_CAPTURE_REQUEST"
	RepairFailed          Reason = "REPAIR_FAILED"
	FetchRequestInvalid   Reason = "FETCH_REQUEST_INVALID"
	FetchHostNotAllowed   Reason = "FETCH_HOST_NOT_ALLOWED"
	FetchMethodNotAllowed Reason = "FETCH_METHOD_NOT_ALLOWED"
	FetchUnavailable      Reason = "FETCH_UNAVAILABLE"
	FetchFailed           Reason = "FETCH_FAILED"
	ResponseTooLarge      Reason = "RESPONSE_TOO_LARGE"
	HostBanned            Reason = "HOST_BANNED"
	BudgetExhausted       Reason = "REQUEST_BUDGET_EXHAUSTED"
)

type ValidationError struct {
	Reason Reason
	Detail string
}

func (e *ValidationError) Error() string { return e.Detail }

type Limits struct {
	Deadline time.Time
	Budget   time.Duration
}

func DecodeLimits(deadline *time.Time, budget *string, now time.Time) (Limits, *ValidationError) {
	if deadline == nil {
		return Limits{}, &ValidationError{DeadlineInvalid, "capture request requires a valid explicit deadline"}
	}
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return Limits{deadline.UTC(), remaining}, nil
	}
	if budget != nil {
		seconds, err := pydec.Parse(*budget)
		if err != nil {
			return Limits{}, &ValidationError{BudgetInvalid, "capture request requires a positive finite budget"}
		}
		value := seconds.Float64()
		if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
			return Limits{}, &ValidationError{BudgetInvalid, "capture request requires a positive finite budget"}
		}
		if value < remaining.Seconds() {
			limit := time.Duration(value * float64(time.Second))
			if limit <= 0 {
				return Limits{}, &ValidationError{BudgetInvalid, "capture request budget is below timer resolution"}
			}
			remaining = limit
		}
	}
	return Limits{deadline.UTC(), remaining}, nil
}

type Claimed struct {
	ID        string
	Writer    string
	StartedAt time.Time
	Kind      *string
	TokenID   *string
	Start     *time.Time
	End       *time.Time
	Deadline  *time.Time
	BudgetS   *string
	Request   json.RawMessage
}

type Result interface{ captureResult() }
type Success interface {
	Result
	captureSuccess()
}

type RepairCoverage struct {
	IncompleteBefore int64 `json:"incomplete_before"`
	IncompleteAfter  int64 `json:"incomplete_after"`
}
type RepairResult struct {
	Instruments map[string]RepairCoverage `json:"instruments"`
}

func (RepairResult) captureResult()  {}
func (RepairResult) captureSuccess() {}

type FetchResult struct {
	StatusCode  int     `json:"status_code"`
	ContentType string  `json:"content_type"`
	SHA256      string  `json:"sha256"`
	FetchedAt   string  `json:"fetched_at"`
	Bytes       int64   `json:"bytes"`
	SourceID    *string `json:"source_id"`
	Body        *string `json:"body,omitempty"`
	BodyB64     *string `json:"body_b64,omitempty"`
}

func (FetchResult) captureResult()  {}
func (FetchResult) captureSuccess() {}

type FetchFailure struct {
	Error    string  `json:"error"`
	SourceID *string `json:"source_id"`
}

func (FetchFailure) captureResult() {}

type RejectedURL struct {
	URL string `json:"url"`
}

func (RejectedURL) captureResult() {}

type RejectedMethod struct {
	Method string `json:"method"`
}

func (RejectedMethod) captureResult() {}

type Outcome struct {
	status Status
	reason *Reason
	result Result
}

func Completed(result Success) Outcome { return Outcome{status: Complete, result: result} }
func Failure(reason Reason, result Result) Outcome {
	return Outcome{status: Failed, reason: &reason, result: result}
}
func Expiry() Outcome             { reason := DeadlinePassed; return Outcome{status: Expired, reason: &reason} }
func (o Outcome) Status() Status  { return o.status }
func (o Outcome) Reason() *Reason { return o.reason }
func (o Outcome) Result() Result  { return o.result }
func (o Outcome) validate() error {
	switch o.status {
	case Complete:
		if o.reason != nil || o.result == nil {
			return errors.New("complete capture request requires a result and no reason")
		}
		if _, ok := o.result.(Success); !ok {
			return errors.New("complete capture request requires a successful result")
		}
	case Expired:
		if o.reason == nil || *o.reason != DeadlinePassed || o.result != nil {
			return errors.New("expired capture request requires deadline evidence and no result")
		}
	case Failed:
		if o.reason == nil || *o.reason == "" {
			return errors.New("failed or expired capture request requires a reason")
		}
	default:
		return errors.New("capture request requires a terminal status")
	}
	switch result := o.result.(type) {
	case FetchResult:
		if o.status != Complete {
			return errors.New("fetch success result requires complete status")
		}
		if result.StatusCode < 100 || result.StatusCode > 599 || result.Bytes < 0 || (result.Body == nil) == (result.BodyB64 == nil) {
			return errors.New("capture fetch result has invalid status or body representation")
		}
		var body []byte
		if result.Body != nil {
			body = []byte(*result.Body)
		}
		digest, err := hex.DecodeString(result.SHA256)
		if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != result.SHA256 {
			return errors.New("capture fetch result requires a SHA256 digest")
		}
		if _, err := time.Parse(time.RFC3339Nano, result.FetchedAt); err != nil {
			return errors.New("capture fetch result requires a collection timestamp")
		}
		if result.Body != nil && int64(len(*result.Body)) != result.Bytes {
			return errors.New("capture fetch body length differs from its byte evidence")
		}
		if result.BodyB64 != nil {
			decoded, err := base64.StdEncoding.Strict().DecodeString(*result.BodyB64)
			if err != nil || int64(len(decoded)) != result.Bytes {
				return errors.New("capture fetch base64 body differs from its byte evidence")
			}
			body = decoded
		}
		sum := sha256.Sum256(body)
		if !bytes.Equal(digest, sum[:]) {
			return errors.New("capture fetch digest differs from its actual body")
		}
	case RepairResult:
		if result.Instruments == nil {
			return errors.New("repair result requires an observed instrument map")
		}
		for id, coverage := range result.Instruments {
			if id == "" || coverage.IncompleteBefore < 0 || coverage.IncompleteAfter < 0 {
				return errors.New("repair result contains invalid observed coverage")
			}
		}
	case FetchFailure, RejectedURL, RejectedMethod:
		if o.status != Failed {
			return errors.New("fetch refusal result requires failed status")
		}
	case nil:
	default:
		return errors.New("unsupported capture request result representation")

	}
	return nil
}
func (o Outcome) JSONResult() (json.RawMessage, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if o.result == nil {
		return nil, nil
	}
	return json.Marshal(o.result)
}

type Completion struct {
	ID         string
	Outcome    Outcome
	FinishedAt time.Time
}

func (c Completion) Validate() error {
	if c.ID == "" {
		return errors.New("capture request without request_id")
	}
	if c.FinishedAt.IsZero() {
		return errors.New("terminal capture request requires an explicit collection finish timestamp")
	}
	return c.Outcome.validate()
}
