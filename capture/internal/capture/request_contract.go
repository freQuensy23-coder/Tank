package capture

import (
	"fmt"
	"math"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

type requestFailureReason string

const (
	invalidRequestDeadline requestFailureReason = "CAPTURE_REQUEST_DEADLINE_INVALID"
	invalidRequestBudget   requestFailureReason = "CAPTURE_REQUEST_BUDGET_INVALID"
	invalidRequestToken    requestFailureReason = "CAPTURE_REQUEST_TOKEN_INVALID"
)

type requestValidationError struct {
	reason requestFailureReason
	detail string
}

func (e *requestValidationError) Error() string { return e.detail }

type requestLimits struct {
	deadline time.Time
	budget   time.Duration
}

func requestTimestamp(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return time.Time{}, err
		}
		return parsed.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("capture request timestamp must be RFC3339 or time.Time, got %T", value)
	}
}

func decodeRequestLimits(row map[string]any, now time.Time) (requestLimits, *requestValidationError) {
	deadline, err := requestTimestamp(row["deadline"])
	if err != nil {
		return requestLimits{}, &requestValidationError{invalidRequestDeadline, "capture request requires a valid explicit deadline"}
	}
	budget := deadline.Sub(now)
	if budget <= 0 {
		return requestLimits{deadline, budget}, nil
	}
	if raw := row["budget_s"]; raw != nil {
		seconds, err := pyval.Decimal(raw)
		if err != nil {
			return requestLimits{}, &requestValidationError{invalidRequestBudget, "capture request requires a positive finite budget"}
		}
		value := seconds.Float64()
		if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
			return requestLimits{}, &requestValidationError{invalidRequestBudget, "capture request requires a positive finite budget"}
		}
		if value < budget.Seconds() {
			limit := time.Duration(value * float64(time.Second))
			if limit <= 0 {
				return requestLimits{}, &requestValidationError{invalidRequestBudget, "capture request budget is below timer resolution"}
			}
			budget = limit
		}
	}
	return requestLimits{deadline, budget}, nil
}
