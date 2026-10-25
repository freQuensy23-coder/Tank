package budget

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestInvalidWindowCannotConsumeLedgerCapacity(t *testing.T) {
	for _, window := range []float64{0, -1, math.NaN(), math.Inf(1), math.MaxFloat64} {
		ledger := NewMemoryLedger(func() time.Time { return time.Unix(100, 0) })
		if _, err := ledger.Acquire(context.Background(), "policy", 1, window, 1, "owner", "host"); !errors.Is(err, ErrInvalidBudget) {
			t.Fatalf("window %v accepted: %v", window, err)
		}
		if wait, err := ledger.Acquire(context.Background(), "policy", 1, 60, 1, "owner", "host"); wait != 0 || err != nil {
			t.Fatalf("rejected policy consumed valid capacity: wait=%v err=%v", wait, err)
		}
	}
}

func TestNonfiniteAssignedCapacityCannotBecomeUnboundedAdmission(t *testing.T) {
	for _, capacity := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		ledger := NewMemoryLedger(time.Now)
		if _, err := ledger.Acquire(context.Background(), "policy", capacity, 1, 1, "owner", "host"); !errors.Is(err, ErrInvalidBudget) {
			t.Fatalf("capacity %v accepted: %v", capacity, err)
		}
	}
}
