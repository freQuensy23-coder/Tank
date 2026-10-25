package budget

import (
	"context"
	"testing"
	"time"
)

func TestCapacityChangesPreserveDebitsAndAtomicGroup(t *testing.T) {
	now := time.Unix(100, 0)
	ledger := NewMemoryLedger(func() time.Time { return now })
	ctx := context.Background()
	scope := "shared|scope"
	for range 2 {
		if wait, err := ledger.Acquire(ctx, scope, 2, 60, 1, "old-cap", ""); wait != 0 || err != nil {
			t.Fatalf("initial admission:wait=%v err=%v", wait, err)
		}
	}
	reduced := Bucket{Key: scope, Capacity: 1, WindowS: 60, Weight: 1}
	sibling := Bucket{Key: "other-window", Capacity: 1, WindowS: 1, Weight: 1}
	if wait, err := ledger.AcquireGroup(ctx, []Bucket{sibling, reduced}, "new-cap", ""); wait != time.Minute || err != nil {
		t.Fatalf("capacity reduction reset spend:wait=%v err=%v", wait, err)
	}
	if used := ledger.Used(sibling.Name(), sibling.WindowS); used != 0 {
		t.Fatalf("blocked capacity transition spent sibling:%v", used)
	}
	if wait, err := ledger.Acquire(ctx, scope, 3, 60, 1, "higher-cap", ""); wait != 0 || err != nil {
		t.Fatalf("third total debit should fit explicit cap3:wait=%v err=%v", wait, err)
	}
	if wait, err := ledger.Acquire(ctx, scope, 3, 60, 1, "higher-cap", ""); wait != time.Minute || err != nil {
		t.Fatalf("higher cap reset prior history:wait=%v err=%v", wait, err)
	}
	now = now.Add(time.Minute)
	if wait, err := ledger.Acquire(ctx, scope, 1, 60, 1, "new-cap", ""); wait != 0 || err != nil {
		t.Fatalf("expired history did not release capacity:wait=%v err=%v", wait, err)
	}
}

func TestMixedCapacityDuplicateGroupIsRejectedBeforeEffects(t *testing.T) {
	ledger := NewMemoryLedger(time.Now)
	first := Bucket{Key: "shared", Capacity: 1, WindowS: 60, Weight: 1}
	second := first
	second.Capacity = 2
	if _, err := ledger.AcquireGroup(context.Background(), []Bucket{first, second}, "owner", ""); err == nil {
		t.Fatal("same scope/window was accepted as two constraints")
	}
	if used := ledger.Used(first.Name(), first.WindowS); used != 0 {
		t.Fatalf("invalid group spent capacity:%v", used)
	}
}

func TestDifferentDeclaredPeriodsRemainIndependent(t *testing.T) {
	ledger := NewMemoryLedger(time.Now)
	ctx := context.Background()
	if wait, err := ledger.Acquire(ctx, "scope", 1, 1, 1, "owner", ""); wait != 0 || err != nil {
		t.Fatalf("short window:wait=%v err=%v", wait, err)
	}
	if wait, err := ledger.Acquire(ctx, "scope", 1, 60, 1, "owner", ""); wait != 0 || err != nil {
		t.Fatalf("distinct declared period was merged:wait=%v err=%v", wait, err)
	}
}
