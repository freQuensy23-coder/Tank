package budget

import (
	"context"
	"testing"
	"time"
)

func TestGroupWaitDoesNotSpendOtherWindow(t *testing.T) {
	now := time.Unix(100, 0)
	ledger := NewMemoryLedger(func() time.Time { return now })
	short := Bucket{Key: "short", Capacity: 1, WindowS: 1, Weight: 1}
	daily := Bucket{Key: "daily", Capacity: 1, WindowS: 86400, Weight: 1}
	if wait, err := ledger.AcquireGroup(context.Background(), []Bucket{daily}, "first", "host"); wait != 0 || err != nil {
		t.Fatalf("seed: wait=%v err=%v", wait, err)
	}
	if wait, err := ledger.AcquireGroup(context.Background(), []Bucket{short, daily}, "second", "host"); wait != 24*time.Hour || err != nil {
		t.Fatalf("blocked group: wait=%v err=%v", wait, err)
	}
	if used := ledger.Used(short.Name(), short.WindowS); used != 0 {
		t.Fatalf("short window spent while daily window blocked: %v", used)
	}
	now = now.Add(24 * time.Hour)
	if wait, err := ledger.AcquireGroup(context.Background(), []Bucket{daily, short}, "second", "host"); wait != 0 || err != nil {
		t.Fatalf("retry: wait=%v err=%v", wait, err)
	}
	for _, b := range []Bucket{daily, short} {
		if used := ledger.Used(b.Name(), b.WindowS); used != 1 {
			t.Fatalf("group failed to spend %s: %v", b.Key, used)
		}
	}
}

func TestInvalidGroupCannotPartiallySpend(t *testing.T) {
	b := Bucket{Key: "valid", Capacity: 1, WindowS: 1, Weight: 1}
	for _, group := range [][]Bucket{{b, b}, {b, {Key: "invalid"}}, {}} {
		ledger := NewMemoryLedger(time.Now)
		if _, err := ledger.AcquireGroup(context.Background(), group, "owner", "host"); err == nil {
			t.Fatal("invalid group accepted")
		}
		if ledger.Used(b.Name(), b.WindowS) != 0 {
			t.Fatal("invalid group consumed capacity")
		}
	}
}
