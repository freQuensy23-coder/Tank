package store

import (
	"context"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

func TestBudgetGroupRollbackPreservesOtherWindowCapacity(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	key := "group-" + suffix()
	short := budget.Bucket{Key: key + "-a", Capacity: 1, WindowS: 1, Weight: 1}
	daily := budget.Bucket{Key: key + "-z", Capacity: 1, WindowS: 86400, Weight: 1}
	var shortName, dailyName string
	if err := s.Pool.QueryRow(ctx, "SELECT ops.request_budget_scope_key($1::text,$2::float8),ops.request_budget_scope_key($3::text,$4::float8)", short.Key, short.WindowS, daily.Key, daily.WindowS).Scan(&shortName, &dailyName); err != nil {
		t.Fatal(err)
	}
	names := []string{shortName, dailyName}
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(ctx, "DELETE FROM ops.request_budget_ledger WHERE bucket=ANY($1::text[])", names); err != nil {
			t.Error(err)
		}
	})
	ledger := s.Ledger()
	if wait, err := ledger.AcquireGroup(ctx, []budget.Bucket{daily}, "seed", ""); wait != 0 || err != nil {
		t.Fatalf("seed: wait=%v err=%v", wait, err)
	}
	if wait, err := ledger.AcquireGroup(ctx, []budget.Bucket{short, daily}, "group", ""); wait <= 0 || err != nil {
		t.Fatalf("blocked group: wait=%v err=%v", wait, err)
	}
	var used float64
	if err := s.Pool.QueryRow(ctx, "SELECT COALESCE(sum(weight),0)::float8 FROM ops.request_budget_ledger WHERE bucket=$1", shortName).Scan(&used); err != nil {
		t.Fatal(err)
	}
	if used != 0 {
		t.Fatalf("rolled back group spent independent window: %v", used)
	}
	if wait, err := ledger.AcquireGroup(ctx, []budget.Bucket{short}, "independent", ""); wait != 0 || err != nil {
		t.Fatalf("independent request lost capacity: wait=%v err=%v", wait, err)
	}
}
