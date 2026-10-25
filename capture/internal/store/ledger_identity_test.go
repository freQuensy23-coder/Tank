package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

func TestCapacityTransitionCoalescesHistoricalAndLegacyClientDebits(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	owner := "identity-" + suffix()
	scope := owner + "|1.0|2.0"
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(ctx, "DELETE FROM ops.request_budget_ledger WHERE owner=$1", owner); err != nil {
			t.Error(err)
		}
	})
	for _, legacy := range []string{scope + "|60.0|2.0", scope + "|60.0|5.0"} {
		if _, err := s.Pool.Exec(ctx, "INSERT INTO ops.request_budget_ledger(bucket,at,weight,owner) VALUES($1,clock_timestamp(),1,$2)", legacy, owner); err != nil {
			t.Fatal(err)
		}
	}
	ledger := s.Ledger()
	if wait, err := ledger.Acquire(ctx, scope, 1, 60, 1, owner, ""); err != nil || wait <= 0 || wait > time.Minute {
		t.Fatalf("new capacity lost historical debits:wait=%v err=%v", wait, err)
	}
	sibling := budget.Bucket{Key: owner + "-sibling", Capacity: 1, WindowS: 1, Weight: 1}
	reduced := budget.Bucket{Key: scope, Capacity: 1, WindowS: 60, Weight: 1}
	if wait, err := ledger.AcquireGroup(ctx, []budget.Bucket{sibling, reduced}, owner, ""); err != nil || wait <= 0 {
		t.Fatalf("reduced-cap group reset historical spend:wait=%v err=%v", wait, err)
	}
	var siblingSpend float64
	if err := s.Pool.QueryRow(ctx, "SELECT COALESCE(sum(weight),0)::float8 FROM ops.request_budget_ledger WHERE bucket=ops.request_budget_scope_key($1::text,$2::float8)", sibling.Key, sibling.WindowS).Scan(&siblingSpend); err != nil {
		t.Fatal(err)
	}
	if siblingSpend != 0 {
		t.Fatalf("capacity transition partially spent another constraint:%v", siblingSpend)
	}
	var wait float64
	if err := s.Pool.QueryRow(ctx, "SELECT ops.request_budget_acquire($1::text,3::numeric,60::float8,1::numeric,$2::text,NULL::text)", scope+"|60.0|3.0", owner).Scan(&wait); err != nil || wait != 0 {
		t.Fatalf("legacy cap3 should admit only third total debit:wait=%v err=%v", wait, err)
	}
	if wait, err := ledger.Acquire(ctx, scope, 3, 60, 1, owner, ""); err != nil || wait <= 0 {
		t.Fatalf("new client ignored old client debit:wait=%v err=%v", wait, err)
	}
	var total float64
	var count int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*)::int,COALESCE(sum(weight),0)::float8 FROM ops.request_budget_ledger WHERE owner=$1", owner).Scan(&count, &total); err != nil {
		t.Fatal(err)
	}
	if count != 3 || total != 3 {
		t.Fatalf("historical debit evidence changed:count=%d weight=%v", count, total)
	}
}

type identityAdmission struct {
	wait time.Duration
	err  error
}

func TestLegacyAndRawScopeClientsSerializeOneSharedCapacity(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	owner := "identity-race-" + suffix()
	scope := owner + "|60.0|1.0"
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(ctx, "DELETE FROM ops.request_budget_ledger WHERE owner=$1", owner); err != nil {
			t.Error(err)
		}
	})
	const clients = 12
	start := make(chan struct{})
	results := make(chan identityAdmission, clients)
	var workers sync.WaitGroup
	for i := range clients {
		workers.Go(func() {
			<-start
			if i%2 == 0 {
				wait, err := s.Ledger().Acquire(ctx, scope, 1, 60, 1, owner, "")
				results <- identityAdmission{wait, err}
				return
			}
			var seconds float64
			err := s.Pool.QueryRow(ctx, "SELECT ops.request_budget_acquire($1::text,1::numeric,60::float8,1::numeric,$2::text,NULL::text)", scope+"|60.0|1.0", owner).Scan(&seconds)
			results <- identityAdmission{time.Duration(seconds * float64(time.Second)), err}
		})
	}
	close(start)
	workers.Wait()
	close(results)
	admitted := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.wait == 0 {
			admitted++
		} else if result.wait < 0 || result.wait > time.Minute {
			t.Fatalf("invalid shared wait:%v", result.wait)
		}
	}
	if admitted != 1 {
		t.Fatalf("mixed clients bypassed shared lock/history:admitted=%d", admitted)
	}
}
