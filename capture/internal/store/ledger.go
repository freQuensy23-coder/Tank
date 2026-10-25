package store

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

type PGLedger struct{ Pool *pgxpool.Pool }

func (s *Store) Ledger() PGLedger { return PGLedger{Pool: s.Pool} }

func numeric(x float64) string { return strconv.FormatFloat(x, 'f', -1, 64) }

func (l PGLedger) Acquire(ctx context.Context, scope string, capacity, windowS, weight float64, owner, host string) (time.Duration, error) {
	return l.AcquireGroup(ctx, []budget.Bucket{{Key: scope, Capacity: capacity, WindowS: windowS, Weight: weight}}, owner, host)
}

func (l PGLedger) Block(ctx context.Context, host string, until time.Time, reason, owner string) error {
	_, err := l.Pool.Exec(ctx, "SELECT ops.request_budget_block($1::text,$2::timestamptz,$3::text,$4::text)", host, until, reason, owner)
	return err
}
