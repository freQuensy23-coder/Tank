package store

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PGLedger struct{ Pool *pgxpool.Pool }

func (s *Store) Ledger() PGLedger { return PGLedger{Pool: s.Pool} }

func numeric(x float64) string { return strconv.FormatFloat(x, 'f', -1, 64) }

func (l PGLedger) Acquire(ctx context.Context, bucket string, capacity, windowS, weight float64, owner, host string) (time.Duration, error) {
	var h any
	if host != "" {
		h = host
	}
	var wait float64
	err := l.Pool.QueryRow(ctx, "SELECT ops.request_budget_acquire($1::text,$2::numeric,$3::float8,$4::numeric,$5::text,$6::text)",
		bucket, numeric(capacity), windowS, numeric(weight), owner, h).Scan(&wait)
	if err != nil {
		return 0, err
	}
	return time.Duration(wait * float64(time.Second)), nil
}

func (l PGLedger) Block(ctx context.Context, host string, until time.Time, reason, owner string) error {
	_, err := l.Pool.Exec(ctx, "SELECT ops.request_budget_block($1::text,$2::timestamptz,$3::text,$4::text)", host, until, reason, owner)
	return err
}
