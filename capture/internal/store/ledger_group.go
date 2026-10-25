package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

func (l PGLedger) AcquireGroup(ctx context.Context, buckets []budget.Bucket, owner, host string) (wait time.Duration, err error) {
	ordered, err := budget.OrderedBuckets(buckets)
	if err != nil {
		return 0, err
	}
	tx, err := l.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	var h *string
	if host != "" {
		h = &host
	}
	for _, b := range ordered {
		var seconds float64
		err = tx.QueryRow(ctx, "SELECT ops.request_budget_acquire_scope($1::text,$2::numeric,$3::float8,$4::numeric,$5::text,$6::text)",
			b.Key, numeric(b.Capacity), b.WindowS, numeric(b.Weight), owner, h).Scan(&seconds)
		if err != nil {
			return 0, err
		}
		wait = max(wait, time.Duration(seconds*float64(time.Second)))
	}
	if wait > 0 {
		return wait, nil
	}
	return 0, tx.Commit(ctx)
}
