package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Migration = "001_schema.sql"

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	Pool *pgxpool.Pool

	CH *ClickHouse

	url        string
	statements sync.Map
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(databaseURL, "pool_max_conns") {
		config.MaxConns = 8
	}
	if _, ok := config.ConnConfig.RuntimeParams["application_name"]; !ok {
		config.ConnConfig.RuntimeParams["application_name"] = "tank-capture"
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	s := &Store{Pool: pool, url: databaseURL}
	if err := s.requireMigration(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) requireMigration(ctx context.Context) error {
	var found bool
	err := s.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM ops.schema_migrations WHERE version=$1)", Migration).Scan(&found)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		found, err = false, nil
	}
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("database lacks migration %s: run `tank db migrate` before starting tank-capture", Migration)
	}
	return nil
}

func liveTable(collection string) string {
	return pgx.Identifier{"mkt", strings.SplitN(collection, ".", 2)[1]}.Sanitize()
}
