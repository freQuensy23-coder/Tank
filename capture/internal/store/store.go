package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Migration = "046_capture_service.sql"

var Written = []string{
	"mkt.flow_bars_1m", "mkt.klines_1m", "mkt.oi_obs", "mkt.funding_obs", "mkt.liq_obs",
	"mkt.book_summaries", "mkt.tickers_24h", "mkt.capture_gaps", "mkt.capture_streams",
	"mkt.backfill_runs",
}

var written = func() map[string]bool {
	m := map[string]bool{}
	for _, c := range Written {
		m[c] = true
	}
	return m
}()

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	Pool   *pgxpool.Pool
	Schema string

	url        string
	statements sync.Map
}

func Open(ctx context.Context, databaseURL, schema string) (*Store, error) {
	if !schemaName.MatchString(schema) {
		return nil, fmt.Errorf("invalid schema name %q", schema)
	}
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
	s := &Store{Pool: pool, Schema: schema, url: databaseURL}
	if err := s.requireMigration(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if schema != "mkt" {
		if err := s.ensureShadow(ctx); err != nil {
			pool.Close()
			return nil, err
		}
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

func (s *Store) ensureShadow(ctx context.Context) error {
	shadow := quote(s.Schema)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('tank:capture:shadow_schema:' || $1, 0))", s.Schema); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+shadow); err != nil {
		return err
	}
	for _, collection := range Written {
		name := strings.SplitN(collection, ".", 2)[1]
		live := pgx.Identifier{"mkt", name}.Sanitize()
		copy := pgx.Identifier{s.Schema, name}.Sanitize()
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+copy+" (LIKE "+live+" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"); err != nil {
			return fmt.Errorf("shadow %s: %w", collection, err)
		}
		rows, err := tx.Query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
			WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
			AND a.attname NOT IN (SELECT attname FROM pg_attribute WHERE attrelid = $2::regclass AND attnum > 0 AND NOT attisdropped)
			ORDER BY a.attnum`, "mkt."+name, s.Schema+"."+name)
		if err != nil {
			return err
		}
		var missing [][2]string
		for rows.Next() {
			var column, kind string
			if err := rows.Scan(&column, &kind); err != nil {
				rows.Close()
				return err
			}
			missing = append(missing, [2]string{column, kind})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, m := range missing {
			if _, err := tx.Exec(ctx, "ALTER TABLE "+copy+" ADD COLUMN IF NOT EXISTS "+quote(m[0])+" "+m[1]); err != nil {
				return fmt.Errorf("shadow %s.%s: %w", collection, m[0], err)
			}
		}
	}
	sequence := pgx.Identifier{s.Schema, "liq_obs_id_seq"}.Sanitize()
	if _, err := tx.Exec(ctx, "CREATE SEQUENCE IF NOT EXISTS "+sequence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "ALTER TABLE "+pgx.Identifier{s.Schema, "liq_obs"}.Sanitize()+" ALTER COLUMN id SET DEFAULT nextval('"+s.Schema+".liq_obs_id_seq')"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) schemaOf(collection string) string {
	if written[collection] {
		return s.Schema
	}
	return "mkt"
}

func (s *Store) Table(collection string) string {
	name := strings.SplitN(collection, ".", 2)[1]
	return pgx.Identifier{s.schemaOf(collection), name}.Sanitize()
}

func liveTable(collection string) string {
	return pgx.Identifier{"mkt", strings.SplitN(collection, ".", 2)[1]}.Sanitize()
}
