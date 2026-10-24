package store

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

func (s *Store) Get(ctx context.Context, collection, key string) (map[string]any, error) {
	if !schema.Known(collection) {
		return nil, fmt.Errorf("unknown market collection %s", collection)
	}
	if ClickHouseTables[collection] {
		if err := s.series(collection); err != nil {
			return nil, err
		}
		return s.chGet(ctx, collection, key)
	}
	rows, err := s.Pool.Query(ctx, "SELECT "+selectList(collection, "")+" FROM "+liveTable(collection)+" WHERE storage_key=$1", key)
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows(collection, rows)
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out[0], nil
}

func typedArgument(collection, column, kind string, value any) (any, error) {
	switch kind {
	case "timestamptz":
		t, err := pytime.Parse(value)
		if err != nil {
			return nil, err
		}
		return pgTime(t), nil
	case "jsonb", "text[]":
		out, err := pyjson.Dumps(value, false)
		if err != nil {
			return nil, err
		}
		return string(out), nil
	case "text":
		if s, ok := value.(string); ok {
			return s, nil
		}
		return pyStr(value), nil
	case "numeric":
		switch v := value.(type) {
		case pydec.Dec:
			return v.String(), nil
		case string:
			return v, nil
		}
		return pyStr(value), nil
	case "bigint", "boolean":
		switch v := value.(type) {
		case bool:
			return strconv.FormatBool(v), nil
		}
		return pyStr(value), nil
	}
	return nil, fmt.Errorf("%s.%s: unknown column type %s", collection, column, kind)
}

func (s *Store) Range(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error) {
	if schema.Kind(collection, field) != "timestamptz" {
		return nil, fmt.Errorf("%s.%s is not a typed timestamp column", collection, field)
	}
	if err := s.series(collection); err != nil {
		return nil, err
	}
	return s.chRange(ctx, collection, field, start, end, filters)
}

func (s *Store) Earliest(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) (time.Time, bool, error) {
	if schema.Kind(collection, field) != "timestamptz" {
		return time.Time{}, false, fmt.Errorf("%s.%s is not a typed timestamp column", collection, field)
	}
	if err := s.series(collection); err != nil {
		return time.Time{}, false, err
	}
	return s.chEarliest(ctx, collection, field, start, end, filters)
}

func (s *Store) Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, instrumentIDs []string) ([]map[string]any, error) {
	aliases := schema.Aliases[collection]
	if target, ok := aliases[field]; ok {
		field = target
	}
	if schema.Kind(collection, field) != "timestamptz" {
		return nil, fmt.Errorf("Market projection requires a typed market time field")
	}
	seen := map[string]bool{}
	if len(fields) == 0 || len(fields) > 100 {
		return nil, fmt.Errorf("Projection requires unique fields")
	}
	for _, requested := range fields {
		if seen[requested] {
			return nil, fmt.Errorf("Projection requires unique fields")
		}
		seen[requested] = true
		column := requested
		if target, ok := aliases[requested]; ok {
			column = target
		}
		if column != "storage_key" && schema.Kind(collection, column) == "" {
			return nil, fmt.Errorf("Unknown typed market projection field")
		}
	}
	if instrumentIDs != nil {
		if schema.Kind(collection, "instrument_id") == "" {
			return nil, fmt.Errorf("Collection lacks instrument_id")
		}
		if len(instrumentIDs) == 0 {
			return nil, nil
		}
	}
	if err := s.series(collection); err != nil {
		return nil, err
	}
	return s.chProjection(ctx, collection, fields, field, start, end, instrumentIDs)
}

func (s *Store) LatestTrades(ctx context.Context, ids []string, start time.Time) (map[string]map[string]any, error) {
	out := map[string]map[string]any{}
	if len(ids) == 0 {
		return out, nil
	}
	unique := map[string]bool{}
	var scope []string
	for _, id := range ids {
		if !unique[id] {
			unique[id] = true
			scope = append(scope, id)
		}
	}
	sort.Strings(scope)
	if err := s.series("mkt.flow_bars_1m"); err != nil {
		return nil, err
	}
	return s.chLatestTrades(ctx, scope, start)
}

func (s *Store) LatestRecords(ctx context.Context, collection string, groupBy []string, timeField string, instrumentIDs []string) ([]map[string]any, error) {
	if !schema.Known(collection) || len(groupBy) == 0 {
		return nil, fmt.Errorf("Latest market records require a market collection and typed grouping")
	}
	for _, f := range append(append([]string(nil), groupBy...), timeField) {
		if schema.Kind(collection, f) == "" {
			return nil, fmt.Errorf("Unknown market column %s", f)
		}
	}
	if schema.Kind(collection, timeField) != "timestamptz" {
		return nil, fmt.Errorf("Latest market time_field must be a typed timestamp")
	}
	if instrumentIDs != nil {
		if schema.Kind(collection, "instrument_id") == "" {
			return nil, fmt.Errorf("instrument_ids requires a typed instrument scope")
		}
		unique := map[string]bool{}
		var scope []string
		for _, id := range instrumentIDs {
			if !unique[id] {
				unique[id] = true
				scope = append(scope, id)
			}
		}
		sort.Strings(scope)
		if len(scope) == 0 {
			return nil, nil
		}
		instrumentIDs = scope
	}
	if err := s.series(collection); err != nil {
		return nil, err
	}
	return s.chLatestRecords(ctx, collection, groupBy, timeField, instrumentIDs)
}

func (s *Store) HotTokens(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+selectList("mkt.hot_tokens", "")+" FROM "+liveTable("mkt.hot_tokens")+" ORDER BY storage_key")
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows("mkt.hot_tokens", rows)
	return out, err
}

func (s *Store) BackfillRuns(ctx context.Context, statuses []string) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+selectList("mkt.backfill_runs", "")+" FROM "+liveTable("mkt.backfill_runs")+
		" WHERE status = ANY($1::text[]) ORDER BY started_at NULLS FIRST,storage_key", statuses)
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows("mkt.backfill_runs", rows)
	return out, err
}

const requests = "mkt.capture_requests"

func (s *Store) ClaimRequest(ctx context.Context, writer string, now time.Time) (map[string]any, error) {
	if writer == "" {
		return nil, fmt.Errorf("capture request requires a writer identity")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	rows, err := tx.Query(ctx, "SELECT "+selectList(requests, "")+" FROM "+liveTable(requests)+
		" WHERE status='PENDING' AND deadline <= $1 FOR UPDATE SKIP LOCKED", now)
	if err != nil {
		return nil, err
	}
	expired, _, err := collectRows(requests, rows)
	if err != nil {
		return nil, err
	}
	for _, row := range expired {
		if _, err := s.applyRequestLifecycle(ctx, tx, row, requestExpiry{finished: now}); err != nil {
			return nil, err
		}
	}

	rows, err = tx.Query(ctx, "SELECT "+selectList(requests, "")+" FROM "+liveTable(requests)+
		" WHERE status='PENDING' ORDER BY requested_at NULLS LAST,storage_key LIMIT 1 FOR UPDATE SKIP LOCKED")
	if err != nil {
		return nil, err
	}
	claimed, _, err := collectRows(requests, rows)
	if err != nil {
		return nil, err
	}
	var row map[string]any
	if len(claimed) == 1 {
		row, err = s.applyRequestLifecycle(ctx, tx, claimed[0], requestClaim{writer: writer, started: now})
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return row, nil
}

func (s *Store) FinishRequest(ctx context.Context, row map[string]any) error {
	id, err := requestIdentity(row)
	if err != nil {
		return err
	}
	update, err := decodeRequestCompletion(row)
	if err != nil {
		return err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	original, err := s.lockRequest(ctx, tx, id)
	if err != nil {
		return err
	}
	if _, err := s.applyRequestLifecycle(ctx, tx, original, update); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) PruneFetches(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.Pool.Exec(ctx, "DELETE FROM "+liveTable(requests)+
		" WHERE kind='HTTP_FETCH' AND status IN ('COMPLETE','FAILED','EXPIRED') AND finished_at < $1", before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.Pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&t)
	return t.UTC(), err
}
