package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/jackc/pgx/v5"
)

// Get is repo.get for a typed collection: the decoded row by storage key, nil when
// absent.
func (s *Store) Get(ctx context.Context, collection, key string) (map[string]any, error) {
	if !schema.Known(collection) {
		return nil, fmt.Errorf("unknown market collection %s", collection)
	}
	rows, err := s.Pool.Query(ctx, "SELECT "+selectList(collection, "")+" FROM "+s.Table(collection)+" WHERE storage_key=$1", key)
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows(collection, rows)
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out[0], nil
}

// typedArgument renders a filter value for "column = $n::kind".
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

// exactPredicate is tank.storage._exact_predicate for a typed collection: aliases
// resolved, None as IS NULL, equality on the typed column otherwise. Placeholders
// start after the existing args.
func exactPredicate(collection string, filters map[string]any, args []any) ([]string, []any, error) {
	names := make([]string, 0, len(filters))
	for k := range filters {
		names = append(names, k)
	}
	sort.Strings(names)
	var clauses []string
	for _, name := range names {
		value := filters[name]
		column := name
		if target, ok := schema.Aliases[collection][name]; ok {
			column = target
		}
		kind := schema.Kind(collection, column)
		if kind == "" {
			return nil, nil, fmt.Errorf("Unknown typed market filter %s.%s", collection, name)
		}
		if isNone(value) {
			clauses = append(clauses, quote(column)+" IS NULL")
			continue
		}
		arg, err := typedArgument(collection, column, kind, value)
		if err != nil {
			return nil, nil, err
		}
		args = append(args, arg)
		clauses = append(clauses, quote(column)+" = $"+strconv.Itoa(len(args))+"::"+kind)
	}
	return clauses, args, nil
}

func bound(clauses []string, args []any, column, operator string, t time.Time) ([]string, []any) {
	if t.IsZero() {
		return clauses, args
	}
	args = append(args, t)
	return append(clauses, column+" "+operator+" $"+strconv.Itoa(len(args))), args
}

// Range is repo.iter_range for a typed collection: rows with start <= field < end
// (a zero time is unbounded) and the exact filters, by (field, storage_key).
func (s *Store) Range(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error) {
	if schema.Kind(collection, field) != "timestamptz" {
		return nil, fmt.Errorf("%s.%s is not a typed timestamp column", collection, field)
	}
	column := quote(field)
	clauses := []string{column + " IS NOT NULL"}
	var args []any
	clauses, args = bound(clauses, args, column, ">=", start)
	clauses, args = bound(clauses, args, column, "<", end)
	extra, args, err := exactPredicate(collection, filters, args)
	if err != nil {
		return nil, err
	}
	clauses = append(clauses, extra...)
	query := "SELECT " + selectList(collection, "") + " FROM " + s.Table(collection) + " WHERE " + strings.Join(clauses, " AND ") + " ORDER BY " + column + ",storage_key"
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows(collection, rows)
	return out, err
}

// Earliest is the smallest field value of the rows with start <= field < end matching
// filters (the zero time and false when there is none): one aggregate, no rows read.
func (s *Store) Earliest(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) (time.Time, bool, error) {
	if schema.Kind(collection, field) != "timestamptz" {
		return time.Time{}, false, fmt.Errorf("%s.%s is not a typed timestamp column", collection, field)
	}
	column := quote(field)
	clauses := []string{column + " IS NOT NULL"}
	var args []any
	clauses, args = bound(clauses, args, column, ">=", start)
	clauses, args = bound(clauses, args, column, "<", end)
	extra, args, err := exactPredicate(collection, filters, args)
	if err != nil {
		return time.Time{}, false, err
	}
	clauses = append(clauses, extra...)
	var first *time.Time
	if err := s.Pool.QueryRow(ctx, "SELECT min("+column+") FROM "+s.Table(collection)+" WHERE "+strings.Join(clauses, " AND "), args...).Scan(&first); err != nil {
		return time.Time{}, false, err
	}
	if first == nil {
		return time.Time{}, false, nil
	}
	return first.UTC(), true, nil
}

// Projection is iter_market_projection: the requested columns (aliases and storage_key
// allowed) of rows with start <= field < end, by (field, storage_key). A nil
// instrumentIDs is unscoped; an empty one matches nothing.
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
	selected := make([]string, len(fields))
	expressions := make([]string, len(fields))
	for i, requested := range fields {
		if seen[requested] {
			return nil, fmt.Errorf("Projection requires unique fields")
		}
		seen[requested] = true
		column := requested
		if target, ok := aliases[requested]; ok {
			column = target
		}
		kind := "text"
		if column != "storage_key" {
			if kind = schema.Kind(collection, column); kind == "" {
				return nil, fmt.Errorf("Unknown typed market projection field")
			}
		}
		selected[i] = kind
		expr := quote(column)
		switch kind {
		case "numeric", "jsonb":
			expr += "::text"
		case "text[]":
			expr = "array_to_json(" + expr + ")::text"
		}
		expressions[i] = expr + " AS " + quote("p"+strconv.Itoa(i))
	}
	if instrumentIDs != nil {
		if schema.Kind(collection, "instrument_id") == "" {
			return nil, fmt.Errorf("Collection lacks instrument_id")
		}
		if len(instrumentIDs) == 0 {
			return nil, nil
		}
	}
	column := quote(field)
	clauses := []string{column + " IS NOT NULL"}
	var args []any
	clauses, args = bound(clauses, args, column, ">=", start)
	clauses, args = bound(clauses, args, column, "<", end)
	if instrumentIDs != nil {
		args = append(args, instrumentIDs)
		clauses = append(clauses, "instrument_id=ANY($"+strconv.Itoa(len(args))+"::text[])")
	}
	query := "SELECT " + strings.Join(expressions, ",") + " FROM " + s.Table(collection) + " WHERE " + strings.Join(clauses, " AND ") + " ORDER BY " + column + ",storage_key"
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]any, len(fields))
		for i, requested := range fields {
			v, err := decodeValue(selected[i], values[i])
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", collection, requested, err)
			}
			row[requested] = v
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// LatestTrades is latest_market_trades: the last_trade of the newest flow bar holding a
// print at or after start, per instrument (the capture cursor a restart resumes from).
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
	query := "SELECT point.instrument_id, point.last_trade::text FROM unnest($1::text[]) AS scope(instrument_id) " +
		"CROSS JOIN LATERAL (SELECT instrument_id, last_trade FROM " + s.Table("mkt.flow_bars_1m") + " " +
		"WHERE instrument_id = scope.instrument_id AND minute_start >= $2 AND last_trade IS NOT NULL " +
		"ORDER BY minute_start DESC LIMIT 1) AS point"
	rows, err := s.Pool.Query(ctx, query, scope, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var iid, text string
		if err := rows.Scan(&iid, &text); err != nil {
			return nil, err
		}
		value, err := loadJSON(text)
		if err != nil {
			return nil, err
		}
		if m, ok := value.(map[string]any); ok {
			out[iid] = m
		}
	}
	return out, rows.Err()
}

// LatestRecords is latest_market_records without bounds: the newest point per group
// (ties by storage key, newest first), optionally for some instruments only.
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
	var query string
	var args []any
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
	groups := make([]string, len(groupBy))
	for i, g := range groupBy {
		groups[i] = quote(g)
	}
	if instrumentIDs != nil && len(groupBy) == 1 && groupBy[0] == "instrument_id" {
		query = "SELECT " + selectList(collection, "point") + " FROM unnest($1::text[]) AS scope(instrument_id) " +
			"CROSS JOIN LATERAL (SELECT * FROM " + s.Table(collection) + " WHERE instrument_id = scope.instrument_id " +
			"ORDER BY " + quote(timeField) + " DESC NULLS LAST,storage_key DESC LIMIT 1) AS point"
		args = append(args, instrumentIDs)
	} else {
		where := "TRUE"
		if instrumentIDs != nil {
			where = "instrument_id = ANY($1::text[])"
			args = append(args, instrumentIDs)
		}
		query = "SELECT DISTINCT ON (" + strings.Join(groups, ",") + ") " + selectList(collection, "") + " FROM " + s.Table(collection) +
			" WHERE " + where + " ORDER BY " + strings.Join(groups, ",") + "," + quote(timeField) + " DESC NULLS LAST,storage_key DESC"
	}
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows(collection, rows)
	return out, err
}

// HotTokens is every row of the live mkt.hot_tokens (core's escalations).
func (s *Store) HotTokens(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+selectList("mkt.hot_tokens", "")+" FROM "+liveTable("mkt.hot_tokens")+" ORDER BY storage_key")
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows("mkt.hot_tokens", rows)
	return out, err
}

// BackfillRuns are the live mkt.backfill_runs rows (core's capture obligations) with
// one of the statuses, oldest first.
func (s *Store) BackfillRuns(ctx context.Context, statuses []string) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+selectList("mkt.backfill_runs", "")+" FROM "+liveTable("mkt.backfill_runs")+
		" WHERE status = ANY($1::text[]) ORDER BY started_at NULLS FIRST,storage_key", statuses)
	if err != nil {
		return nil, err
	}
	out, _, err := collectRows("mkt.backfill_runs", rows)
	return out, err
}

// requests is core's repair request collection; it always resolves to mkt.
const requests = "mkt.capture_requests"

// ClaimRequest takes one PENDING repair request: requests whose deadline passed become
// EXPIRED first, then the oldest PENDING one becomes RUNNING for writer. It returns the
// claimed row (nil when there is none).
func (s *Store) ClaimRequest(ctx context.Context, writer string, now time.Time) (map[string]any, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "SELECT "+selectList(requests, "")+" FROM "+liveTable(requests)+
		" WHERE status='PENDING' AND deadline <= $1 FOR UPDATE SKIP LOCKED", now)
	if err != nil {
		return nil, err
	}
	expired, keys, err := collectRows(requests, rows)
	if err != nil {
		return nil, err
	}
	for _, row := range expired {
		row["status"], row["reason"] = "EXPIRED", "REQUEST_DEADLINE_PASSED"
		pytime.CollectionClock(now).Into(row, "finished_at")
	}
	if len(expired) > 0 {
		if _, err := s.putMarketBatch(ctx, tx, requests, expired, keys); err != nil {
			return nil, err
		}
	}
	rows, err = tx.Query(ctx, "SELECT "+selectList(requests, "")+" FROM "+liveTable(requests)+
		" WHERE status='PENDING' ORDER BY requested_at NULLS LAST,storage_key LIMIT 1 FOR UPDATE SKIP LOCKED")
	if err != nil {
		return nil, err
	}
	claimed, keys, err := collectRows(requests, rows)
	if err != nil {
		return nil, err
	}
	var row map[string]any
	if len(claimed) == 1 {
		row = claimed[0]
		row["status"], row["writer"] = "RUNNING", writer
		pytime.CollectionClock(now).Into(row, "started_at")
		if _, err := s.putMarketBatch(ctx, tx, requests, []map[string]any{row}, keys); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return row, nil
}

// FinishRequest writes a claimed request back (status, finished_at, result, reason)
// under the storage key core gave it.
func (s *Store) FinishRequest(ctx context.Context, row map[string]any) error {
	id, _ := row["request_id"].(string)
	if id == "" {
		return fmt.Errorf("capture request without request_id")
	}
	key := id
	err := s.Pool.QueryRow(ctx, "SELECT storage_key FROM "+liveTable(requests)+" WHERE request_id=$1", id).Scan(&key)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	_, err = s.putMarketBatch(ctx, s.Pool, requests, []map[string]any{row}, []string{key})
	return err
}

// Now is the database clock (clock_timestamp()).
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.Pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&t)
	return t.UTC(), err
}
