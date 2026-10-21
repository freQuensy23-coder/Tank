package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

const WriteChunk = 500

const Unproven = "STREAM_COVERAGE_UNPROVEN"

func prepareRecords(collection string, records []map[string]any, keys []string) ([]string, map[string]map[string]any, error) {
	encoded := make([]map[string]any, len(records))
	for i, r := range records {
		e, err := schema.Encode(collection, r)
		if err != nil {
			return nil, nil, err
		}
		encoded[i] = e
	}
	identity := schema.Identities[collection]
	if keys == nil {
		keys = make([]string, len(encoded))
		for i, r := range encoded {
			parts := make([]string, len(identity))
			for j, name := range identity {
				if v, ok := r[name]; ok {
					parts[j] = pyStr(v)
				}
			}
			keys[i] = strings.Join(parts, ":")
		}
	}
	if len(keys) != len(records) {
		return nil, nil, errors.New("keys and records must have equal lengths")
	}
	var order []string
	unique := map[string]map[string]any{}
	identityKeys := map[string]string{}
	for i, key := range keys {
		record := encoded[i]
		values := make([]any, len(identity))
		for j, name := range identity {
			var v any = key
			if name != "storage_key" {
				v = record[name]
			}
			if isNone(v) || v == "" {
				return nil, nil, fmt.Errorf("Missing typed market identity %s: %v", collection, identity)
			}
			values[j] = v
		}
		ik := identityKey(values)
		if previousKey, ok := identityKeys[ik]; ok {
			previous, held := unique[previousKey]
			if !held {
				return nil, nil, fmt.Errorf("KeyError: %q", previousKey)
			}
			delete(unique, previousKey)
			for n, k := range order {
				if k == previousKey {
					order = append(order[:n], order[n+1:]...)
					break
				}
			}
			if collection == "mkt.funding_obs" || collection == "mkt.tickers_24h" {
				merged, err := mergePoint(collection, previous, record)
				if err != nil {
					return nil, nil, err
				}
				if record, err = schema.Encode(collection, merged); err != nil {
					return nil, nil, err
				}
			}
		}
		identityKeys[ik] = key
		record["semantic_fingerprint"] = schema.Fingerprint(record, collection)
		if _, exists := unique[key]; !exists {
			order = append(order, key)
		}
		unique[key] = record
	}
	return order, unique, nil
}

func (s *Store) upsert(collection string) string {
	if q, ok := s.statements.Load(collection); ok {
		return q.(string)
	}
	q := s.buildUpsert(collection)
	s.statements.Store(collection, q)
	return q
}

func (s *Store) buildUpsert(collection string) string {
	table := s.Table(collection)
	names := []string{"storage_key"}
	for _, c := range schema.Columns[collection] {
		names = append(names, c.Name)
	}
	kind := func(name string) string {
		if name == "storage_key" {
			return "text"
		}
		return schema.Kind(collection, name)
	}
	quoted := make([]string, len(names))
	selection := make([]string, len(names))
	unnest := make([]string, len(names))
	for i, name := range names {
		q := quote(name)
		quoted[i] = q
		unnest[i] = "$" + strconv.Itoa(i+1) + "::text[]"
		switch k := kind(name); {
		case k == "text[]":
			selection[i] = "CASE WHEN u." + q + " IS NULL THEN NULL ELSE ARRAY(SELECT jsonb_array_elements_text(u." + q + "::jsonb)) END"
		case collection == "mkt.liq_obs" && name == "id":
			selection[i] = "coalesce(u." + q + "::bigint,nextval('" + s.schemaOf(collection) + ".liq_obs_id_seq'))"
		case k == "text":
			selection[i] = "u." + q
		default:
			selection[i] = "u." + q + "::" + k
		}
	}
	isIdentity := map[string]bool{}
	identity := make([]string, 0, len(schema.Identities[collection]))
	for _, name := range schema.Identities[collection] {
		isIdentity[name] = true
		identity = append(identity, quote(name))
	}
	var assignments []string
	for i, name := range names {
		if !isIdentity[name] {
			assignments = append(assignments, quoted[i]+"=EXCLUDED."+quoted[i])
		}
	}
	source := "SELECT " + strings.Join(selection, ",") + " FROM unnest(" + strings.Join(unnest, ",") + ") AS u(" + strings.Join(quoted, ",") + ")"
	head := "INSERT INTO " + table + " AS current (" + strings.Join(quoted, ",") + ") " + source + " ON CONFLICT (" + strings.Join(identity, ",") + ") DO UPDATE SET "
	returning := " RETURNING " + selectList(collection, "current")
	switch collection {
	case "mkt.funding_obs", "mkt.tickers_24h":
		mode := "ticker"
		if collection == "mkt.funding_obs" {
			mode = "funding"
		}
		merged := "ops.merge_market_point('" + mode + "',to_jsonb(current),to_jsonb(EXCLUDED))"
		points := make([]string, len(quoted))
		for i, q := range quoted {
			points[i] = "point." + q
		}
		return head + "(" + strings.Join(quoted, ",") + ") = (SELECT " + strings.Join(points, ",") + " FROM jsonb_populate_record(NULL::" + table + "," + merged + ") AS point)" +
			" WHERE ops.market_semantic(" + merged + ") IS DISTINCT FROM ops.market_semantic(to_jsonb(current))" + returning
	case "mkt.flow_bars_1m":
		return head + strings.Join(assignments, ",") + " WHERE current.semantic_fingerprint IS DISTINCT FROM EXCLUDED.semantic_fingerprint" +
			" AND NOT (current.complete IS TRUE AND EXCLUDED.complete IS NOT TRUE AND coalesce(EXCLUDED.gap_reason,'" + Unproven + "') = '" + Unproven + "')" + returning
	}
	return head + strings.Join(assignments, ",") + " WHERE current.semantic_fingerprint IS DISTINCT FROM EXCLUDED.semantic_fingerprint" + returning
}

func (s *Store) PutMarketBatch(ctx context.Context, collection string, rows []map[string]any, keys []string) ([]map[string]any, error) {
	return s.putMarketBatch(ctx, s.Pool, collection, rows, keys)
}

func (s *Store) putMarketBatch(ctx context.Context, q querier, collection string, rows []map[string]any, keys []string) ([]map[string]any, error) {
	if !schema.Known(collection) {
		return nil, fmt.Errorf("unknown market collection %s", collection)
	}
	if len(rows) <= WriteChunk {
		return s.putChunk(ctx, q, collection, rows, keys)
	}
	var changed []map[string]any
	for at := 0; at < len(rows); at += WriteChunk {
		end := min(at+WriteChunk, len(rows))
		var part []string
		if keys != nil {
			part = keys[at:min(end, len(keys))]
		}
		out, err := s.putChunk(ctx, q, collection, rows[at:end], part)
		if err != nil {
			return changed, err
		}
		changed = append(changed, out...)
	}
	return changed, nil
}

func (s *Store) putChunk(ctx context.Context, q querier, collection string, rows []map[string]any, keys []string) ([]map[string]any, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	order, unique, err := prepareRecords(collection, rows, keys)
	if err != nil {
		return nil, err
	}
	columns := schema.Columns[collection]
	arrays := make([]any, 0, len(columns)+1)
	storageKeys := make([]*string, len(order))
	for i := range order {
		storageKeys[i] = &order[i]
	}
	arrays = append(arrays, storageKeys)
	for _, c := range columns {
		values := make([]*string, len(order))
		for i, key := range order {
			v, err := pgText(collection, c.Name, c.Kind, unique[key][c.Name])
			if err != nil {
				return nil, err
			}
			values[i] = v
		}
		arrays = append(arrays, values)
	}
	result, err := q.Query(ctx, s.upsert(collection), arrays...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", collection, err)
	}
	changed, _, err := collectRows(collection, result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", collection, err)
	}
	return changed, nil
}
