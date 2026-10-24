package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

var ClickHouseTables = map[string]bool{
	"mkt.klines_1m": true, "mkt.flow_bars_1m": true, "mkt.book_summaries": true, "mkt.oi_obs": true,
	"mkt.funding_obs": true, "mkt.liq_obs": true, "mkt.tickers_24h": true,
}

type ClickHouse struct {
	base, user, password string
	HTTP                 *http.Client
	version              atomic.Uint64
}

func NewClickHouse(rawURL, password string) (*ClickHouse, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	user := "default"
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok && password == "" {
			password = p
		}
	}
	c := &ClickHouse{base: u.Scheme + "://" + u.Host + "/", user: user, password: password,
		HTTP: &http.Client{Timeout: 120 * time.Second}}
	c.version.Store(uint64(time.Now().UnixNano()))
	return c, nil
}

func (c *ClickHouse) nextVersion() uint64 {
	for {
		now, last := uint64(time.Now().UnixNano()), c.version.Load()
		next := max(now, last+1)
		if c.version.CompareAndSwap(last, next) {
			return next
		}
	}
}

func (c *ClickHouse) post(ctx context.Context, query string, params map[string]string, body io.Reader) ([]byte, error) {
	q := url.Values{}
	q.Set("date_time_output_format", "iso")
	q.Set("output_format_json_quote_64bit_integers", "0")
	q.Set("output_format_json_quote_decimals", "0")
	q.Set("max_partitions_per_insert_block", "1000")
	q.Set("do_not_merge_across_partitions_select_final", "1")
	q.Set("use_skip_indexes_if_final", "0")
	q.Set("max_query_size", "268435456")
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	if body == nil {
		body = strings.NewReader(query)
	} else {
		q.Set("query", query)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"?"+q.Encode(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.password)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, strings.TrimSpace(string(out[:min(len(out), 500)])))
	}
	return out, nil
}

func (c *ClickHouse) rows(ctx context.Context, query string, params map[string]string) ([]map[string]any, error) {
	body, err := c.post(ctx, query+" FORMAT JSONEachRow", params, nil)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *ClickHouse) Rows(ctx context.Context, query string, params map[string]string) ([]map[string]any, error) {
	return c.rows(ctx, query, params)
}

func (c *ClickHouse) Exec(ctx context.Context, query string) error {
	_, err := c.post(ctx, query, nil, nil)
	return err
}

func (c *ClickHouse) Insert(ctx context.Context, table string, lines [][]byte) error {
	if len(lines) == 0 {
		return nil
	}
	_, err := c.post(ctx, "INSERT INTO "+table+" FORMAT JSONEachRow", nil, bytes.NewReader(bytes.Join(lines, []byte{'\n'})))
	return err
}

type chQuery struct {
	clauses []string
	params  map[string]string
}

func newCHQuery() *chQuery { return &chQuery{params: map[string]string{}} }

const chTime = "DateTime64(6, 'UTC')"

func (q *chQuery) param(value, kind string) string {
	name := "p" + strconv.Itoa(len(q.params))
	q.params[name] = value
	return "{" + name + ":" + kind + "}"
}

func (q *chQuery) where(clause string) { q.clauses = append(q.clauses, clause) }

func (q *chQuery) predicate() string {
	if len(q.clauses) == 0 {
		return "1"
	}
	return strings.Join(q.clauses, " AND ")
}

func chTimeText(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

func chArray(values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func chColumn(name string) string { return "`" + name + "`" }

func chFilters(q *chQuery, collection string, filters map[string]any) error {
	names := make([]string, 0, len(filters))
	for k := range filters {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		value := filters[name]
		column := name
		if target, ok := schema.Aliases[collection][name]; ok {
			column = target
		}
		kind := schema.Kind(collection, column)
		if kind == "" {
			return fmt.Errorf("Unknown typed market filter %s.%s", collection, name)
		}
		if isNone(value) {
			q.where(chColumn(column) + " IS NULL")
			continue
		}
		arg, err := typedArgument(collection, column, kind, value)
		if err != nil {
			return err
		}
		text := fmt.Sprint(arg)
		switch kind {
		case "timestamptz":
			t, err := pytime.Parse(value)
			if err != nil {
				return err
			}
			q.where(chColumn(column) + " = " + q.param(chTimeText(t), chTime))
		case "numeric":
			q.where("toDecimal256OrNull(" + chColumn(column) + ", 30) = toDecimal256(" + q.param(text, "String") + ", 30)")
		case "boolean":
			q.where(chColumn(column) + " = " + q.param(text, "Bool"))
		case "bigint":
			q.where(chColumn(column) + " = " + q.param(text, "Int64"))
		case "text":
			q.where(chColumn(column) + " = " + q.param(text, "String"))
		default:
			return fmt.Errorf("filter on %s.%s (%s) is not supported on ClickHouse", collection, column, kind)
		}
	}
	return nil
}

func chBound(q *chQuery, column, op string, t time.Time) {
	if !t.IsZero() {
		q.where(chColumn(column) + " " + op + " " + q.param(chTimeText(t), chTime))
	}
}

func chValue(kind string, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch kind {
	case "timestamptz":
		s, _ := v.(string)
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, err
		}
		return t, nil
	case "bigint":
		n, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("bigint read as %T", v)
		}
		return n.Int64()
	case "text[]":
		list, _ := v.([]any)
		if len(list) == 0 {
			return nil, nil
		}
		out, err := json.Marshal(list)
		return string(out), err
	}
	return v, nil
}

func chDecode(collection string, row map[string]any) (map[string]any, string, error) {
	fields := []string{"storage_key"}
	values := []any{row["storage_key"]}
	for _, c := range schema.Columns[collection] {
		v, err := chValue(c.Kind, row[c.Name])
		if err != nil {
			return nil, "", fmt.Errorf("%s.%s: %w", collection, c.Name, err)
		}
		fields = append(fields, c.Name)
		values = append(values, v)
	}
	return decodeRow(collection, fields, values)
}

func (s *Store) series(collection string) error {
	if !ClickHouseTables[collection] {
		return fmt.Errorf("%s is not a ClickHouse market series", collection)
	}
	if s.CH == nil {
		return fmt.Errorf("%s: ClickHouse is not configured", collection)
	}
	return nil
}

var chDay = map[string]string{
	"mkt.klines_1m": "open_time", "mkt.flow_bars_1m": "minute_start", "mkt.book_summaries": "exchange_ts",
	"mkt.oi_obs": "ts", "mkt.funding_obs": "as_of", "mkt.liq_obs": "ts", "mkt.tickers_24h": "ts",
}

func (s *Store) chSelect(ctx context.Context, collection, where string, params map[string]string, tail string) ([]map[string]any, []string, error) {
	day := chColumn(chDay[collection])
	picked, err := s.CH.rows(ctx, "SELECT storage_key, instrument_id, toString("+day+") AS _day FROM "+collection+" FINAL WHERE "+where+" "+tail, params)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", collection, err)
	}
	if len(picked) == 0 {
		return []map[string]any{}, []string{}, nil
	}
	order := make([]string, len(picked))
	low, high := "", ""
	seen := map[string]bool{}
	var instruments []string
	for i, r := range picked {
		order[i], _ = r["storage_key"].(string)
		if iid, _ := r["instrument_id"].(string); !seen[iid] {
			seen[iid] = true
			instruments = append(instruments, iid)
		}
		d, _ := r["_day"].(string)
		if low == "" || d < low {
			low = d
		}
		if d > high {
			high = d
		}
	}
	quoted, ids := chArray(order), chArray(instruments)
	rows, err := s.CH.rows(ctx, "SELECT * FROM "+collection+" FINAL WHERE instrument_id IN ("+ids[1:len(ids)-1]+") AND "+
		day+" >= toDateTime64('"+low+"', 6, 'UTC') AND "+day+" <= toDateTime64('"+high+"', 6, 'UTC') AND storage_key IN ("+
		quoted[1:len(quoted)-1]+")", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", collection, err)
	}
	byKey := make(map[string]map[string]any, len(rows))
	for _, r := range rows {
		k, _ := r["storage_key"].(string)
		byKey[k] = r
	}
	out := make([]map[string]any, 0, len(order))
	keys := make([]string, 0, len(order))
	for _, k := range order {
		r, ok := byKey[k]
		if !ok {
			continue
		}
		rec, key, err := chDecode(collection, r)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, rec)
		keys = append(keys, key)
	}
	return out, keys, nil
}

func (s *Store) chGet(ctx context.Context, collection, key string) (map[string]any, error) {
	q := newCHQuery()
	q.where("storage_key = " + q.param(key, "String"))
	out, _, err := s.chSelect(ctx, collection, q.predicate(), q.params, "LIMIT 1")
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out[0], nil
}

func (s *Store) chRange(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error) {
	q := newCHQuery()
	q.where(chColumn(field) + " IS NOT NULL")
	chBound(q, field, ">=", start)
	chBound(q, field, "<", end)
	if err := chFilters(q, collection, filters); err != nil {
		return nil, err
	}
	out, _, err := s.chSelect(ctx, collection, q.predicate(), q.params, "ORDER BY "+chColumn(field)+", storage_key")
	return out, err
}

func (s *Store) chEarliest(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) (time.Time, bool, error) {
	q := newCHQuery()
	q.where(chColumn(field) + " IS NOT NULL")
	chBound(q, field, ">=", start)
	chBound(q, field, "<", end)
	if err := chFilters(q, collection, filters); err != nil {
		return time.Time{}, false, err
	}
	rows, err := s.CH.rows(ctx, "SELECT min("+chColumn(field)+") AS first, count() AS n FROM "+collection+" FINAL WHERE "+q.predicate(), q.params)
	if err != nil || len(rows) == 0 {
		return time.Time{}, false, err
	}
	if n, _ := rows[0]["n"].(json.Number).Int64(); n == 0 {
		return time.Time{}, false, nil
	}
	t, err := chValue("timestamptz", rows[0]["first"])
	if err != nil || t == nil {
		return time.Time{}, false, err
	}
	return t.(time.Time).UTC(), true, nil
}

func (s *Store) chProjection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, instrumentIDs []string) ([]map[string]any, error) {
	aliases := schema.Aliases[collection]
	q := newCHQuery()
	q.where(chColumn(field) + " IS NOT NULL")
	chBound(q, field, ">=", start)
	chBound(q, field, "<", end)
	if instrumentIDs != nil {
		q.where("instrument_id IN " + q.param(chArray(instrumentIDs), "Array(String)"))
	}
	expressions := make([]string, len(fields))
	kinds := make([]string, len(fields))
	for i, requested := range fields {
		column := requested
		if target, ok := aliases[requested]; ok {
			column = target
		}
		kinds[i] = "text"
		if column != "storage_key" {
			kinds[i] = schema.Kind(collection, column)
		}
		expressions[i] = chColumn(column) + " AS p" + strconv.Itoa(i)
	}
	rows, err := s.CH.rows(ctx, "SELECT "+strings.Join(expressions, ",")+" FROM "+collection+" FINAL WHERE "+q.predicate()+
		" ORDER BY "+chColumn(field)+", storage_key", q.params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", collection, err)
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		row := make(map[string]any, len(fields))
		for i, requested := range fields {
			raw, err := chValue(kinds[i], r["p"+strconv.Itoa(i)])
			if err != nil {
				return nil, err
			}
			v, err := decodeValue(kinds[i], raw)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", collection, requested, err)
			}
			row[requested] = v
		}
		out = append(out, row)
	}
	return out, nil
}

func (s *Store) chLatestTrades(ctx context.Context, scope []string, start time.Time) (map[string]map[string]any, error) {
	q := newCHQuery()
	q.where("instrument_id IN " + q.param(chArray(scope), "Array(String)"))
	q.where("minute_start >= " + q.param(chTimeText(start), chTime))
	q.where("last_trade IS NOT NULL")
	rows, err := s.CH.rows(ctx, "SELECT instrument_id, last_trade FROM mkt.flow_bars_1m FINAL WHERE "+q.predicate()+
		" ORDER BY instrument_id, minute_start DESC LIMIT 1 BY instrument_id", q.params)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for _, r := range rows {
		text, _ := r["last_trade"].(string)
		value, err := loadJSON(text)
		if err != nil {
			return nil, err
		}
		if m, ok := value.(map[string]any); ok {
			out[r["instrument_id"].(string)] = m
		}
	}
	return out, nil
}

func (s *Store) chLatestRecords(ctx context.Context, collection string, groupBy []string, timeField string, instrumentIDs []string) ([]map[string]any, error) {
	q := newCHQuery()
	if instrumentIDs != nil {
		q.where("instrument_id IN " + q.param(chArray(instrumentIDs), "Array(String)"))
	}
	groups := make([]string, len(groupBy))
	for i, g := range groupBy {
		groups[i] = chColumn(g)
	}
	g := strings.Join(groups, ",")
	out, _, err := s.chSelect(ctx, collection, q.predicate(), q.params,
		"ORDER BY "+g+", "+chColumn(timeField)+" DESC NULLS LAST, storage_key DESC LIMIT 1 BY "+g)
	return out, err
}

func chLine(collection, key string, record map[string]any, version uint64) ([]byte, error) {
	row := map[string]any{"storage_key": key, "_version": version}
	for _, c := range schema.Columns[collection] {
		value := record[c.Name]
		if isNone(value) {
			if c.Kind == "text[]" {
				row[c.Name] = []any{}
			}
			continue
		}
		switch c.Kind {
		case "timestamptz":
			t, ok := value.(time.Time)
			if !ok {
				return nil, fmt.Errorf("%s.%s is not a time: %T", collection, c.Name, value)
			}
			row[c.Name] = chTimeText(t)
		case "numeric":
			d, ok := value.(pydec.Dec)
			if !ok {
				return nil, fmt.Errorf("%s.%s is not a decimal: %T", collection, c.Name, value)
			}
			row[c.Name] = d.String()
		case "jsonb":
			out, err := pyjson.Dumps(value, false)
			if err != nil {
				return nil, err
			}
			row[c.Name] = string(out)
		case "text[]":
			row[c.Name] = value
		default:
			row[c.Name] = value
		}
	}
	return json.Marshal(row)
}

func (s *Store) chPutChunk(ctx context.Context, collection string, rows []map[string]any, keys []string) ([]map[string]any, error) {
	order, unique, err := prepareRecords(collection, rows, keys)
	if err != nil {
		return nil, err
	}
	identity := schema.Identities[collection]
	stored, err := s.chStored(ctx, collection, identity, order, unique)
	if err != nil {
		return nil, err
	}
	var lines [][]byte
	var changed []map[string]any
	version := s.CH.nextVersion()
	for _, key := range order {
		record := unique[key]
		ik := recordIdentity(identity, key, record)
		if old, ok := stored[ik]; ok {
			switch collection {
			case "mkt.flow_bars_1m":
				if old["complete"] == true && record["complete"] != true {
					reason, _ := record["gap_reason"].(string)
					if reason == "" || reason == Unproven {
						continue
					}
				}
			case "mkt.funding_obs", "mkt.tickers_24h":
				merged, err := mergePoint(collection, old, record)
				if err != nil {
					return nil, err
				}
				if record, err = schema.Encode(collection, merged); err != nil {
					return nil, err
				}
				record["semantic_fingerprint"] = schema.Fingerprint(record, collection)
			}
			if old["semantic_fingerprint"] == record["semantic_fingerprint"] {
				continue
			}
		}
		line, err := chLine(collection, key, record, version)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
		decoded, _, err := chDecodeEncoded(collection, key, record)
		if err != nil {
			return nil, err
		}
		changed = append(changed, decoded)
	}
	if err := s.CH.Insert(ctx, collection, lines); err != nil {
		return nil, fmt.Errorf("%s: %w", collection, err)
	}
	return changed, nil
}

func recordIdentity(identity []string, key string, record map[string]any) string {
	values := make([]any, len(identity))
	for i, name := range identity {
		if name == "storage_key" {
			values[i] = key
		} else {
			values[i] = record[name]
		}
	}
	return identityKey(values)
}

func (s *Store) chStored(ctx context.Context, collection string, identity, order []string, unique map[string]map[string]any) (map[string]map[string]any, error) {
	q := newCHQuery()
	for _, name := range identity {
		set := map[string]bool{}
		var values []string
		for _, key := range order {
			var text string
			if name == "storage_key" {
				text = key
			} else if t, ok := unique[key][name].(time.Time); ok {
				text = chTimeText(t)
			} else {
				text = pyStr(unique[key][name])
			}
			if !set[text] {
				set[text] = true
				values = append(values, text)
			}
		}
		kind := "Array(String)"
		if schema.Kind(collection, name) == "timestamptz" {
			kind = "Array(" + chTime + ")"
		}
		q.where(chColumn(name) + " IN " + q.param(chArray(values), kind))
	}
	rows, keys, err := s.chSelect(ctx, collection, q.predicate(), q.params, "")
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for i, row := range rows {
		encoded, err := schema.Encode(collection, stripAliases(collection, row))
		if err != nil {
			return nil, err
		}
		encoded["semantic_fingerprint"] = row["semantic_fingerprint"]
		out[recordIdentity(identity, keys[i], encoded)] = row
	}
	return out, nil
}

func stripAliases(collection string, row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		if _, alias := schema.Aliases[collection][k]; !alias {
			out[k] = v
		}
	}
	return out
}

func chDecodeEncoded(collection, key string, record map[string]any) (map[string]any, string, error) {
	fields := []string{"storage_key"}
	values := []any{key}
	for _, c := range schema.Columns[collection] {
		v := record[c.Name]
		if !isNone(v) {
			switch c.Kind {
			case "numeric":
				v = v.(pydec.Dec).String()
			case "jsonb", "text[]":
				out, err := pyjson.Dumps(v, false)
				if err != nil {
					return nil, "", err
				}
				v = string(out)
			}
		} else {
			v = nil
		}
		fields = append(fields, c.Name)
		values = append(values, v)
	}
	return decodeRow(collection, fields, values)
}
