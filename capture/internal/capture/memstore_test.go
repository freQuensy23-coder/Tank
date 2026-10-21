package capture

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

type memStore struct {
	mu       sync.Mutex
	queued   []points.Row
	rows     map[string]map[string]map[string]any
	writes   map[string]int
	hot      []map[string]any
	runs     []map[string]any
	requests []map[string]any
}

func newMemStore() *memStore {
	return &memStore{rows: map[string]map[string]map[string]any{}, writes: map[string]int{}}
}

func (m *memStore) Sink(r points.Row) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queued = append(m.queued, r)
}

func (m *memStore) Flush(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.queued {

		if r.Collection == "mkt.flow_bars_1m" && (r.Record["instrument_id"] == nil || r.Record["minute_start"] == nil) {
			return fmt.Errorf("Missing typed market identity %s: [instrument_id minute_start]", r.Collection)
		}
		table := m.rows[r.Collection]
		if table == nil {
			table = map[string]map[string]any{}
			m.rows[r.Collection] = table
		}
		row := plain(r.Record)
		if old := table[r.Key]; old != nil && r.Collection == "mkt.flow_bars_1m" && old["complete"] == true &&
			row["complete"] != true && (row["gap_reason"] == nil || row["gap_reason"] == Unproven) {
			continue
		}
		table[r.Key] = row
		m.writes[r.Collection]++
	}
	m.queued = nil
	return nil
}

func plain(record map[string]any) map[string]any {
	out := make(map[string]any, len(record))
	for k, v := range record {
		switch x := v.(type) {
		case time.Time:
			out[k] = pytime.ISO(x)
		case map[string]any, []any, nil, string, bool, int64:
			out[k] = x
		default:
			out[k] = pyval.Str(x)
		}
	}
	return out
}

func (m *memStore) Get(_ context.Context, collection, key string) (map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if row := m.rows[collection][key]; row != nil {
		return copyRow(row), nil
	}
	return nil, nil
}

func (m *memStore) Range(_ context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, row := range m.rows[collection] {
		t, err := pytime.Parse(row[field])
		if err != nil || (!start.IsZero() && t.Before(start)) || (!end.IsZero() && !t.Before(end)) {
			continue
		}
		match := true
		for k, v := range filters {
			if row[k] != v {
				match = false
			}
		}
		if match {
			out = append(out, copyRow(row))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return pytime.MustParse(out[i][field]).Before(pytime.MustParse(out[j][field]))
	})
	return out, nil
}

func (m *memStore) Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, ids []string) ([]map[string]any, error) {
	var out []map[string]any
	for _, id := range ids {
		rows, _ := m.Range(ctx, collection, field, start, end, map[string]any{"instrument_id": id})
		for _, r := range rows {

			p := make(map[string]any, len(fields))
			for _, f := range fields {
				if v, ok := r[f]; ok {
					p[f] = v
				}
			}
			out = append(out, p)
		}
	}
	return out, nil
}

func (m *memStore) Earliest(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) (time.Time, bool, error) {
	rows, _ := m.Range(ctx, collection, field, start, end, filters)
	if len(rows) == 0 {
		return time.Time{}, false, nil
	}
	return pytime.MustParse(rows[0][field]), true, nil
}

func (m *memStore) PruneFetches(context.Context, time.Time) (int64, error) { return 0, nil }

func (m *memStore) LatestTrades(context.Context, []string, time.Time) (map[string]map[string]any, error) {
	return map[string]map[string]any{}, nil
}

func (m *memStore) LatestRecords(context.Context, string, []string, string, []string) ([]map[string]any, error) {
	return nil, nil
}

func (m *memStore) HotTokens(context.Context) ([]map[string]any, error) { return m.hot, nil }

func (m *memStore) BackfillRuns(_ context.Context, statuses []string) ([]map[string]any, error) {
	var out []map[string]any
	for _, r := range m.runs {
		for _, st := range statuses {
			if r["status"] == st {
				out = append(out, r)
			}
		}
	}
	return out, nil
}

func (m *memStore) ClaimRequest(context.Context, string, time.Time) (map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.requests {
		if r["status"] == "PENDING" {
			r["status"] = "RUNNING"
			return copyRow(r), nil
		}
	}
	return nil, nil
}

func (m *memStore) FinishRequest(_ context.Context, row map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, r := range m.requests {
		if r["request_id"] == row["request_id"] {
			m.requests[i] = copyRow(row)
		}
	}
	return nil
}

func (m *memStore) bars(iid string) map[string]map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]map[string]any{}
	for key, row := range m.rows["mkt.flow_bars_1m"] {
		if strings.HasPrefix(key, iid+":") {
			out[text(row["minute_start"])] = row
		}
	}
	return out
}

var _ = schema.Known

func pointsRow(iid string, minute time.Time, row map[string]any) points.Row {
	return points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":" + pytime.ISO(minute), Record: row}
}
