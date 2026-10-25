package capture

import (
	"context"
	"errors"
	"fmt"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
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
	requests []requeststate.Record
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
		if r.Collection == "mkt.backfill_runs" {
			found := false
			for index, original := range m.runs {
				if original["backfill_id"] == row["backfill_id"] {
					m.runs[index] = copyRow(row)
					found = true
					break
				}
			}
			if !found {
				m.runs = append(m.runs, copyRow(row))
			}
		}
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
	type timedRecord struct {
		timestamp time.Time
		row       map[string]any
	}
	var selected []timedRecord
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
			selected = append(selected, timedRecord{timestamp: t, row: copyRow(row)})
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].timestamp.Before(selected[j].timestamp) })
	var out []map[string]any
	for _, record := range selected {
		out = append(out, record.row)
	}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, r := range m.runs {
		for _, st := range statuses {
			if r["status"] == st {
				out = append(out, copyRow(r))
			}
		}
	}
	return out, nil
}

func requestText(value string) *string { return &value }

func pendingRequest(id, token string, start, end, deadline time.Time) requeststate.Record {
	return requeststate.Record{StorageKey: id, RequestID: id, Kind: requestText("REPAIR_WINDOWS"), TokenID: &token, Start: &start, End: &end, Deadline: &deadline, Status: requestText("PENDING")}
}
func (m *memStore) ClaimRequest(_ context.Context, writer string, now time.Time) (*requeststate.Claimed, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.requests {
		row := &m.requests[i]
		if row.Status != nil && *row.Status == "PENDING" && row.Deadline != nil && !row.Deadline.After(now) {
			row.Status = requestText("EXPIRED")
			row.Reason = requestText("REQUEST_DEADLINE_PASSED")
			row.FinishedAt = &now
			row.FinishedAtPrec = requestText(pytime.CollectionOnly)
			row.FinishedAtPrecReason = nil
		}
	}
	for i := range m.requests {
		row := &m.requests[i]
		if row.Status != nil && *row.Status == "PENDING" {
			row.Status = requestText("RUNNING")
			row.Writer = &writer
			row.StartedAt = &now
			row.StartedAtPrec = requestText(pytime.CollectionOnly)
			row.StartedAtPrecReason = nil
			value, err := row.Claimed()
			return &value, err
		}
	}
	return nil, nil
}
func (m *memStore) FinishRequest(_ context.Context, completion requeststate.Completion) error {
	if err := completion.Validate(); err != nil {
		return err
	}
	result, err := completion.Outcome.JSONResult()
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.requests {
		row := &m.requests[i]
		if row.RequestID == completion.ID {
			row.Status = requestText(string(completion.Outcome.Status()))
			row.Reason = nil
			if reason := completion.Outcome.Reason(); reason != nil {
				row.Reason = requestText(string(*reason))
			}
			row.ResultJSON = nil
			if result != nil {
				row.ResultJSON = requestText(string(result))
			}
			row.FinishedAt = &completion.FinishedAt
			row.FinishedAtPrec = requestText(pytime.CollectionOnly)
			row.FinishedAtPrecReason = nil
			return nil
		}
	}
	return errors.New("capture request does not exist")
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
