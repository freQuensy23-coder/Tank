package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
)

const memoryBackfills = "mkt.backfill_runs"

func queuedBackfill(t *testing.T, memory *memStore, key string, row map[string]any) backfillstate.Observation {
	t.Helper()
	memory.seedBackfill(key, row)
	observations, err := memory.BackfillObligations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range observations {
		if observation.Reference().StorageKey() == key {
			return observation
		}
	}
	panic("authored pending backfill was not observed from actual memory persistence")
}

func memoryBackfillRecord(key string, row map[string]any) (backfillstate.Record, error) {
	projection := copyRow(row)
	if value, present := projection["storage_key"]; present && value != key {
		return backfillstate.Record{}, errors.New("memory backfill key differs from its actual storage identity")
	}
	projection["storage_key"] = key
	document, err := json.Marshal(projection)
	if err != nil {
		return backfillstate.Record{}, err
	}
	var record backfillstate.Record
	wire := struct {
		Result json.RawMessage `json:"result"`
		*backfillstate.Record
	}{Record: &record}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return backfillstate.Record{}, err
	}
	if wire.Result != nil && !bytes.Equal(wire.Result, []byte("null")) {
		text := string(wire.Result)
		record.ResultJSON = &text
	}
	return record, nil
}

func (m *memStore) seedBackfill(key string, row map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rows[memoryBackfills] == nil {
		m.rows[memoryBackfills] = map[string]map[string]any{}
	}
	m.rows[memoryBackfills][key] = copyRow(row)
	m.backfillRevisions[key]++
}

func (m *memStore) observedBackfill(key string, row map[string]any) (backfillstate.Observation, error) {
	revision, present := m.backfillRevisions[key]
	if !present {
		return backfillstate.Observation{}, errors.New("memory backfill has no actual mutation revision")
	}
	reference, err := backfillstate.NewReference(key, strconv.FormatUint(revision, 10))
	if err != nil {
		return backfillstate.Observation{}, err
	}
	record, err := memoryBackfillRecord(key, row)
	if err != nil {
		return backfillstate.Observation{}, err
	}
	return backfillstate.Observe(reference, record)
}

func (m *memStore) BackfillObligations(ctx context.Context) ([]backfillstate.Observation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	type selected struct {
		observation backfillstate.Observation
		started     *time.Time
	}
	var rows []selected
	for key, row := range m.rows[memoryBackfills] {
		if row["status"] != "PENDING" && row["status"] != "RUNNING" {
			continue
		}
		observation, err := m.observedBackfill(key, row)
		if err != nil {
			return nil, err
		}
		record, err := memoryBackfillRecord(key, row)
		if err != nil {
			return nil, err
		}
		var started *time.Time
		if record.StartedAt != nil {
			value, err := time.Parse(time.RFC3339Nano, string(*record.StartedAt))
			if err != nil {
				return nil, err
			}
			started = &value
		}
		rows = append(rows, selected{observation: observation, started: started})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if (a.started == nil) != (b.started == nil) {
			return a.started == nil
		}
		if a.started != nil && !a.started.Equal(*b.started) {
			return a.started.Before(*b.started)
		}
		return a.observation.Reference().StorageKey() < b.observation.Reference().StorageKey()
	})
	result := make([]backfillstate.Observation, len(rows))
	for index, row := range rows {
		result[index] = row.observation
	}
	return result, nil
}

type memoryBackfillTransition interface {
	Validate() error
	Reference() backfillstate.Reference
	Bind(backfillstate.Observation) error
	Writer() backfillstate.WriterIdentity
	RecordedAt() time.Time
}

func (m *memStore) StartBackfill(ctx context.Context, start backfillstate.Start) (backfillstate.Acknowledgement, error) {
	return m.writeBackfill(ctx, start)
}

func (m *memStore) FinishBackfill(ctx context.Context, completion backfillstate.Completion) (backfillstate.Acknowledgement, error) {
	return m.writeBackfill(ctx, completion)
}

func (m *memStore) writeBackfill(ctx context.Context, transition memoryBackfillTransition) (backfillstate.Acknowledgement, error) {
	if err := ctx.Err(); err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	if err := transition.Validate(); err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	expected := transition.Reference()
	row, present := m.rows[memoryBackfills][expected.StorageKey()]
	if !present {
		return backfillstate.Unchanged(expected, backfillstate.WriteMissing)
	}
	actual, err := m.observedBackfill(expected.StorageKey(), row)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	if actual.Reference() != expected || (row["status"] != "PENDING" && row["status"] != "RUNNING") {
		return backfillstate.Unchanged(expected, backfillstate.WriteSuperseded)
	}
	if err := transition.Bind(actual); err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	changed := copyRow(row)
	clock := func(field string, at time.Time) {
		changed[field], changed[field+"_prec"], changed[field+"_prec_reason"] = pytime.ISO(at), pytime.CollectionOnly, nil
	}
	switch value := transition.(type) {
	case backfillstate.Start:
		changed["status"] = "RUNNING"
		clock("started_at", value.StartedAt())
	case backfillstate.Completion:
		changed["status"] = value.Outcome().Status()
		if reason := value.Outcome().Reason(); reason != nil {
			changed["reason"] = *reason
		} else {
			changed["reason"] = nil
		}
		clock("finished_at", value.FinishedAt())
		switch outcome := value.Outcome().(type) {
		case backfillstate.CaptureOutcome:
			document, err := outcome.JSONResult()
			if err != nil {
				return backfillstate.Acknowledgement{}, err
			}
			var result map[string]any
			decoder := json.NewDecoder(bytes.NewReader(document))
			decoder.UseNumber()
			if err := decoder.Decode(&result); err != nil {
				return backfillstate.Acknowledgement{}, err
			}
			changed["result"] = result
		case backfillstate.Failure:
		default:
			return backfillstate.Acknowledgement{}, errors.New("memory backfill outcome is not supported")
		}
	default:
		return backfillstate.Acknowledgement{}, errors.New("memory backfill transition is not supported")
	}
	clock("recorded_at", transition.RecordedAt())
	changed["config_version"], changed["code_version"] = transition.Writer().ConfigVersion(), transition.Writer().CodeVersion()
	record, err := memoryBackfillRecord(expected.StorageKey(), changed)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	document, err := record.SemanticDocument()
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	fingerprint, err := schema.FingerprintDocument(document, memoryBackfills)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	changed["semantic_fingerprint"] = fingerprint
	revision := m.backfillRevisions[expected.StorageKey()]
	if revision == ^uint64(0) {
		return backfillstate.Acknowledgement{}, errors.New("memory backfill mutation revision exhausted")
	}
	reference, err := backfillstate.NewReference(expected.StorageKey(), strconv.FormatUint(revision+1, 10))
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	acknowledgement, err := backfillstate.Applied(expected, reference)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	m.rows[memoryBackfills][expected.StorageKey()] = changed
	m.backfillRevisions[expected.StorageKey()] = revision + 1
	m.writes[memoryBackfills]++
	return acknowledgement, nil
}
