package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/jackc/pgx/v5"
)

type requestLifecycle interface {
	patch() (map[string]any, error)
}

type requestClaim struct {
	writer  string
	started time.Time
}

func (u requestClaim) patch() (map[string]any, error) {
	result := map[string]any{"status": "RUNNING", "writer": u.writer}
	pytime.CollectionClock(u.started).Into(result, "started_at")
	return result, nil
}

type requestExpiry struct{ finished time.Time }

func (u requestExpiry) patch() (map[string]any, error) {
	result := map[string]any{"status": "EXPIRED", "reason": "REQUEST_DEADLINE_PASSED"}
	pytime.CollectionClock(u.finished).Into(result, "finished_at")
	return result, nil
}

type requestCompletion struct {
	status   string
	reason   *string
	result   json.RawMessage
	finished pytime.Timestamp
}

func (u requestCompletion) patch() (map[string]any, error) {
	var reason any
	if u.reason != nil {
		reason = *u.reason
	}
	var result any
	if u.result != nil {
		decoded, err := loadJSON(string(u.result))
		if err != nil {
			return nil, err
		}
		result = decoded
	}
	patch := map[string]any{"status": u.status, "reason": reason, "result": result}
	u.finished.Into(patch, "finished_at")
	return patch, nil
}

func decodeRequestCompletion(row map[string]any) (requestCompletion, error) {
	status, ok := row["status"].(string)
	if !ok || (status != "COMPLETE" && status != "FAILED" && status != "EXPIRED") {
		return requestCompletion{}, errors.New("capture request requires a terminal status")
	}
	var reason *string
	if raw := row["reason"]; raw != nil {
		text, valid := raw.(string)
		if !valid {
			return requestCompletion{}, errors.New("capture request reason requires text or null")
		}
		reason = &text
	}
	if status != "COMPLETE" && (reason == nil || *reason == "") {
		return requestCompletion{}, errors.New("failed or expired capture request requires a reason")
	}
	finished, err := pytime.Propagate(row, "finished_at", nil, "", "")
	if err != nil {
		return requestCompletion{}, err
	}
	if finished.Value == "" || finished.Precision != pytime.CollectionOnly {
		return requestCompletion{}, errors.New("terminal capture request requires an explicit collection finish timestamp")
	}
	var result json.RawMessage
	if row["result"] != nil {
		result, err = pyjson.Dumps(row["result"], false)
		if err != nil {
			return requestCompletion{}, err
		}
	}
	return requestCompletion{status, reason, result, finished}, nil
}

func requestIdentity(row map[string]any) (string, error) {
	id, ok := row["request_id"].(string)
	if !ok || id == "" {
		return "", errors.New("capture request without request_id")
	}
	return id, nil
}

func (s *Store) applyRequestLifecycle(ctx context.Context, tx pgx.Tx, row map[string]any, update requestLifecycle) (map[string]any, error) {
	id, err := requestIdentity(row)
	if err != nil {
		return nil, err
	}
	patch, err := update.patch()
	if err != nil {
		return nil, err
	}
	encoded, err := schema.Encode(requests, patch)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]any, len(row)+len(encoded))
	for key, value := range row {
		merged[key] = value
	}
	for key, value := range encoded {
		merged[key] = value
	}
	encoded["semantic_fingerprint"] = schema.Fingerprint(merged, requests)
	fields := make([]string, 0, len(encoded))
	for field := range encoded {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	assignments := make([]string, len(fields))
	arguments := []any{id}
	for index, field := range fields {
		kind := schema.Kind(requests, field)
		value, err := pgText(requests, field, kind, encoded[field])
		if err != nil {
			return nil, err
		}
		arguments = append(arguments, value)
		assignments[index] = quote(field) + "=$" + strconv.Itoa(index+2) + "::" + kind
	}
	rows, err := tx.Query(ctx, "UPDATE "+liveTable(requests)+" SET "+strings.Join(assignments, ",")+" WHERE request_id=$1 RETURNING "+selectList(requests, ""), arguments...)
	if err != nil {
		return nil, err
	}
	changed, _, err := collectRows(requests, rows)
	if err != nil {
		return nil, err
	}
	if len(changed) != 1 {
		return nil, fmt.Errorf("capture request lifecycle update matched %d rows", len(changed))
	}
	return changed[0], nil
}

func (s *Store) lockRequest(ctx context.Context, tx pgx.Tx, id string) (map[string]any, error) {
	rows, err := tx.Query(ctx, "SELECT "+selectList(requests, "")+" FROM "+liveTable(requests)+" WHERE request_id=$1 FOR UPDATE", id)
	if err != nil {
		return nil, err
	}
	found, _, err := collectRows(requests, rows)
	if err != nil {
		return nil, err
	}
	if len(found) != 1 {
		return nil, errors.New("capture request does not exist")
	}
	return found[0], nil
}
