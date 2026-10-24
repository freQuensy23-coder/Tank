package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/jackc/pgx/v5"
)

type requestLifecycle interface {
	bindings() (requestBindings, error)
}

type requestBindings struct {
	status      string
	writeWriter bool
	writer      string
	writeReason bool
	reason      *string
	writeResult bool
	result      *string
	started     *time.Time
	finished    *time.Time
}

type requestClaim struct {
	writer  string
	started time.Time
}

func (u requestClaim) bindings() (requestBindings, error) {
	return requestBindings{status: "RUNNING", writeWriter: true, writer: u.writer, started: &u.started}, nil
}

type requestExpiry struct{ finished time.Time }

func (u requestExpiry) bindings() (requestBindings, error) {
	reason := "REQUEST_DEADLINE_PASSED"
	return requestBindings{status: "EXPIRED", writeReason: true, reason: &reason, finished: &u.finished}, nil
}

type requestCompletion struct {
	status   string
	reason   *string
	result   json.RawMessage
	finished pytime.Timestamp
}

func (u requestCompletion) bindings() (requestBindings, error) {
	finished, err := time.Parse(time.RFC3339Nano, u.finished.Value)
	if err != nil {
		return requestBindings{}, err
	}
	var result *string
	if u.result != nil {
		value := string(u.result)
		result = &value
	}
	return requestBindings{status: u.status, writeReason: true, reason: u.reason, writeResult: true, result: result, finished: &finished}, nil
}

func (u requestBindings) fingerprint(row map[string]any) (string, error) {
	merged := make(map[string]any, len(row)+10)
	for key, value := range row {
		merged[key] = value
	}
	merged["status"] = u.status
	if u.writeWriter {
		merged["writer"] = u.writer
	}
	if u.writeReason {
		merged["reason"] = nil
		if u.reason != nil {
			merged["reason"] = *u.reason
		}
	}
	if u.writeResult {
		merged["result"] = nil
		if u.result != nil {
			value, err := loadJSON(*u.result)
			if err != nil {
				return "", err
			}
			merged["result"] = value
		}
	}
	if u.started != nil {
		merged["started_at"] = *u.started
		merged["started_at_prec"] = pytime.CollectionOnly
		merged["started_at_prec_reason"] = nil
	}
	if u.finished != nil {
		merged["finished_at"] = *u.finished
		merged["finished_at_prec"] = pytime.CollectionOnly
		merged["finished_at_prec_reason"] = nil
	}
	return schema.Fingerprint(merged, requests), nil
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
	bindings, err := update.bindings()
	if err != nil {
		return nil, err
	}
	fingerprint, err := bindings.fingerprint(row)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, "UPDATE "+liveTable(requests)+`
 SET status=$2::text,
 writer=CASE WHEN $3::boolean THEN $4::text ELSE writer END,
 reason=CASE WHEN $5::boolean THEN $6::text ELSE reason END,
 result=CASE WHEN $7::boolean THEN $8::jsonb ELSE result END,
 started_at=CASE WHEN $9::timestamptz IS NOT NULL THEN $9::timestamptz ELSE started_at END,
 started_at_prec=CASE WHEN $9::timestamptz IS NOT NULL THEN 'COLLECTION_ONLY' ELSE started_at_prec END,
 started_at_prec_reason=CASE WHEN $9::timestamptz IS NOT NULL THEN NULL ELSE started_at_prec_reason END,
 finished_at=CASE WHEN $10::timestamptz IS NOT NULL THEN $10::timestamptz ELSE finished_at END,
 finished_at_prec=CASE WHEN $10::timestamptz IS NOT NULL THEN 'COLLECTION_ONLY' ELSE finished_at_prec END,
 finished_at_prec_reason=CASE WHEN $10::timestamptz IS NOT NULL THEN NULL ELSE finished_at_prec_reason END,
 semantic_fingerprint=$11::text WHERE request_id=$1 RETURNING `+selectList(requests, ""),
		id, bindings.status, bindings.writeWriter, bindings.writer,
		bindings.writeReason, bindings.reason, bindings.writeResult, bindings.result,
		bindings.started, bindings.finished, fingerprint)
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
