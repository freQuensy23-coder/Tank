package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
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
	reason := string(requeststate.DeadlinePassed)
	return requestBindings{status: "EXPIRED", writeReason: true, reason: &reason, finished: &u.finished}, nil
}

type requestCompletion struct{ completion requeststate.Completion }

func (u requestCompletion) bindings() (requestBindings, error) {
	if err := u.completion.Validate(); err != nil {
		return requestBindings{}, err
	}
	raw, err := u.completion.Outcome.JSONResult()
	if err != nil {
		return requestBindings{}, err
	}
	var result *string
	if raw != nil {
		text := string(raw)
		result = &text
	}
	var reason *string
	if typedReason := u.completion.Outcome.Reason(); typedReason != nil {
		text := string(*typedReason)
		reason = &text
	}
	return requestBindings{status: string(u.completion.Outcome.Status()), writeReason: true, reason: reason, writeResult: true, result: result, finished: &u.completion.FinishedAt}, nil
}

func (u requestBindings) apply(row requeststate.Record) requeststate.Record {
	row.Status = &u.status
	if u.writeWriter {
		row.Writer = &u.writer
	}
	if u.writeReason {
		row.Reason = u.reason
	}
	if u.writeResult {
		row.ResultJSON = u.result
	}
	if u.started != nil {
		row.StartedAt = u.started
		precision := pytime.CollectionOnly
		row.StartedAtPrec = &precision
		row.StartedAtPrecReason = nil
	}
	if u.finished != nil {
		row.FinishedAt = u.finished
		precision := pytime.CollectionOnly
		row.FinishedAtPrec = &precision
		row.FinishedAtPrecReason = nil
	}
	return row
}
func (u requestBindings) fingerprint(row requeststate.Record) (string, error) {
	document, err := u.apply(row).SemanticDocument()
	if err != nil {
		return "", err
	}
	return schema.FingerprintDocument(document, requests)
}

func (s *Store) applyRequestLifecycle(ctx context.Context, tx pgx.Tx, row requeststate.Record, update requestLifecycle) (requeststate.Record, error) {
	if row.RequestID == "" {
		return requeststate.Record{}, errors.New("capture request without request_id")
	}
	bindings, err := update.bindings()
	if err != nil {
		return requeststate.Record{}, err
	}
	fingerprint, err := bindings.fingerprint(row)
	if err != nil {
		return requeststate.Record{}, err
	}
	changed, err := decodePhysicalRequest(tx.QueryRow(ctx, "UPDATE "+liveTable(requests)+` AS request_row
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
 semantic_fingerprint=$11::text WHERE request_id=$1 RETURNING `+requestProjection,
		row.RequestID, bindings.status, bindings.writeWriter, bindings.writer,
		bindings.writeReason, bindings.reason, bindings.writeResult, bindings.result,
		bindings.started, bindings.finished, fingerprint))
	if err != nil {
		return requeststate.Record{}, fmt.Errorf("capture request lifecycle update: %w", err)
	}
	return changed, nil
}
func (s *Store) lockRequest(ctx context.Context, tx pgx.Tx, id string) (requeststate.Record, error) {
	row, err := decodePhysicalRequest(tx.QueryRow(ctx, "SELECT "+requestProjection+" FROM "+liveTable(requests)+" request_row WHERE request_id=$1 FOR UPDATE", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return requeststate.Record{}, errors.New("capture request does not exist")
	}
	return row, err
}
