package store

import (
	"context"
	"errors"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/jackc/pgx/v5"
)

type backfillTransition interface {
	Validate() error
	Reference() backfillstate.Reference
	RecordedAt() time.Time
	Writer() backfillstate.WriterIdentity
	Bind(backfillstate.Observation) error
}

type backfillBindings struct {
	status            string
	writeReason       bool
	reason            *string
	writeResult       bool
	result            *string
	started, finished *time.Time
	recorded          time.Time
	writer            backfillstate.WriterIdentity
}

func bindBackfill(transition backfillTransition) (backfillBindings, error) {
	bindings := backfillBindings{recorded: transition.RecordedAt(), writer: transition.Writer()}
	switch value := transition.(type) {
	case backfillstate.Start:
		at := value.StartedAt()
		bindings.status, bindings.started = "RUNNING", &at
	case backfillstate.Completion:
		at := value.FinishedAt()
		bindings.status, bindings.finished = value.Outcome().Status(), &at
		bindings.writeReason, bindings.reason = true, value.Outcome().Reason()
		switch outcome := value.Outcome().(type) {
		case backfillstate.CaptureOutcome:
			raw, err := outcome.JSONResult()
			if err != nil {
				return backfillBindings{}, err
			}
			text := string(raw)
			bindings.writeResult, bindings.result = true, &text
		case backfillstate.Failure:
		default:
			return backfillBindings{}, errors.New("backfill completion outcome is not supported")
		}
	default:
		return backfillBindings{}, errors.New("backfill transition is not supported")
	}
	return bindings, nil
}

func backfillTimestamp(value time.Time) *backfillstate.TimestampText {
	text := backfillstate.TimestampText(pytime.ISO(value))
	return &text
}

func (b backfillBindings) apply(row backfillstate.Record) backfillstate.Record {
	row.Status = &b.status
	if b.writeReason {
		row.Reason = b.reason
	}
	if b.writeResult {
		row.ResultJSON = b.result
	}
	precision := pytime.CollectionOnly
	if b.started != nil {
		row.StartedAt, row.StartedAtPrec, row.StartedAtPrecReason = backfillTimestamp(*b.started), &precision, nil
	}
	if b.finished != nil {
		row.FinishedAt, row.FinishedAtPrec, row.FinishedAtPrecReason = backfillTimestamp(*b.finished), &precision, nil
	}
	row.RecordedAt, row.RecordedAtPrec, row.RecordedAtPrecReason = backfillTimestamp(b.recorded), &precision, nil
	configVersion, codeVersion := b.writer.ConfigVersion(), b.writer.CodeVersion()
	row.ConfigVersion, row.CodeVersion = &configVersion, &codeVersion
	return row
}

func (s *Store) StartBackfill(ctx context.Context, start backfillstate.Start) (backfillstate.Acknowledgement, error) {
	return s.applyBackfill(ctx, start)
}

func (s *Store) FinishBackfill(ctx context.Context, completion backfillstate.Completion) (backfillstate.Acknowledgement, error) {
	return s.applyBackfill(ctx, completion)
}

func (s *Store) applyBackfill(ctx context.Context, transition backfillTransition) (backfillstate.Acknowledgement, error) {
	if err := transition.Validate(); err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	expected := transition.Reference()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	current, err := decodeBackfillRow(tx.QueryRow(ctx, "SELECT "+backfillProjection+",backfill_row.xmin::text FROM "+liveTable(backfillRuns)+" backfill_row WHERE storage_key=$1 FOR UPDATE", expected.StorageKey()))
	if errors.Is(err, pgx.ErrNoRows) {
		return backfillstate.Unchanged(expected, backfillstate.WriteMissing)
	}
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	if current.transaction != expected.Revision() || current.record.Status == nil || (*current.record.Status != "PENDING" && *current.record.Status != "RUNNING") {
		return backfillstate.Unchanged(expected, backfillstate.WriteSuperseded)
	}
	actual, err := backfillstate.Observe(expected, current.record)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	if err := transition.Bind(actual); err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	bindings, err := bindBackfill(transition)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	document, err := bindings.apply(current.record).SemanticDocument()
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	fingerprint, err := schema.FingerprintDocument(document, backfillRuns)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	changed, err := decodeBackfillRow(tx.QueryRow(ctx, "UPDATE "+liveTable(backfillRuns)+` AS backfill_row SET
 status=$3::text, reason=CASE WHEN $4::boolean THEN $5::text ELSE reason END,
 result=CASE WHEN $6::boolean THEN $7::jsonb ELSE result END,
 started_at=CASE WHEN $8::timestamptz IS NOT NULL THEN $8::timestamptz ELSE started_at END,
 started_at_prec=CASE WHEN $8::timestamptz IS NOT NULL THEN 'COLLECTION_ONLY' ELSE started_at_prec END,
 started_at_prec_reason=CASE WHEN $8::timestamptz IS NOT NULL THEN NULL ELSE started_at_prec_reason END,
 finished_at=CASE WHEN $9::timestamptz IS NOT NULL THEN $9::timestamptz ELSE finished_at END,
 finished_at_prec=CASE WHEN $9::timestamptz IS NOT NULL THEN 'COLLECTION_ONLY' ELSE finished_at_prec END,
 finished_at_prec_reason=CASE WHEN $9::timestamptz IS NOT NULL THEN NULL ELSE finished_at_prec_reason END,
 recorded_at=$10::timestamptz,recorded_at_prec='COLLECTION_ONLY',recorded_at_prec_reason=NULL,
 config_version=$11::text,code_version=$12::text,semantic_fingerprint=$13::text
 WHERE storage_key=$1 AND xmin::text=$2 RETURNING `+backfillProjection+",backfill_row.xmin::text",
		expected.StorageKey(), expected.Revision(), bindings.status, bindings.writeReason, bindings.reason,
		bindings.writeResult, bindings.result, bindings.started, bindings.finished, bindings.recorded,
		bindings.writer.ConfigVersion(), bindings.writer.CodeVersion(), fingerprint))
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	reference, err := backfillstate.NewReference(changed.record.StorageKey, changed.transaction)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	acknowledgement, err := backfillstate.Applied(expected, reference)
	if err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return backfillstate.Acknowledgement{}, err
	}
	return acknowledgement, nil
}
