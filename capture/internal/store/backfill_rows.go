package store

import (
	"context"
	"errors"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/jackc/pgx/v5"
)

const backfillRuns = "mkt.backfill_runs"

const backfillProjection = `jsonb_build_object(
 'storage_key',backfill_row.storage_key,
 'anchor',backfill_row.anchor,'anchor_prec',backfill_row.anchor_prec,
 'anchor_prec_reason',backfill_row.anchor_prec_reason,
 'backfill_id',backfill_row.backfill_id,'code_version',backfill_row.code_version,
 'config_version',backfill_row.config_version,'event_id',backfill_row.event_id,
 'finished_at',backfill_row.finished_at,'finished_at_prec',backfill_row.finished_at_prec,
 'finished_at_prec_reason',backfill_row.finished_at_prec_reason,
 'instrument_id',backfill_row.instrument_id,'reason',backfill_row.reason,
 'received_ts',backfill_row.received_ts,'received_ts_prec',backfill_row.received_ts_prec,
 'received_ts_prec_reason',backfill_row.received_ts_prec_reason,
 'recorded_at',backfill_row.recorded_at,'recorded_at_prec',backfill_row.recorded_at_prec,
 'recorded_at_prec_reason',backfill_row.recorded_at_prec_reason,
 'started_at',backfill_row.started_at,'started_at_prec',backfill_row.started_at_prec,
 'started_at_prec_reason',backfill_row.started_at_prec_reason,
 'status',backfill_row.status,'token_id',backfill_row.token_id,
 'result',backfill_row.result::text,'semantic_fingerprint',backfill_row.semantic_fingerprint
)::text`

type backfillSnapshot struct {
	record      backfillstate.Record
	transaction string
}

func decodeBackfillRow(row pgx.Row) (backfillSnapshot, error) {
	var document, transaction string
	if err := row.Scan(&document, &transaction); err != nil {
		return backfillSnapshot{}, err
	}
	record, err := backfillstate.DecodeRecord([]byte(document))
	if err != nil {
		return backfillSnapshot{}, err
	}
	if transaction == "" {
		return backfillSnapshot{}, errors.New("physical backfill row has no observed transaction revision")
	}
	return backfillSnapshot{record: record, transaction: transaction}, nil
}

func (s *Store) backfillRows(ctx context.Context, statuses []string) ([]backfillSnapshot, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+backfillProjection+",backfill_row.xmin::text FROM "+liveTable(backfillRuns)+
		" backfill_row WHERE status = ANY($1::text[]) ORDER BY started_at NULLS FIRST,storage_key", statuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]backfillSnapshot, 0)
	for rows.Next() {
		row, err := decodeBackfillRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
