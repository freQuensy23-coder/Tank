package store

import (
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"github.com/jackc/pgx/v5"
)

const requestProjection = `(to_jsonb(request_row) || jsonb_build_object(
 'budget_s', request_row.budget_s::text,
 'request', request_row.request::text,
 'result', request_row.result::text
))::text`

func decodePhysicalRequest(row pgx.Row) (requeststate.Record, error) {
	var document string
	if err := row.Scan(&document); err != nil {
		return requeststate.Record{}, err
	}
	return requeststate.DecodeRecord([]byte(document))
}

func physicalRequests(rows pgx.Rows) ([]requeststate.Record, error) {
	defer rows.Close()
	result := make([]requeststate.Record, 0)
	for rows.Next() {
		row, err := decodePhysicalRequest(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
