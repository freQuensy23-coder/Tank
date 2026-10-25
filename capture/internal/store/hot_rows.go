package store

import (
	"context"
	"errors"

	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
)

func (s *Store) HotTokens(ctx context.Context) ([]hotstate.Record, error) {
	rows, err := s.Pool.Query(ctx, "SELECT to_jsonb(hot_row)::text FROM "+liveTable("mkt.hot_tokens")+" hot_row ORDER BY storage_key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]hotstate.Record, 0)
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			return nil, err
		}
		record, err := hotstate.DecodeRecord([]byte(document))
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

const anchorProjection = `jsonb_build_object(
 'storage_key',anchor_row.storage_key,'backfill_id',anchor_row.backfill_id,
 'event_id',anchor_row.event_id,'token_id',anchor_row.token_id,
 'anchor',anchor_row.anchor,'anchor_prec',anchor_row.anchor_prec,
 'anchor_prec_reason',anchor_row.anchor_prec_reason,
 'started_at',anchor_row.started_at,'started_at_prec',anchor_row.started_at_prec,
 'started_at_prec_reason',anchor_row.started_at_prec_reason,'status',anchor_row.status
)::text`

func (s *Store) BackfillAnchors(ctx context.Context, scopes []hotstate.Scope) ([]hotstate.AnchorRecord, error) {
	if len(scopes) == 0 {
		return nil, nil
	}
	seen := make(map[hotstate.Scope]struct{}, len(scopes))
	var tokens, events []string
	for _, scope := range scopes {
		if scope.TokenID == "" || scope.EventID == "" {
			return nil, errors.New("backfill anchor lookup requires explicit token and event scopes")
		}
		if _, exists := seen[scope]; exists {
			continue
		}
		seen[scope] = struct{}{}
		tokens = append(tokens, scope.TokenID)
		events = append(events, scope.EventID)
	}
	rows, err := s.Pool.Query(ctx, "WITH wanted(token_id,event_id) AS (SELECT * FROM unnest($1::text[],$2::text[])) SELECT "+
		anchorProjection+" FROM "+liveTable("mkt.backfill_runs")+" anchor_row JOIN wanted USING(token_id,event_id) ORDER BY started_at NULLS FIRST,storage_key", tokens, events)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]hotstate.AnchorRecord, 0)
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			return nil, err
		}
		record, err := hotstate.DecodeAnchorRecord([]byte(document))
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}
