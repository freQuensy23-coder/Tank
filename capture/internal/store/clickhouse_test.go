package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func chStore(t *testing.T) *Store {
	t.Helper()
	raw := os.Getenv("CLICKHOUSE_TEST_URL")
	if raw == "" {
		t.Skip("CLICKHOUSE_TEST_URL not set")
	}
	ch, err := NewClickHouse(raw, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for table := range ClickHouseTables {
		if _, err := ch.post(ctx, "TRUNCATE TABLE "+table, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	return &Store{CH: ch}
}

func flowBar(minute time.Time, complete bool, reason any) map[string]any {
	return map[string]any{
		"instrument_id": "binance:spot:BTCUSDT", "token_id": "bitcoin:BTC",
		"minute_start": pytime.ISO(minute), "minute_start_prec": "EXACT",
		"buy_qty": "1.5", "sell_qty": "0.5", "complete": complete, "gap_reason": reason,
		"last_trade":  map[string]any{"trade_id": "9", "ts": pytime.ISO(minute.Add(30 * time.Second))},
		"recorded_at": pytime.ISO(minute.Add(61 * time.Second)), "recorded_at_prec": "COLLECTION_ONLY",
	}
}

func TestClickHouseWritesLikeThePostgresUpsert(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	m0 := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	key := "binance:spot:BTCUSDT:" + pytime.ISO(m0)
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowBar(m0, true, nil)}, []string{key}); err != nil || len(out) != 1 {
		t.Fatalf("first write: %v %v", out, err)
	}

	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowBar(m0, true, nil)}, []string{key}); err != nil || len(out) != 0 {
		t.Fatalf("same write changed %v %v", out, err)
	}
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowBar(m0, false, Unproven)}, []string{key}); err != nil || len(out) != 0 {
		t.Fatalf("unproven write changed %v %v", out, err)
	}
	got, err := s.Get(ctx, "mkt.flow_bars_1m", key)
	if err != nil || got == nil || got["complete"] != true || got["buy_qty"] != "1.5" {
		t.Fatalf("get = %v %v", got, err)
	}

	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowBar(m0, false, "REST_REPAIR_INCOMPLETE")}, []string{key}); err != nil || len(out) != 1 {
		t.Fatalf("reasoned write: %v %v", out, err)
	}
	if got, _ := s.Get(ctx, "mkt.flow_bars_1m", key); got["complete"] != false || got["gap_reason"] != "REST_REPAIR_INCOMPLETE" {
		t.Fatalf("after reasoned write %v", got)
	}
	m1 := m0.Add(time.Minute)
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowBar(m1, true, nil)}, []string{"binance:spot:BTCUSDT:" + pytime.ISO(m1)}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Range(ctx, "mkt.flow_bars_1m", "minute_start", m0, m0.Add(time.Hour), map[string]any{"instrument_id": "binance:spot:BTCUSDT"})
	if err != nil || len(rows) != 2 || rows[1]["minute_start"] != pytime.ISO(m1) {
		t.Fatalf("range = %v %v", rows, err)
	}
	proj, err := s.Projection(ctx, "mkt.flow_bars_1m", []string{"minute_start", "complete", "last_trade"}, "minute_start", m0, m0.Add(time.Hour), []string{"binance:spot:BTCUSDT"})
	if err != nil || len(proj) != 2 || proj[1]["complete"] != true {
		t.Fatalf("projection = %v %v", proj, err)
	}
	if lt, ok := proj[1]["last_trade"].(map[string]any); !ok || lt["trade_id"] != "9" {
		t.Fatalf("last_trade = %v", proj[1]["last_trade"])
	}
	first, found, err := s.Earliest(ctx, "mkt.flow_bars_1m", "minute_start", m0.Add(-time.Hour), m0.Add(time.Hour), nil)
	if err != nil || !found || !first.Equal(m0) {
		t.Fatalf("earliest = %v %v %v", first, found, err)
	}
	if _, found, _ := s.Earliest(ctx, "mkt.flow_bars_1m", "minute_start", m0.Add(-48*time.Hour), m0.Add(-24*time.Hour), nil); found {
		t.Fatal("earliest of an empty range found something")
	}
	latest, err := s.LatestRecords(ctx, "mkt.flow_bars_1m", []string{"instrument_id"}, "minute_start", []string{"binance:spot:BTCUSDT"})
	if err != nil || len(latest) != 1 || latest[0]["minute_start"] != pytime.ISO(m1) {
		t.Fatalf("latest = %v %v", latest, err)
	}
	trades, err := s.LatestTrades(ctx, []string{"binance:spot:BTCUSDT"}, m0)
	if err != nil || trades["binance:spot:BTCUSDT"]["trade_id"] != "9" {
		t.Fatalf("latest trades = %v %v", trades, err)
	}
}

func TestClickHouseTickerPointsMerge(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	at := pytime.ISO(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC))
	point := func(field, value, asOf string) map[string]any {
		return map[string]any{
			"instrument_id": "okx:perp_linear:BTC-USDT-SWAP", "token_id": "bitcoin:BTC", "ts": at, "ts_prec": "EXACT",
			field: value, "kind": "MARK_PRICE", "capture_source": "WEBSOCKET",
			"field_sources": map[string]any{field: map[string]any{"as_of": asOf, "capture_source": "WEBSOCKET"}},
			"recorded_at":   at, "recorded_at_prec": "COLLECTION_ONLY",
		}
	}
	key := "okx:perp_linear:BTC-USDT-SWAP:" + at
	if _, err := s.PutMarketBatch(ctx, "mkt.tickers_24h", []map[string]any{point("mark_price", "100", at)}, []string{key}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.tickers_24h", []map[string]any{point("index_price", "99", at)}, []string{key}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "mkt.tickers_24h", key)
	if err != nil || got["mark_price"] != "100" || got["index_price"] != "99" {
		t.Fatalf("merged ticker = %v %v", got, err)
	}
}
