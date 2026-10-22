package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
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

func flowRow(iid, at string) map[string]any {
	return map[string]any{
		"instrument_id": iid, "token_id": "BTC", "minute_start": at,
		"buy_qty": "1.00000000000000000001", "sell_qty": "0", "buy_usd": "10", "sell_usd": "0",
		"size_buckets": map[string]any{}, "n_buy": 1, "n_sell": 0, "sum_sq_qty": "1", "complete": true,
		"recorded_at": "2026-10-18T00:01:00Z", "recorded_at_prec": "COLLECTION_ONLY",
		"first_trade": map[string]any{"price": "123.456789012345678901"},
	}
}

func pointPackets(iid string) map[string][]map[string]any {
	base := map[string]any{"instrument_id": iid, "token_id": "BTC", "kind": "CURRENT_ESTIMATE", "as_of": "2026-10-18T00:00:00Z", "metric_key": "funding", "interval_h": "8"}
	rest := with(base, "rate", "0.02", "rate_8h_eq", "0.02", "capture_source", "PUBLIC_REST", "source_path", "rest", "received_ts", "2026-10-18T00:00:01Z")
	ws := with(base, "rate", "0.01", "rate_8h_eq", "0.01", "capture_source", "WEBSOCKET", "source_path", "ws", "received_ts", "2026-10-18T00:00:02Z")
	a := map[string]any{"instrument_id": iid, "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "mark_price": "10",
		"field_sources": map[string]any{"mark_price": map[string]any{"as_of": "2026-10-18T00:00:02Z", "capture_source": "WEBSOCKET", "source_path": "ws"}}}
	b := map[string]any{"instrument_id": iid, "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "mark_price": "9", "index_price": "11",
		"field_sources": map[string]any{
			"mark_price":  map[string]any{"as_of": "2026-10-18T00:00:01Z", "capture_source": "PUBLIC_REST", "source_path": "rest"},
			"index_price": map[string]any{"as_of": "2026-10-18T00:00:01Z", "capture_source": "PUBLIC_REST", "source_path": "rest"},
		}}
	return map[string][]map[string]any{
		"mkt.funding_obs": {with(rest, "source_observations", map[string]any{"PUBLIC_REST": rest}), with(ws, "source_observations", map[string]any{"WEBSOCKET": ws})},
		"mkt.tickers_24h": {a, b},
	}
}

func TestPutMarketBatchRoundTrip(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:BTC"
	row := flowRow(iid, "2026-10-18T00:00:00Z")
	result, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{row, row}, []string{iid + ":legacy", iid + ":canonical"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0]["buy_qty"] != row["buy_qty"] || pyjson.Canonical(result[0]["first_trade"]) != pyjson.Canonical(row["first_trade"]) {
		t.Fatalf("written %v", result)
	}
	if result[0]["minute_start_prec"] != nil || result[0]["close"] != nil || result[0]["minute_start"] != "2026-10-18T00:00:00Z" {
		t.Fatalf("decode: %v", result[0])
	}
	again, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{row}, []string{iid + ":canonical"})
	if err != nil || len(again) != 0 {
		t.Fatalf("an equal fingerprint is a no-op: %v %v", again, err)
	}
	stored, err := s.Get(ctx, "mkt.flow_bars_1m", iid+":canonical")
	if err != nil || pyjson.Canonical(stored) != pyjson.Canonical(result[0]) {
		t.Fatalf("get %v %v", stored, err)
	}
	if missing, err := s.Get(ctx, "mkt.flow_bars_1m", iid+":legacy"); err != nil || missing != nil {
		t.Fatalf("the later key replaced the earlier one: %v %v", missing, err)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{with(row, "instrument_id", nil)}, nil); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("identity: %v", err)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{with(row, "unregistered", "lost")}, nil); err == nil || !strings.Contains(err.Error(), "nknown typed") {
		t.Fatalf("unknown field: %v", err)
	}
}

func TestSeriesRequireClickHouse(t *testing.T) {
	s := &Store{}
	ctx := context.Background()
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowRow("x:BTC", "2026-10-18T00:00:00Z")}, nil); err == nil || !strings.Contains(err.Error(), "ClickHouse") {
		t.Fatalf("a series write without ClickHouse: %v", err)
	}
	if _, err := s.Range(ctx, "mkt.flow_bars_1m", "minute_start", time.Time{}, time.Time{}, nil); err == nil {
		t.Fatal("a series read without ClickHouse must fail")
	}
}

func TestUnprovenFlowBarNeverReplacesComplete(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:BTC"
	proven := flowRow(iid, "2026-10-18T00:00:00Z")
	key := []string{iid + ":bar"}
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{proven}, key); err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	for _, reason := range []any{nil, Unproven} {
		out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{with(proven, "complete", false, "gap_reason", reason)}, key)
		if err != nil || len(out) != 0 {
			t.Fatalf("an unproven bar replaced a complete one: %v %v", out, err)
		}
	}
	lost := with(proven, "complete", false, "gap_reason", "STREAM_DISCONNECTED: keepalive ping timeout")
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{lost}, key); err != nil || len(out) != 1 {
		t.Fatalf("evidence of loss invalidates: %v %v", out, err)
	}
	stored, _ := s.Get(ctx, "mkt.flow_bars_1m", key[0])
	if stored["complete"] != false || !strings.HasPrefix(stored["gap_reason"].(string), "STREAM_DISCONNECTED") {
		t.Fatalf("stored %v", stored)
	}
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{proven}, key); err != nil || len(out) != 1 {
		t.Fatalf("an incomplete bar may become complete: %v %v", out, err)
	}
}

func TestPointMergeIsOrderIndependent(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:BTC"
	for collection, rows := range pointPackets(iid) {
		for _, row := range rows {
			if _, err := s.PutMarketBatch(ctx, collection, []map[string]any{row}, []string{iid + ":seq"}); err != nil {
				t.Fatal(err)
			}
		}
		seq, err := s.Get(ctx, collection, iid+":seq")
		if err != nil || seq == nil {
			t.Fatalf("%s seq: %v %v", collection, seq, err)
		}
		if _, err := s.PutMarketBatch(ctx, collection, []map[string]any{with(rows[1], "instrument_id", iid+"b"), with(rows[0], "instrument_id", iid+"b")}, []string{iid + ":batch0", iid + ":batch1"}); err != nil {
			t.Fatal(err)
		}
		batched, err := s.Get(ctx, collection, iid+":batch1")
		if err != nil || batched == nil {
			t.Fatalf("%s batch: %v %v", collection, batched, err)
		}
		for _, field := range []string{"mark_price", "index_price", "mark_as_of", "index_as_of", "field_sources", "capture_sources", "rate", "rate_8h_eq", "capture_source", "primary_source", "received_ts", "source_disagreement", "source_observations"} {
			if g, w := pyjson.Canonical(batched[field]), pyjson.Canonical(seq[field]); g != w {
				t.Errorf("%s.%s: one batch %s, sequential writes %s", collection, field, g, w)
			}
		}
		switch collection {
		case "mkt.funding_obs":
			if seq["rate"] != "0.01" || seq["capture_source"] != "WEBSOCKET" || seq["source_disagreement"].(map[string]any)["present"] != true {
				t.Fatalf("funding merge: %v", seq)
			}
		case "mkt.tickers_24h":
			if seq["mark_price"] != "10" || seq["index_price"] != "11" {
				t.Fatalf("ticker merge: %v", seq)
			}
		}
	}
}

func TestLatestTradesRangeAndProjection(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:BTC"
	var rows []map[string]any
	var keys []string
	for i, at := range []string{"2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z", "2026-10-18T00:02:00Z"} {
		row := flowRow(iid, at)
		if i < 2 {
			row["last_trade"] = map[string]any{"trade_id": string(rune('a' + i)), "ts": at, "price": "1.5", "n": 1}
		}
		rows, keys = append(rows, row), append(keys, iid+":"+at)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", rows, keys); err != nil {
		t.Fatal(err)
	}
	latest, err := s.LatestTrades(ctx, []string{iid, iid, "storetest-none"}, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC))
	if err != nil || len(latest) != 1 || latest[iid]["trade_id"] != "b" || latest[iid]["n"] != int64(1) {
		t.Fatalf("latest %v %v", latest, err)
	}
	ranged, err := s.Range(ctx, "mkt.flow_bars_1m", "minute_start", time.Date(2026, 10, 18, 0, 1, 0, 0, time.UTC), time.Time{}, map[string]any{"instrument_id": iid, "complete": true})
	if err != nil || len(ranged) != 2 || ranged[0]["minute_start"] != "2026-10-18T00:01:00Z" {
		t.Fatalf("range %v %v", ranged, err)
	}
	projected, err := s.Projection(ctx, "mkt.flow_bars_1m", []string{"minute_start", "buy_notional", "storage_key", "n_buy"}, "minute_start",
		time.Time{}, time.Date(2026, 10, 18, 0, 2, 0, 0, time.UTC), []string{iid})
	if err != nil || len(projected) != 2 || projected[1]["buy_notional"] != "10" || projected[1]["storage_key"] != keys[1] || projected[1]["n_buy"] != int64(1) {
		t.Fatalf("projection %v %v", projected, err)
	}
	if none, err := s.Projection(ctx, "mkt.flow_bars_1m", []string{"minute_start"}, "minute_start", time.Time{}, time.Time{}, []string{}); err != nil || none != nil {
		t.Fatalf("empty scope %v %v", none, err)
	}
	first, found, err := s.Earliest(ctx, "mkt.flow_bars_1m", "minute_start", time.Time{}, time.Time{}, map[string]any{"instrument_id": iid})
	if err != nil || !found || !first.Equal(time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("earliest %v %v %v", first, found, err)
	}
	if _, found, _ := s.Earliest(ctx, "mkt.flow_bars_1m", "minute_start", time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC), time.Time{}, nil); found {
		t.Fatal("earliest of an empty range found something")
	}
	for _, packet := range pointPackets(iid)["mkt.funding_obs"] {
		if _, err := s.PutMarketBatch(ctx, "mkt.funding_obs", []map[string]any{packet}, []string{iid + ":f"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, scope := range [][]string{nil, {iid}} {
		records, err := s.LatestRecords(ctx, "mkt.funding_obs", []string{"instrument_id", "kind"}, "as_of", scope)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range records {
			found = found || r["instrument_id"] == iid && r["kind"] == "CURRENT_ESTIMATE"
		}
		if !found {
			t.Fatalf("latest records (scope %v) miss the point", scope)
		}
	}
	single, err := s.LatestRecords(ctx, "mkt.funding_obs", []string{"instrument_id"}, "as_of", []string{iid})
	if err != nil || len(single) != 1 || single[0]["instrument_id"] != iid {
		t.Fatalf("latest per instrument %v %v", single, err)
	}
}

func TestWriterFlush(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:BTC"
	w := NewWriter(s)
	kline := map[string]any{"instrument_id": iid, "token_id": "BTC", "open_time": "2026-10-18T00:00:00Z", "o": "1", "h": "2", "l": "1", "c": "2", "volume_native": "3", "complete": true}
	w.Sink(points.Row{Collection: "mkt.klines_1m", Key: iid + ":k", Record: kline})
	w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":f", Record: with(flowRow(iid, "2026-10-18T00:00:00Z"), "sell_qty", "5")})
	w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":f", Record: flowRow(iid, "2026-10-18T00:00:00Z")})
	if w.Pending() != 2 {
		t.Fatalf("pending %d (the newest row per key)", w.Pending())
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Pending() != 0 || w.LastFlush().IsZero() {
		t.Fatal("flushed")
	}
	bar, _ := s.Get(ctx, "mkt.flow_bars_1m", iid+":f")
	stored, _ := s.Get(ctx, "mkt.klines_1m", iid+":k")
	if bar == nil || bar["sell_qty"] != "0" || stored == nil {
		t.Fatalf("written %v %v", bar, stored)
	}

	w.Sink(points.Row{Collection: "mkt.klines_1m", Key: iid + ":k2", Record: with(kline, "open_time", "2026-10-18T00:01:00Z")})
	w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":bad", Record: with(flowRow(iid, "2026-10-18T00:03:00Z"), "unregistered", "x")})
	if err := w.Flush(ctx); err == nil {
		t.Fatal("an invalid row must fail the flush")
	}
	if w.Pending() != 2 {
		t.Fatalf("requeued %d", w.Pending())
	}
}

func TestTickerPointsMerge(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	at := pytime.ISO(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC))
	point := func(field, value string) map[string]any {
		return map[string]any{
			"instrument_id": "okx:perp_linear:BTC-USDT-SWAP", "token_id": "bitcoin:BTC", "ts": at, "ts_prec": "EXACT",
			field: value, "kind": "MARK_PRICE", "capture_source": "WEBSOCKET",
			"field_sources": map[string]any{field: map[string]any{"as_of": at, "capture_source": "WEBSOCKET"}},
			"recorded_at":   at, "recorded_at_prec": "COLLECTION_ONLY",
		}
	}
	key := "okx:perp_linear:BTC-USDT-SWAP:" + at
	if _, err := s.PutMarketBatch(ctx, "mkt.tickers_24h", []map[string]any{point("mark_price", "100")}, []string{key}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.tickers_24h", []map[string]any{point("index_price", "99")}, []string{key}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "mkt.tickers_24h", key)
	if err != nil || got["mark_price"] != "100" || got["index_price"] != "99" {
		t.Fatalf("merged ticker = %v %v", got, err)
	}
}
