package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

func regressionStore(t *testing.T) (*Store, string) {
	t.Helper()
	prefix := os.Getenv("TANK_CAPTURE_REGRESSION_ID")
	if prefix == "" {
		return chStore(t), "producer"
	}
	raw := os.Getenv("CLICKHOUSE_TEST_URL")
	if raw == "" {
		t.Skip("CLICKHOUSE_TEST_URL not set")
	}
	ch, err := NewClickHouse(raw, "")
	if err != nil {
		t.Fatal(err)
	}
	return &Store{CH: ch}, prefix
}

func regressionCount(t *testing.T, s *Store, query string, params map[string]string) string {
	t.Helper()
	rows, err := s.CH.Rows(context.Background(), query, params)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%s: %v %v", query, rows, err)
	}
	return fmt.Sprint(rows[0]["n"])
}

func regressionEqual(t *testing.T, got, want any) {
	t.Helper()
	if pyjson.Canonical(got) != pyjson.Canonical(want) {
		t.Fatalf("got %s; want %s", pyjson.Canonical(got), pyjson.Canonical(want))
	}
}

func regressionRow(t *testing.T, s *Store, collection, key string) map[string]any {
	t.Helper()
	row, err := s.Get(context.Background(), collection, key)
	if err != nil || row == nil {
		t.Fatalf("%s/%s: %v %v", collection, key, row, err)
	}
	return row
}

func regressionMap(t *testing.T, value any) map[string]any {
	t.Helper()
	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object, got %T: %v", value, value)
	}
	return m
}

func TestProducerPrecisionFundingAndMissingUSD(t *testing.T) {
	s, prefix := regressionStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 21, 12, 1, 2, 123000000, time.UTC)
	clock := &pytime.Fixed{T: now}
	received := pytime.ISO(now)
	timestamp := now.UnixMilli()
	bucket := timestamp / 300000 * 300000
	writer := NewWriter(s)
	p := points.New(clock, writer.Sink, "regression-config", "regression-code", normalize.NewFX())
	samples := []struct {
		venue, kind, precision string
		raw                    map[string]any
	}{
		{"binance", "current", "EXACT", map[string]any{"openInterest": "123.000000000000000001", "time": timestamp}},
		{"binance", "history", "BUCKET(300s)", map[string]any{"sumOpenInterest": "124", "timestamp": bucket}},
		{"okx", "current", "EXACT", map[string]any{"oiCcy": "125", "ts": timestamp}},
		{"bybit", "history", "EXACT", map[string]any{"singleOpenInterest": "126", "timestamp": bucket}},
		{"hyperliquid", "current", "COLLECTION_ONLY", map[string]any{"openInterest": "127", "_capture_observed_at": received}},
	}
	var instruments []*venue.Instrument
	var first map[string]any
	for _, sample := range samples {
		inst := &venue.Instrument{ID: prefix + ":" + sample.venue + ":" + sample.kind, TokenID: "test:PRECISION", Venue: sample.venue, Symbol: "BTCUSDT", MarketType: "PERP_LINEAR", QuoteAsset: "USDT", ContractMultiplier: "1", Active: true}
		instruments = append(instruments, inst)
		row, err := normalize.OIRecord(inst, sample.raw, normalize.OIOptions{Received: &now}, now)
		if err != nil {
			t.Fatal(err)
		}
		regressionEqual(t, row["ts_prec"], sample.precision)
		regressionEqual(t, row["timestamp_precision"], sample.precision)
		key := inst.ID + ":" + row["ts"].(string)
		p.Put("mkt.oi_obs", key, row)
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		saved := regressionRow(t, s, "mkt.oi_obs", key)
		regressionEqual(t, saved["received_ts"], received)
		if first == nil {
			first = row
		}
		if sample.venue == "hyperliquid" {
			regressionEqual(t, saved["ts"], received)
			regressionEqual(t, saved["sidedness"], "UNKNOWN")
			regressionEqual(t, saved["synchronized_eligible"], false)
		}
	}
	regressionEqual(t, regressionCount(t, s, "SELECT count() AS n FROM mkt.oi_obs FINAL WHERE startsWith(instrument_id, {p:String}) AND ts_prec = timestamp_precision", map[string]string{"p": prefix + ":"}), "5")
	contradictory := with(first, "ts_prec", "COLLECTION_ONLY")
	if _, err := s.PutMarketBatch(ctx, "mkt.oi_obs", []map[string]any{contradictory}, nil); err == nil || !strings.Contains(strings.ToLower(err.Error()), "conflict") {
		t.Fatalf("conflicting precision must fail: %v", err)
	}
	funding := [][2]map[string]any{
		{{"fundingRate": ".0001", "fundingTime": timestamp}, {"lastFundingRate": ".0002", "time": timestamp}},
		{{"fundingRate": ".0001", "fundingTime": timestamp}, {"fundingRate": ".0002", "ts": timestamp}},
		{{"fundingRate": ".0001", "fundingRateTimestamp": timestamp}, {"fundingRate": ".0002"}},
		{{"fundingRate": ".0001", "time": timestamp}, {"funding": ".0002", "_capture_observed_at": received}},
	}
	for i, inst := range []*venue.Instrument{instruments[0], instruments[2], instruments[3], instruments[4]} {
		for j, kind := range []string{"SETTLED", "CURRENT_ESTIMATE"} {
			row, err := normalize.FundingRecord(inst, funding[i][j], kind, "8", &now, now)
			if err != nil {
				t.Fatal(err)
			}
			if row["as_of"] == nil || row["as_of"] == "" {
				t.Fatal("funding as_of absent")
			}
			if kind == "SETTLED" {
				regressionEqual(t, row["as_of"], row["funding_time"])
				regressionEqual(t, row["as_of"], row["settlement_time"])
			}
			p.Put("mkt.funding_obs", inst.ID+":"+kind, row)
		}
	}
	liq, err := normalize.LiquidationRecord(instruments[0], map[string]any{"S": "SELL", "q": "1", "ap": "100", "T": timestamp}, now)
	if err != nil {
		t.Fatal(err)
	}
	regressionEqual(t, liq["notional_usd"], nil)
	regressionEqual(t, regressionMap(t, liq["null_reasons"])["notional_usd"], "INCOMPLETE")
	p.Put("mkt.liq_obs", prefix+":liq", liq)
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestProducerTickerFundingIdentityAfterRestart(t *testing.T) {
	s, prefix := regressionStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	clock := &pytime.Fixed{T: now}
	point := pytime.ISO(now.Add(-10 * time.Second))
	older := pytime.ISO(now.Add(-11 * time.Second))
	later := pytime.ISO(now.Add(-9 * time.Second))
	writer := NewWriter(s)
	newPoints := func() *points.Points {
		return points.New(clock, writer.Sink, "regression-config", "regression-code", normalize.NewFX())
	}
	p := newPoints()
	inst := &venue.Instrument{ID: prefix + ":HL", TokenID: prefix + ":BTC", Venue: "hyperliquid", Symbol: "BTC", MarketType: "PERP_LINEAR", QuoteAsset: "USDC", Active: true}
	legacy := inst.Clone()
	legacy.ID = prefix + ":legacy"
	raw := map[string]any{"markPx": "100", "oraclePx": "99", "dayNtlVlm": "1000", "funding": ".001", "_capture_observed_at": point}
	ticker := func(producer *points.Points, instrument *venue.Instrument, value map[string]any) {
		t.Helper()
		if err := producer.Ticker(instrument, value); err != nil {
			t.Fatal(err)
		}
	}
	flush := func() {
		t.Helper()
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mark := func(producer *points.Points, instrument *venue.Instrument, price, ts, index, indexTS, source string) {
		producer.SaveMark(instrument, points.MarkInput{Price: price, TS: ts, Index: index, IndexTS: indexTS, Source: source, Received: point, Persist: true})
	}
	ticker(p, inst, raw)
	mark(p, inst, "100", point, "99", point, "PUBLIC_REST")
	key := inst.ID + ":" + point
	flush()
	original := regressionRow(t, s, "mkt.tickers_24h", key)
	clock.T = now.Add(time.Second)
	for i := 0; i < 2; i++ {
		ticker(p, inst, raw)
		mark(p, inst, "100", point, "99", point, "PUBLIC_REST")
	}
	flush()
	regressionEqual(t, regressionRow(t, s, "mkt.tickers_24h", key)["recorded_at"], original["recorded_at"])

	clock.T = now.Add(2 * time.Second)
	restarted := newPoints()
	restored, err := s.LatestRecords(ctx, "mkt.tickers_24h", []string{"instrument_id"}, "ts", []string{inst.ID})
	if err != nil || len(restored) != 1 {
		t.Fatalf("restore: %v %v", restored, err)
	}
	restarted.Restore("mkt.tickers_24h", key, restored[0])
	mark(restarted, inst, "90", older, "99", point, "PUBLIC_REST")
	ticker(restarted, inst, raw)
	flush()
	stored := regressionRow(t, s, "mkt.tickers_24h", key)
	for field, want := range map[string]any{"mark_price": "100", "mark_as_of": point, "index_price": "99", "quote_volume": "1000", "ts": point, "received_ts": point, "recorded_at": original["recorded_at"]} {
		regressionEqual(t, stored[field], want)
	}
	regressionEqual(t, restarted.Cached("mkt.tickers_24h", key)["mark_as_of"], point)
	mark(restarted, inst, "80", older, "101", later, "PUBLIC_REST")
	mark(restarted, inst, "70", older, "98", older, "PUBLIC_REST")
	flush()
	newest := regressionRow(t, s, "mkt.tickers_24h", inst.ID+":"+later)
	for field, want := range map[string]any{"mark_price": "100", "index_price": "101", "mark_as_of": point, "index_as_of": later} {
		regressionEqual(t, newest[field], want)
	}
	p.Put("mkt.tickers_24h", legacy.ID+":mark:"+point, with(original, "instrument_id", legacy.ID))
	ticker(restarted, legacy, raw)
	flush()
	regressionEqual(t, regressionRow(t, s, "mkt.tickers_24h", legacy.ID+":"+point)["index_price"], "99")
	clocked := inst.Clone()
	clocked.ID = prefix + ":clock-provenance"
	mark(p, clocked, "200", older, "201", point, "WEBSOCKET")
	clockKey := clocked.ID + ":" + point
	ticker(p, clocked, with(raw, "markPx", "199"))
	flush()
	clockRow := regressionRow(t, s, "mkt.tickers_24h", clockKey)
	for field, want := range map[string]any{"mark_price": "199", "mark_as_of": point, "index_price": "201", "index_as_of": point, "capture_source": nil, "capture_sources": []any{"PUBLIC_REST", "WEBSOCKET"}} {
		regressionEqual(t, clockRow[field], want)
	}
	sources := regressionMap(t, clockRow["field_sources"])
	regressionEqual(t, regressionMap(t, sources["mark_price"])["clock_basis"], "COLLECTION_TIME")
	regressionEqual(t, regressionMap(t, sources["mark_price"])["capture_source"], "PUBLIC_REST")
	regressionEqual(t, regressionMap(t, sources["index_price"])["capture_source"], "WEBSOCKET")
	p.SaveTickerObservation(clocked, map[string]any{"instrument_id": clocked.ID, "token_id": clocked.TokenID, "ts": point, "mark_price": "198", "capture_source": "PUBLIC_REST", "received_ts": point})
	flush()
	unclocked := regressionRow(t, s, "mkt.tickers_24h", clockKey)
	regressionEqual(t, unclocked["mark_price"], "199")
	regressionEqual(t, unclocked["mark_as_of"], point)
	unknown := regressionMap(t, regressionMap(t, unclocked["unclocked_components"])["mark_price"])
	regressionEqual(t, unknown["value"], "198")
	regressionEqual(t, unknown["as_of"], nil)
	regressionEqual(t, unknown["reason"], "SOURCE_CLOCK_UNAVAILABLE")
	regressionEqual(t, regressionMap(t, unclocked["field_sources"])["index_price"], sources["index_price"])
	mark(p, clocked, "190", older, "191", older, "WEBSOCKET")
	flush()
	retained := regressionRow(t, s, "mkt.tickers_24h", clockKey)
	regressionEqual(t, retained["mark_price"], "199")
	regressionEqual(t, retained["index_price"], "201")
	for _, field := range []string{"mark_price", "index_price"} {
		regressionEqual(t, regressionMap(t, retained["field_sources"])[field], sources[field])
	}
	fund := func(producer *points.Points, value map[string]any, kind, source string) {
		t.Helper()
		if err := producer.Funding(inst, value, kind, "1", source); err != nil {
			t.Fatal(err)
		}
	}
	fund(p, raw, "CURRENT_ESTIMATE", "PUBLIC_REST")
	wsReceived := pytime.ISO(pytime.MustParse(point).Add(time.Millisecond))
	ws := map[string]any{"funding": ".002", "time": point, "_capture_observed_at": wsReceived}
	fund(p, ws, "CURRENT_ESTIMATE", "WEBSOCKET")
	fundKey := inst.ID + ":CURRENT_ESTIMATE:" + point
	flush()
	funding := regressionRow(t, s, "mkt.funding_obs", fundKey)
	regressionEqual(t, regressionMap(t, funding["source_disagreement"])["present"], true)
	observations := regressionMap(t, funding["source_observations"])
	for source, want := range map[string]string{"PUBLIC_REST": "0.001", "WEBSOCKET": "0.002"} {
		observation := regressionMap(t, observations[source])
		regressionEqual(t, observation["rate"], want)
		if observation["recorded_at"] == nil || observation["recorded_at"] == "" {
			t.Fatal("source recorded_at absent")
		}
	}
	regressionEqual(t, regressionMap(t, observations["PUBLIC_REST"])["received_ts"], point)
	regressionEqual(t, regressionMap(t, observations["WEBSOCKET"])["received_ts"], wsReceived)
	clock.T = now.Add(3 * time.Second)
	restarted.Restore("mkt.funding_obs", fundKey, funding)
	fund(restarted, ws, "CURRENT_ESTIMATE", "WEBSOCKET")
	flush()
	regressionEqual(t, regressionRow(t, s, "mkt.funding_obs", fundKey)["recorded_at"], funding["recorded_at"])
	settled := map[string]any{"fundingRate": ".0001", "time": point, "_capture_observed_at": point}
	fund(p, settled, "SETTLED", "PUBLIC_REST")
	fund(restarted, settled, "SETTLED", "WEBSOCKET")
	flush()
	regressionEqual(t, regressionCount(t, s, "SELECT count() AS n FROM mkt.tickers_24h FINAL WHERE instrument_id IN ({a:String}, {b:String})", map[string]string{"a": inst.ID, "b": legacy.ID}), "3")
	regressionEqual(t, regressionCount(t, s, "SELECT count() AS n FROM mkt.funding_obs FINAL WHERE instrument_id = {a:String}", map[string]string{"a": inst.ID}), "2")
}
