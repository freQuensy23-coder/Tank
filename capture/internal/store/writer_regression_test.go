package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type writerBatch struct {
	collection string
	rows       int
	failed     bool
}

type writerTraceKey struct{}

// Observe actual pgx queries without replacing Store or its write behavior.
type writerTrace struct {
	mu         sync.Mutex
	statements map[string]string
	batches    []writerBatch
	inject     func()
	injected   bool
}

func (tr *writerTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	collection, ok := tr.statements[data.SQL]
	if !ok {
		return ctx
	}
	keys, ok := data.Args[0].([]*string)
	if !ok {
		panic("typed writer storage-key argument changed")
	}
	return context.WithValue(ctx, writerTraceKey{}, writerBatch{collection: collection, rows: len(keys)})
}

func (tr *writerTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	batch, ok := ctx.Value(writerTraceKey{}).(writerBatch)
	if !ok {
		return
	}
	batch.failed = data.Err != nil
	tr.mu.Lock()
	tr.batches = append(tr.batches, batch)
	inject := batch.collection == "mkt.klines_1m" && batch.rows == FlushRows && !batch.failed && !tr.injected
	if inject {
		tr.injected = true
	}
	tr.mu.Unlock()
	if inject {
		tr.inject()
	}
}

func (tr *writerTrace) snapshot() []writerBatch {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]writerBatch(nil), tr.batches...)
}

// Live observations that arrive during a chunk preempt remaining backfill chunks.
// A real PostgreSQL constraint failure requeues only the unwritten tail; a newer
// queued replacement wins and retry never resubmits already successful chunks.
func TestWriterLiveChunksAndFailureRetry(t *testing.T) {
	s := open(t, "mkt_shadow_"+suffix())
	ctx := context.Background()
	iid := "writer-live-" + suffix() + ":BTC"
	w := NewWriter(s)
	base := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	tr := &writerTrace{statements: map[string]string{}}
	for _, collection := range []string{"mkt.klines_1m", "mkt.tickers_24h", "mkt.flow_bars_1m"} {
		tr.statements[s.upsert(collection)] = collection
	}
	cfg := s.Pool.Config()
	cfg.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.Pool.Close()
	s.Pool = pool
	// Only this test-owned shadow table is modified; this fails on the real last
	// INSERT, after the preceding chunks have committed.
	if _, err := s.Pool.Exec(ctx, "ALTER TABLE "+s.Table("mkt.klines_1m")+" ADD CONSTRAINT writer_tail_rejected CHECK (c <> 0)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*FlushRows+1; i++ {
		close := "2"
		if i == 2*FlushRows {
			close = "0"
		}
		row := map[string]any{
			"instrument_id": iid, "token_id": "BTC", "open_time": pytime.ISO(base.Add(time.Duration(i) * time.Minute)),
			"o": "1", "h": "3", "l": "1", "c": close, "volume_native": "3", "complete": true,
		}
		w.Sink(points.Row{Collection: "mkt.klines_1m", Key: fmt.Sprintf("%s:k%d", iid, i), Record: row})
	}
	producer := points.New(&pytime.Fixed{T: base}, w.Sink, "writer-regression", "writer-regression", normalize.NewFX())
	inst := &venue.Instrument{ID: iid, TokenID: "BTC", Venue: "hyperliquid", MarketType: "PERP_LINEAR", QuoteAsset: "USDC", Active: true}
	if err := producer.Ticker(inst, map[string]any{"markPx": "2", "dayNtlVlm": "1000", "_capture_observed_at": pytime.ISO(base)}); err != nil {
		t.Fatal(err)
	}
	flowKey := iid + ":live"
	tr.inject = func() {
		w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: flowKey, Record: flowRow(iid, pytime.ISO(base))})
	}
	err = w.Flush(ctx)
	var failure *pgconn.PgError
	if !errors.As(err, &failure) || failure.Code != "23514" || failure.ConstraintName != "writer_tail_rejected" {
		t.Fatalf("expected actual PostgreSQL tail failure, got %v", err)
	}
	expected := []writerBatch{
		{"mkt.tickers_24h", 1, false},
		{"mkt.klines_1m", FlushRows, false},
		{"mkt.flow_bars_1m", 1, false},
		{"mkt.klines_1m", FlushRows, false},
		{"mkt.klines_1m", 1, true},
	}
	if got := tr.snapshot(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("actual SQL batch order = %v; want %v", got, expected)
	}
	tailKey := fmt.Sprintf("%s:k%d", iid, 2*FlushRows)
	if w.Pending() != 1 || !reflect.DeepEqual(w.order, []string{"mkt.klines_1m"}) {
		t.Fatalf("only tail requeued: pending=%d order=%v", w.Pending(), w.order)
	}
	tail := w.queue["mkt.klines_1m"]
	if !reflect.DeepEqual(tail.keys, []string{tailKey}) || tail.rows[tailKey]["c"] != "0" {
		t.Fatalf("wrong queued tail: %v", tail)
	}
	if !w.LastFlush().IsZero() {
		t.Fatal("failed flush was marked complete")
	}
	var count int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM "+s.Table("mkt.klines_1m")+" WHERE instrument_id=$1", iid).Scan(&count); err != nil {
		t.Fatal(err)
	}
	regressionEqual(t, count, 2*FlushRows)
	if missing, err := s.Get(ctx, "mkt.klines_1m", tailKey); err != nil || missing != nil {
		t.Fatalf("failed tail persisted: %v %v", missing, err)
	}
	regressionRow(t, s, "mkt.flow_bars_1m", flowKey)
	regressionRow(t, s, "mkt.tickers_24h", iid+":"+pytime.ISO(base))
	corrected := with(tail.rows[tailKey], "c", "3")
	w.Sink(points.Row{Collection: "mkt.klines_1m", Key: tailKey, Record: corrected})
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	expected = append(expected, writerBatch{"mkt.klines_1m", 1, false})
	if got := tr.snapshot(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("retry replayed successful rows: got %v; want %v", got, expected)
	}
	if w.Pending() != 0 || w.LastFlush().IsZero() {
		t.Fatal("successful retry not fully drained")
	}
	regressionEqual(t, regressionRow(t, s, "mkt.klines_1m", tailKey)["c"], "3")
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM "+s.Table("mkt.klines_1m")+" WHERE instrument_id=$1", iid).Scan(&count); err != nil {
		t.Fatal(err)
	}
	regressionEqual(t, count, 2*FlushRows+1)
}
