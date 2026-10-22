package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type writerBatch struct {
	collection string
	rows       int
	failed     bool
}

type writerTrace struct {
	next     http.RoundTripper
	mu       sync.Mutex
	batches  []writerBatch
	reject   func(collection string, body []byte) bool
	inject   func()
	injected bool
}

func (tr *writerTrace) RoundTrip(req *http.Request) (*http.Response, error) {
	query := req.URL.Query().Get("query")
	if !strings.HasPrefix(query, "INSERT INTO ") || req.Body == nil {
		return tr.next.RoundTrip(req)
	}
	collection := strings.Fields(query)[2]
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	batch := writerBatch{collection: collection, rows: bytes.Count(body, []byte{'\n'}) + 1}
	if tr.reject != nil && tr.reject(collection, body) {
		batch.failed = true
		tr.mu.Lock()
		tr.batches = append(tr.batches, batch)
		tr.mu.Unlock()
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader("rejected tail")), Header: http.Header{}, Request: req}, nil
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	resp, err := tr.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	tr.mu.Lock()
	tr.batches = append(tr.batches, batch)
	inject := collection == "mkt.klines_1m" && batch.rows == FlushRows && !tr.injected
	if inject {
		tr.injected = true
	}
	tr.mu.Unlock()
	if inject {
		tr.inject()
	}
	return resp, nil
}

func (tr *writerTrace) snapshot() []writerBatch {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]writerBatch(nil), tr.batches...)
}

func TestWriterLiveChunksAndFailureRetry(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "writer-live:BTC"
	w := NewWriter(s)
	base := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	tr := &writerTrace{next: http.DefaultTransport}
	tr.reject = func(collection string, body []byte) bool {
		return collection == "mkt.klines_1m" && bytes.Contains(body, []byte(`"c":"0"`))
	}
	s.CH.HTTP = &http.Client{Transport: tr, Timeout: time.Minute}

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
	err := w.Flush(ctx)
	if err == nil || !strings.Contains(err.Error(), "rejected tail") {
		t.Fatalf("expected the tail insert to fail, got %v", err)
	}
	expected := []writerBatch{
		{"mkt.tickers_24h", 1, false},
		{"mkt.klines_1m", FlushRows, false},
		{"mkt.flow_bars_1m", 1, false},
		{"mkt.klines_1m", FlushRows, false},
		{"mkt.klines_1m", 1, true},
	}
	if got := tr.snapshot(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("insert order = %v; want %v", got, expected)
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
	count := func() string {
		rows, err := s.CH.Rows(ctx, "SELECT count() AS n FROM mkt.klines_1m FINAL WHERE instrument_id = {iid:String}", map[string]string{"iid": iid})
		if err != nil || len(rows) != 1 {
			t.Fatalf("count: %v %v", rows, err)
		}
		return fmt.Sprint(rows[0]["n"])
	}
	regressionEqual(t, count(), fmt.Sprint(2*FlushRows))
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
	regressionEqual(t, count(), fmt.Sprint(2*FlushRows+1))
}
