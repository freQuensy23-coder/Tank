package rawstore

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/store"
)

func print(id string, ts time.Time) *normalize.Trade {
	return &normalize.Trade{
		InstrumentID: "kraken:spot:BTC/USD", TokenID: "bitcoin:BTC", Venue: "kraken", VenueSymbol: "BTC/USD",
		MarketType: "SPOT", TradeID: id, TS: ts, TSPrec: "EXACT", Side: "BUY",
		Qty: pydec.MustParse("0.5"), Price: pydec.MustParse("84758.80"), QuoteNotional: pydec.MustParse("42379.4"),
		QuoteAsset: "USD", MetricKey: "kraken.spot.trades", CaptureSource: "WS",
	}
}

func TestTradesInsertAndDeduplicate(t *testing.T) {
	raw := os.Getenv("CLICKHOUSE_TEST_URL")
	if raw == "" {
		t.Skip("CLICKHOUSE_TEST_URL not set")
	}
	ch, err := store.NewClickHouse(raw, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ch.Exec(ctx, "TRUNCATE TABLE "+Table); err != nil {
		t.Fatal(err)
	}
	w := New(ch, "tank-capture/test", "cfg", slog.New(slog.NewTextHandler(io.Discard, nil)))
	at := time.Date(2026, 10, 21, 12, 0, 1, 500000000, time.UTC)
	w.Add(print("1", at))
	w.Add(print("2", at.Add(time.Second)))
	if err := w.Flush(at); err != nil {
		t.Fatal(err)
	}
	w.Add(print("1", at))
	if err := w.Flush(at); err != nil {
		t.Fatal(err)
	}
	rows, err := ch.Rows(ctx, "SELECT count() AS n, toString(min(price)) AS p, toString(min(ts)) AS ts FROM "+Table+" FINAL", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := rows[0]["n"]; n == nil || n.(interface{ String() string }).String() != "2" || rows[0]["p"] != "84758.8" || rows[0]["ts"] != "2026-10-21 12:00:01.500000" {
		t.Fatalf("rows = %v", rows)
	}
}
