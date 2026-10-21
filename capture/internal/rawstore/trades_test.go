package rawstore

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
)

func print(id string, ts time.Time) *normalize.Trade {
	return &normalize.Trade{
		InstrumentID: "kraken:spot:BTC/USD", TokenID: "bitcoin:BTC", Venue: "kraken", VenueSymbol: "BTC/USD",
		MarketType: "SPOT", TradeID: id, TS: ts, TSPrec: "EXACT", Side: "BUY",
		Qty: pydec.MustParse("0.5"), Price: pydec.MustParse("100"), QuoteNotional: pydec.MustParse("50"),
		QuoteAsset: "USD", MetricKey: "kraken.spot.trades", CaptureSource: "STREAM",
	}
}

func rows(t *testing.T, path string) []Row {
	t.Helper()
	var out []Row
	if err := readParquet(path, func(r Row) error { out = append(out, r); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestJournalThenSealMergesAndDeduplicates(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hour := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	w := New(dir, "tank-capture/abc", "cfg1", 2*time.Minute, log)
	w.Add(print("1", hour.Add(time.Minute)))
	w.Add(print("2", hour.Add(2*time.Minute)))
	if err := w.Flush(hour.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raw", "trades", "kraken", "spot", "BTC%2FUSD", "2026-10-21", "14.parquet")
	if _, err := os.Stat(journalOf(path)); err != nil {
		t.Fatalf("journal not written: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("hour sealed before it ended")
	}
	// A restarted process: the duplicate print and a new one, then the hour ends.
	w2 := New(dir, "tank-capture/abc", "cfg1", 2*time.Minute, log)
	w2.Add(print("2", hour.Add(2*time.Minute)))
	w2.Add(print("3", hour.Add(59*time.Minute)))
	if err := w2.Flush(hour.Add(time.Hour + time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("hour sealed inside its grace")
	}
	if err := w2.Flush(hour.Add(time.Hour + 3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got := rows(t, path)
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}
	r := got[0]
	if *r["trade_id"] != "1" || *r["price"] != "100" || *r["code_version"] != "tank-capture/abc" ||
		*r["ts"] != "2026-10-21T14:01:00Z" || r["notional_usd"] != nil {
		t.Fatalf("row = %s %s %s %s", *r["trade_id"], *r["price"], *r["code_version"], *r["ts"])
	}
	if _, err := os.Stat(journalOf(path)); !os.IsNotExist(err) {
		t.Fatal("journal kept after seal")
	}
	// A late print of a sealed hour is merged into the existing file.
	w2.Add(print("4", hour.Add(59*time.Minute)))
	if err := w2.Flush(hour.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n := len(rows(t, path)); n != 4 {
		t.Fatalf("after late print rows = %d, want 4", n)
	}
}
