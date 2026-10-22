package rawstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

const Table = "mkt.trades"

type Inserter interface {
	Insert(ctx context.Context, table string, lines [][]byte) error
}

type Trades struct {
	ch           Inserter
	code, config string
	log          *slog.Logger
	mu           sync.Mutex
	buf          [][]byte
}

func New(ch Inserter, code, config string, log *slog.Logger) *Trades {
	return &Trades{ch: ch, code: code, config: config, log: log}
}

func text(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	}
	return pyjson.Canonical(v)
}

func chTime(v any) any {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return t.UTC().Format("2006-01-02 15:04:05.000000")
}

func (t *Trades) Row(tr *normalize.Trade, version uint64) map[string]any {
	rec := tr.Record()
	row := map[string]any{"_version": version, "code_version": t.code, "config_version": t.config}
	for _, name := range []string{
		"instrument_id", "trade_id", "token_id", "venue", "market_type", "venue_symbol", "side",
		"price", "qty_native", "quote_notional", "notional_usd", "quote_asset", "usd_reason",
		"metric_key", "trade_aggregation", "raw_first_id", "raw_last_id", "sequence",
		"sequence_semantics", "capture_source", "ts_prec", "ts_prec_reason",
		"received_ts_prec", "received_ts_prec_reason",
	} {
		row[name] = text(rec[name])
	}
	row["ts"] = chTime(rec["ts"])
	row["received_ts"] = chTime(rec["received_ts"])
	return row
}

func (t *Trades) Add(tr *normalize.Trade) {
	line, err := json.Marshal(t.Row(tr, uint64(time.Now().UnixNano())))
	if err != nil {
		t.log.Warn("raw trade not stored", "instrument", tr.InstrumentID, "error", err.Error())
		return
	}
	t.mu.Lock()
	t.buf = append(t.buf, line)
	t.mu.Unlock()
}

func (t *Trades) Flush(time.Time) error {
	t.mu.Lock()
	lines := t.buf
	t.buf = nil
	t.mu.Unlock()
	if len(lines) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := t.ch.Insert(ctx, Table, lines); err != nil {
		t.mu.Lock()
		t.buf = append(lines, t.buf...)
		t.mu.Unlock()
		return err
	}
	return nil
}
