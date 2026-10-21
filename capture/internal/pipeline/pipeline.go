// Package pipeline turns processed stream frames into typed rows: it implements
// stream.Sink with MarketService's semantics (_save_book's 8 s summary grid on a
// verified book, context_due's 10 s stream-context cadence, liquidation and OI rows,
// marks and funding through the points pipeline). The service and the parity harness
// share it, so the harness checks the production path.
package pipeline

import (
	"math"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/books"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/stream"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// ContextS is CONTEXT_S: streamed mark/index/funding/OI context is stored at most this
// often per instrument (03 §5.1 hot cadence), the newest value is kept in between.
const ContextS = 10.0

// BookSaveS is BOOK_SAVE_S, the default summary grid (thresholds book_save_interval_s).
const BookSaveS = 8.0

// Clock is the pipeline's view of time: the wall clock and a monotonic one, both in
// float seconds so due times round exactly as Python's time.time()/time.monotonic().
type Clock interface {
	Now() time.Time
	Monotonic() time.Duration
}

// Seconds is a duration as Python's float seconds.
func Seconds(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }

// Wall is a time as time.time().
func Wall(t time.Time) float64 { return float64(t.UnixMicro()) / 1e6 }

// FloorDiv is Python float floor division (float_divmod).
func FloorDiv(x, y float64) float64 {
	mod := math.Mod(x, y)
	div := (x - mod) / y
	if mod != 0 && (y < 0) != (mod < 0) {
		div -= 1.0
	}
	if div != 0 {
		floor := math.Floor(div)
		if div-floor > 0.5 {
			floor += 1.0
		}
		return floor
	}
	return math.Copysign(0, x/y)
}

// Confirmer fetches the second book path (03 §5.9) for a listed venue; it returns the
// fields merged into the summary row, or an error recorded as the reason.
type Confirmer func(inst *venue.Instrument, row map[string]any) (map[string]any, error)

// Pipeline implements stream.Sink.
type Pipeline struct {
	Points    *points.Points
	FX        *normalize.FX
	Clock     Clock
	OnTrade   func(inst *venue.Instrument, t *normalize.Trade)
	Confirm   Confirmer
	BookSaveS float64
	// Async runs a confirmation; nil runs it inline (the harness).
	Async func(func())

	mu           sync.Mutex
	bookDue      map[string]float64
	contextDue   map[[2]string]float64
	confirmation map[string]bool
}

// New is a pipeline with the default cadences.
func New(p *points.Points, fx *normalize.FX, clock Clock, onTrade func(*venue.Instrument, *normalize.Trade), confirm Confirmer) *Pipeline {
	return &Pipeline{
		Points: p, FX: fx, Clock: clock, OnTrade: onTrade, Confirm: confirm, BookSaveS: BookSaveS,
		bookDue: map[string]float64{}, contextDue: map[[2]string]float64{}, confirmation: map[string]bool{},
	}
}

var _ stream.Sink = (*Pipeline)(nil)

// Now is the collection clock.
func (p *Pipeline) Now() time.Time { return p.Clock.Now() }

// Trade normalizes a streamed print (source WS) and hands it to the flow owner.
func (p *Pipeline) Trade(inst *venue.Instrument, raw any) error {
	t, err := normalize.NormalizeTrade(inst, raw, p.Now(), "WS", p.FX)
	if err != nil {
		return err
	}
	if p.OnTrade != nil {
		p.OnTrade(inst, t)
	}
	return nil
}

// ContextDue is context_due: true at most once per ContextS per (instrument, kind).
func (p *Pipeline) ContextDue(iid, kind string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := Seconds(p.Clock.Monotonic())
	key := [2]string{iid, kind}
	if p.contextDue[key] > now {
		return false
	}
	p.contextDue[key] = now + ContextS
	return true
}

// SummaryDue is summary_due: the instrument's next summary grid point has passed.
func (p *Pipeline) SummaryDue(iid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bookDue[iid] <= Wall(p.Now())
}

// listed are the venues with a listed independent REST book (02 §4.1, §4.3).
func listed(inst *venue.Instrument) bool {
	k := inst.Key()
	return k == "binance.spot" || k == "coinbase.spot"
}

// BookUpdated is _save_book: summaries on a fixed wall-clock grid from the first
// verified update at or after each grid point; listed venues get the REST second path
// in the background, one confirmation in flight per instrument.
func (p *Pipeline) BookUpdated(inst *venue.Instrument, book *books.Book) error {
	if !book.Verified {
		return nil // summarize only a checksum-verified state (the next one is <= 1 s away)
	}
	now := Wall(p.Now())
	p.mu.Lock()
	if p.bookDue[inst.ID] > now {
		p.mu.Unlock()
		return nil
	}
	interval := p.BookSaveS
	p.bookDue[inst.ID] = (FloorDiv(now, interval) + 1) * interval
	p.mu.Unlock()
	frozen := book.Frozen()
	row, err := frozen.Summary(p.Now())
	if err != nil {
		return err
	}
	if !frozen.IntegrityOK && book.IntegrityOK {
		book.IntegrityOK, book.GapReason = false, frozen.GapReason
	}
	key := inst.ID + ":" + row["exchange_ts"].(string)
	row["second_path_confirmed"] = false
	row["second_path_reason"] = "NO_LISTED_SECOND_PATH"
	if listed(inst) {
		row["second_path_reason"] = "REST_CONFIRMATION_PENDING"
	}
	p.Points.Put("mkt.book_summaries", key, row)
	if row["integrity_ok"] != true || !listed(inst) || p.Confirm == nil {
		return nil
	}
	p.mu.Lock()
	if p.confirmation[inst.ID] {
		p.mu.Unlock()
		return nil
	}
	p.confirmation[inst.ID] = true
	p.mu.Unlock()
	confirm := func() {
		fields, err := p.Confirm(inst, row)
		updated := map[string]any{}
		for k, v := range row {
			updated[k] = v
		}
		if err != nil {
			updated["second_path_confirmed"] = false
			updated["second_path_reason"] = err.Error()
		} else {
			for k, v := range fields {
				updated[k] = v
			}
		}
		p.Points.Put("mkt.book_summaries", key, updated)
		p.mu.Lock()
		delete(p.confirmation, inst.ID)
		p.mu.Unlock()
	}
	if p.Async != nil {
		p.Async(confirm)
	} else {
		confirm()
	}
	return nil
}

// Liquidation is save_liquidation: one row per distinct normalized liquidation.
func (p *Pipeline) Liquidation(inst *venue.Instrument, raw map[string]any) error {
	row, err := normalize.LiquidationRecord(inst, raw, p.Now())
	if err != nil {
		return err
	}
	p.Points.Put("mkt.liq_obs", inst.ID+":"+pyjson.SHA256(row), row)
	return nil
}

// Mark is save_mark from a stream.
func (p *Pipeline) Mark(inst *venue.Instrument, m stream.Mark) error {
	p.Points.SaveMark(inst, points.MarkInput{Price: m.Price, TS: m.TS, Index: m.Index, IndexTS: m.IndexTS, Persist: m.Persist})
	return nil
}

// OKXIndex is the OKX index-tickers branch.
func (p *Pipeline) OKXIndex(inst *venue.Instrument, idxPx, ts any, persist func() bool) error {
	p.Points.OKXIndex(inst, idxPx, ts, persist)
	return nil
}

// Funding is save_funding: a streamed funding estimate.
func (p *Pipeline) Funding(inst *venue.Instrument, raw map[string]any, kind string, interval any) error {
	return p.Points.Funding(inst, raw, kind, interval, "WEBSOCKET")
}

// OI is a streamed open-interest point.
func (p *Pipeline) OI(inst *venue.Instrument, raw map[string]any) error {
	row, err := normalize.OIRecord(inst, raw, normalize.OIOptions{}, p.Now())
	if err != nil {
		return err
	}
	p.Points.Put("mkt.oi_obs", inst.ID+":"+row["metric_key"].(string)+":"+row["ts"].(string), row)
	return nil
}
