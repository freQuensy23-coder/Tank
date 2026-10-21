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

const ContextS = 10.0

const BookSaveS = 8.0

type Clock interface {
	Now() time.Time
	Monotonic() time.Duration
}

func Seconds(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }

func Wall(t time.Time) float64 { return float64(t.UnixMicro()) / 1e6 }

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

type Confirmer func(inst *venue.Instrument, row map[string]any) (map[string]any, error)

type Pipeline struct {
	Points    *points.Points
	FX        *normalize.FX
	Clock     Clock
	OnTrade   func(inst *venue.Instrument, t *normalize.Trade)
	Confirm   Confirmer
	BookSaveS float64

	Async func(func())

	mu           sync.Mutex
	bookDue      map[string]float64
	contextDue   map[[2]string]float64
	confirmation map[string]bool
}

func New(p *points.Points, fx *normalize.FX, clock Clock, onTrade func(*venue.Instrument, *normalize.Trade), confirm Confirmer) *Pipeline {
	return &Pipeline{
		Points: p, FX: fx, Clock: clock, OnTrade: onTrade, Confirm: confirm, BookSaveS: BookSaveS,
		bookDue: map[string]float64{}, contextDue: map[[2]string]float64{}, confirmation: map[string]bool{},
	}
}

var _ stream.Sink = (*Pipeline)(nil)

func (p *Pipeline) Now() time.Time { return p.Clock.Now() }

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

func (p *Pipeline) SummaryDue(iid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bookDue[iid] <= Wall(p.Now())
}

func listed(inst *venue.Instrument) bool {
	k := inst.Key()
	return k == "binance.spot" || k == "coinbase.spot"
}

func (p *Pipeline) BookUpdated(inst *venue.Instrument, book *books.Book) error {
	if !book.Verified {
		return nil
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

func (p *Pipeline) Liquidation(inst *venue.Instrument, raw map[string]any) error {
	row, err := normalize.LiquidationRecord(inst, raw, p.Now())
	if err != nil {
		return err
	}
	p.Points.Put("mkt.liq_obs", inst.ID+":"+pyjson.SHA256(row), row)
	return nil
}

func (p *Pipeline) Mark(inst *venue.Instrument, m stream.Mark) error {
	p.Points.SaveMark(inst, points.MarkInput{Price: m.Price, TS: m.TS, Index: m.Index, IndexTS: m.IndexTS, Persist: m.Persist})
	return nil
}

func (p *Pipeline) OKXIndex(inst *venue.Instrument, idxPx, ts any, persist func() bool) error {
	p.Points.OKXIndex(inst, idxPx, ts, persist)
	return nil
}

func (p *Pipeline) Funding(inst *venue.Instrument, raw map[string]any, kind string, interval any) error {
	return p.Points.Funding(inst, raw, kind, interval, "WEBSOCKET")
}

func (p *Pipeline) OI(inst *venue.Instrument, raw map[string]any) error {
	row, err := normalize.OIRecord(inst, raw, normalize.OIOptions{}, p.Now())
	if err != nil {
		return err
	}
	p.Points.Put("mkt.oi_obs", inst.ID+":"+row["metric_key"].(string)+":"+row["ts"].(string), row)
	return nil
}
