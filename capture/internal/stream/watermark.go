package stream

import (
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// WatermarkS is WATERMARK_S: a socket watermark is sampled at most this often per
// instrument; it only ever lags the socket, so proofs built on it stay conservative.
const WatermarkS = 0.25

// Watermark is the observed exchange-time progress of one instrument's socket.
type Watermark struct {
	TS       time.Time // newest exchange time seen
	Received time.Time // local clock when it was recorded
}

// Watermarks is record_stream_watermark's state.
type Watermarks struct {
	mu    sync.Mutex
	due   map[string]float64
	Marks map[string]Watermark
}

// NewWatermarks is an empty state.
func NewWatermarks() *Watermarks {
	return &Watermarks{due: map[string]float64{}, Marks: map[string]Watermark{}}
}

// Record is record_stream_watermark for a processed frame: monotonic is the float
// seconds of the monotonic clock, now the collection clock.
func (w *Watermarks) Record(inst *venue.Instrument, raw any, monotonic float64, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.due[inst.ID] > monotonic {
		return
	}
	stamps := WatermarkStamps(inst, raw)
	if len(stamps) == 0 {
		return
	}
	w.due[inst.ID] = monotonic + WatermarkS
	var newest time.Time
	for i, s := range stamps {
		t, err := pytime.Parse(s)
		if err != nil {
			return
		}
		if i == 0 || t.After(newest) {
			newest = t
		}
	}
	if previous, ok := w.Marks[inst.ID]; !ok || !newest.Before(previous.TS) {
		w.Marks[inst.ID] = Watermark{TS: newest, Received: now}
	}
}

// Get is the watermark of an instrument.
func (w *Watermarks) Get(iid string) (Watermark, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m, ok := w.Marks[iid]
	return m, ok
}

// Drop forgets an instrument (its member left the socket).
func (w *Watermarks) Drop(iid string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.Marks, iid)
	delete(w.due, iid)
}
