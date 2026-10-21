package stream

import (
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const WatermarkS = 0.25

type Watermark struct {
	TS       time.Time
	Received time.Time
}

type Watermarks struct {
	mu    sync.Mutex
	due   map[string]float64
	Marks map[string]Watermark
}

func NewWatermarks() *Watermarks {
	return &Watermarks{due: map[string]float64{}, Marks: map[string]Watermark{}}
}

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

func (w *Watermarks) Get(iid string) (Watermark, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m, ok := w.Marks[iid]
	return m, ok
}

func (w *Watermarks) Drop(iid string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.Marks, iid)
	delete(w.due, iid)
}
