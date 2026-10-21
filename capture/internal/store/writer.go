package store

import (
	"context"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/points"
)

// FlushRows is FLUSH_ROWS: typed rows per write; a live flush can run between two.
const FlushRows = 500

// LiveFirst is LIVE_FIRST: collections flushed ahead of other typed rows.
var LiveFirst = []string{"mkt.flow_bars_1m", "mkt.tickers_24h"}

func liveFirst(collection string) bool {
	for _, c := range LiveFirst {
		if c == collection {
			return true
		}
	}
	return false
}

// pending is one collection's queued rows: the newest row per key, in first-queued order
// (a Python dict).
type pending struct {
	keys []string
	rows map[string]map[string]any
}

func newPending() *pending { return &pending{rows: map[string]map[string]any{}} }

func (p *pending) set(key string, row map[string]any) {
	if _, ok := p.rows[key]; !ok {
		p.keys = append(p.keys, key)
	}
	p.rows[key] = row
}

func (p *pending) setDefault(key string, row map[string]any) {
	if _, ok := p.rows[key]; !ok {
		p.set(key, row)
	}
}

type queued struct {
	collection string
	rows       *pending
}

// Writer queues typed rows and writes them like MarketService.flush_market: one
// multi-row write per collection and chunk, live bars and tickers first.
type Writer struct {
	store *Store

	mu         sync.Mutex
	queue      map[string]*pending
	order      []string // collections in first-queued order
	lastFlush  time.Time
	flushMutex sync.Mutex
}

// NewWriter is an empty queue writing through s.
func NewWriter(s *Store) *Writer { return &Writer{store: s, queue: map[string]*pending{}} }

// Sink queues a row (points.Sink): the newest row per (collection, key) is written.
func (w *Writer) Sink(r points.Row) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p := w.queue[r.Collection]
	if p == nil {
		p = newPending()
		w.queue[r.Collection] = p
		w.order = append(w.order, r.Collection)
	}
	record := r.Record
	if r.Collection == "mkt.book_summaries" {
		// Summary fields only: the order-book levels are not stored (03 §14; Market Truth
		// never reads them and they were 88% of each row).
		record = make(map[string]any, len(r.Record))
		for k, v := range r.Record {
			if k != "bids" && k != "asks" {
				record[k] = v
			}
		}
	}
	p.set(r.Key, record)
}

// Pending is the number of queued rows not yet taken by a flush.
func (w *Writer) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, p := range w.queue {
		n += len(p.rows)
	}
	return n
}

// LastFlush is when the last flush completed without error (zero before the first).
func (w *Writer) LastFlush() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastFlush
}

// take empties the queue: its collections, live-first ones first (a stable sort).
func (w *Writer) take() []queued {
	var live, rest []queued
	for _, c := range w.order {
		item := queued{c, w.queue[c]}
		if liveFirst(c) {
			live = append(live, item)
		} else {
			rest = append(rest, item)
		}
	}
	w.queue, w.order = map[string]*pending{}, nil
	return append(live, rest...)
}

// takeLive moves the live collections queued during a flush to its front.
func (w *Writer) takeLive() []queued {
	var out []queued
	for _, c := range LiveFirst {
		if p := w.queue[c]; p != nil {
			out = append(out, queued{c, p})
			delete(w.queue, c)
			for i, o := range w.order {
				if o == c {
					w.order = append(w.order[:i], w.order[i+1:]...)
					break
				}
			}
		}
	}
	return out
}

// Flush writes every queued row. On an error the rows not yet written return to the
// queue, where rows queued meanwhile win, and the error is returned.
func (w *Writer) Flush(ctx context.Context) error {
	w.flushMutex.Lock()
	defer w.flushMutex.Unlock()
	w.mu.Lock()
	queue := w.take()
	w.mu.Unlock()
	for len(queue) > 0 {
		head := queue[0]
		n := min(FlushRows, len(head.rows.keys))
		keys := head.rows.keys[:n]
		rows := make([]map[string]any, n)
		for i, k := range keys {
			rows[i] = head.rows.rows[k]
		}
		if _, err := w.store.PutMarketBatch(ctx, head.collection, rows, keys); err != nil {
			w.requeue(queue)
			return err
		}
		for _, k := range keys {
			delete(head.rows.rows, k)
		}
		head.rows.keys = head.rows.keys[n:]
		if len(head.rows.keys) == 0 {
			queue = queue[1:]
		}
		if len(queue) > 0 && !liveFirst(queue[0].collection) {
			w.mu.Lock()
			live := w.takeLive()
			w.mu.Unlock()
			queue = append(live, queue...)
		}
	}
	w.mu.Lock()
	w.lastFlush = time.Now()
	w.mu.Unlock()
	return nil
}

func (w *Writer) requeue(queue []queued) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, item := range queue {
		target := w.queue[item.collection]
		if target == nil {
			target = newPending()
			w.queue[item.collection] = target
			w.order = append(w.order, item.collection)
		}
		for _, k := range item.rows.keys {
			target.setDefault(k, item.rows.rows[k])
		}
	}
}
