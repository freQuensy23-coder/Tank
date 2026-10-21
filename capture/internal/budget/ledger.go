package budget

import (
	"context"
	"sync"
	"time"
)

type Ledger interface {
	Acquire(ctx context.Context, bucket string, capacity, windowS, weight float64, owner, host string) (time.Duration, error)

	Block(ctx context.Context, host string, until time.Time, reason, owner string) error
}

const minWait = time.Millisecond

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

type spend struct {
	at     time.Time
	weight float64
}

type MemoryLedger struct {
	mu      sync.Mutex
	now     func() time.Time
	windows map[string][]spend
	blocks  map[string]time.Time
}

func NewMemoryLedger(now func() time.Time) *MemoryLedger {
	if now == nil {
		now = time.Now
	}
	return &MemoryLedger{now: now, windows: map[string][]spend{}, blocks: map[string]time.Time{}}
}

func (m *MemoryLedger) Acquire(_ context.Context, bucket string, capacity, windowS, weight float64, _, host string) (time.Duration, error) {
	if err := valid(capacity, weight); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if until, ok := m.blocks[host]; ok && host != "" && until.After(now) {
		return max(minWait, until.Sub(now)), nil
	}
	window := seconds(windowS)
	history := m.windows[bucket]
	for len(history) > 0 && !history[0].at.After(now.Add(-window)) {
		history = history[1:]
	}
	used := 0.0
	for _, s := range history {
		used += s.weight
	}
	if used+weight <= capacity {
		m.windows[bucket] = append(history, spend{now, weight})
		return 0, nil
	}
	m.windows[bucket] = history
	return max(minWait, history[0].at.Add(window).Sub(now)), nil
}

func (m *MemoryLedger) Block(_ context.Context, host string, until time.Time, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous, ok := m.blocks[host]; !ok || until.After(previous) {
		m.blocks[host] = until
	}
	return nil
}

func (m *MemoryLedger) Blocked(host string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	until, ok := m.blocks[host]
	return until, ok
}

func (m *MemoryLedger) Used(bucket string, windowS float64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	used := 0.0
	for _, s := range m.windows[bucket] {
		if s.at.After(now.Add(-seconds(windowS))) {
			used += s.weight
		}
	}
	return used
}
