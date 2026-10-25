package budget

import (
	"context"
	"sync"
	"time"
)

type Ledger interface {
	AcquireGroup(ctx context.Context, buckets []Bucket, owner, host string) (time.Duration, error)

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

func (m *MemoryLedger) Acquire(ctx context.Context, scope string, capacity, windowS, weight float64, owner, host string) (time.Duration, error) {
	return m.AcquireGroup(ctx, []Bucket{{Key: scope, Capacity: capacity, WindowS: windowS, Weight: weight}}, owner, host)
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
