package budget

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
)

func OrderedBuckets(buckets []Bucket) ([]Bucket, error) {
	if len(buckets) == 0 {
		return nil, ErrInvalidBudget
	}
	ordered := slices.Clone(buckets)
	for _, b := range ordered {
		if err := b.Validate(); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(ordered, func(a, b Bucket) int { return strings.Compare(a.Name(), b.Name()) })
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].Name() == ordered[i].Name() {
			return nil, errors.New("duplicate request budget bucket")
		}
	}
	return ordered, nil
}

func (m *MemoryLedger) AcquireGroup(ctx context.Context, buckets []Bucket, _, host string) (time.Duration, error) {
	ordered, err := OrderedBuckets(buckets)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if until, ok := m.blocks[host]; ok && host != "" && until.After(now) {
		return max(minWait, until.Sub(now)), nil
	}
	histories := make([][]spend, len(ordered))
	var wait time.Duration
	for i, b := range ordered {
		history := m.windows[b.Name()]
		window := seconds(b.WindowS)
		for len(history) > 0 && !history[0].at.After(now.Add(-window)) {
			history = history[1:]
		}
		histories[i] = history
		used := 0.0
		for _, s := range history {
			used += s.weight
		}
		if used+b.Weight > b.Capacity {
			wait = max(wait, minWait, history[0].at.Add(window).Sub(now))
		}
	}
	if wait > 0 {
		return wait, nil
	}
	for i, b := range ordered {
		m.windows[b.Name()] = append(histories[i], spend{now, b.Weight})
	}
	return 0, nil
}
