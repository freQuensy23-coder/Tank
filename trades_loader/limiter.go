package main

import (
	"context"
	"sync"
	"time"
)

// Limiter spaces requests to one venue by a fixed minimum interval.
type Limiter struct {
	mu   sync.Mutex
	next time.Time
	gap  time.Duration
}

func NewLimiter(gap time.Duration) *Limiter { return &Limiter{gap: gap} }

func (l *Limiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	now := time.Now()
	at := l.next
	if at.Before(now) {
		at = now
	}
	l.next = at.Add(l.gap)
	l.mu.Unlock()
	d := time.Until(at)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Backoff pushes the venue's next slot out, for 429/418 answers.
func (l *Limiter) Backoff(d time.Duration) {
	l.mu.Lock()
	if n := time.Now().Add(d); n.After(l.next) {
		l.next = n
	}
	l.mu.Unlock()
}
