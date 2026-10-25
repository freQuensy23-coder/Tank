package budget

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

const HostInFlight = 4

func Interval(host string) time.Duration {
	switch host {
	case "api.kraken.com":
		return time.Second
	case "api.exchange.coinbase.com":
		return 210 * time.Millisecond
	case "api.etherscan.io":
		return 350 * time.Millisecond
	}
	return 80 * time.Millisecond
}

func InFlight(host string) int {
	if host == "api.kraken.com" {
		return 1
	}
	return HostInFlight
}

type BannedError struct {
	Host  string
	Until time.Time
}

func (e *BannedError) Error() string {
	return fmt.Sprintf("%s: HTTP 418 IP ban, host closed until %s", e.Host, pytime.ISO(e.Until))
}

type hostGate struct {
	slots *priorityGate
	pace  *priorityGate
	last  time.Time
}

type Gate struct {
	ledger   Ledger
	owner    string
	policies Policies

	Interval func(host string) time.Duration
	InFlight func(host string) int

	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error

	mu     sync.Mutex
	hosts  map[string]*hostGate
	urgent map[string]int
	banned map[string]time.Time
}

func NewGate(l Ledger, owner string, policies Policies) *Gate {
	return &Gate{
		ledger: l, owner: owner, policies: policies, Interval: Interval, InFlight: InFlight,
		now: time.Now, sleep: sleep,
		hosts: map[string]*hostGate{}, urgent: map[string]int{}, banned: map[string]time.Time{},
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
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

func (g *Gate) host(host string) *hostGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	h := g.hosts[host]
	if h == nil {
		h = &hostGate{slots: newPriorityGate(g.InFlight(host)), pace: newPriorityGate(1)}
		g.hosts[host] = h
	}
	return h
}

func (g *Gate) Admit(ctx context.Context, method, host, path string, params map[string]string, body json.RawMessage, priority int) error {
	buckets, err := g.policies.Plan(budgetpolicy.Request{Method: method, Host: host, Path: path, Query: params, Body: body})
	if err != nil {
		return err
	}
	return g.AdmitBuckets(ctx, host, buckets, priority)
}

func (g *Gate) PlanFetch(method, host, path string, params map[string]string, body json.RawMessage) ([]Bucket, error) {
	return g.policies.Plan(budgetpolicy.Request{Method: method, Host: host, Path: path, Query: params, Body: body})
}

func (g *Gate) QueryFields(host, path string) []string {
	return g.policies.QueryFields(host, path)
}

func (g *Gate) AdmitBuckets(ctx context.Context, host string, buckets []Bucket, priority int) error {
	g.mu.Lock()
	until, banned := g.banned[host]
	g.mu.Unlock()
	if banned && time.Now().Before(until) {
		return &BannedError{Host: host, Until: until}
	}
	return g.acquireGroup(ctx, host, buckets, priority)
}

func (g *Gate) acquire(ctx context.Context, host string, b Bucket, priority int) error {
	return g.acquireGroup(ctx, host, []Bucket{b}, priority)
}

func (g *Gate) acquireGroup(ctx context.Context, host string, buckets []Bucket, priority int) error {
	ordered, err := OrderedBuckets(buckets)
	if err != nil {
		return err
	}
	urgent := priority < 0
	if urgent {
		g.mu.Lock()
		for _, b := range ordered {
			g.urgent[b.Name()]++
		}
		g.mu.Unlock()
		defer func() {
			g.mu.Lock()
			for _, b := range ordered {
				name := b.Name()
				if g.urgent[name]--; g.urgent[name] <= 0 {
					delete(g.urgent, name)
				}
			}
			g.mu.Unlock()
		}()
	}
	for {
		g.mu.Lock()
		yielding := false
		wait := 50 * time.Millisecond
		for _, b := range ordered {
			yielding = yielding || (!urgent && g.urgent[b.Name()] > 0)
			wait = min(wait, seconds(b.WindowS))
		}
		g.mu.Unlock()
		if !yielding {
			w, err := g.ledger.AcquireGroup(ctx, ordered, g.owner, host)
			if err != nil {
				return err
			}
			if w <= 0 {
				return nil
			}
			wait = w
		}
		if err := g.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func (g *Gate) Enter(ctx context.Context, host string, priority int) (release func(), err error) {
	h := g.host(host)
	if err := h.slots.enter(ctx, priority); err != nil {
		return nil, err
	}
	if err := h.pace.enter(ctx, priority); err != nil {
		h.slots.release()
		return nil, err
	}
	defer h.pace.release()
	for {
		if err := ctx.Err(); err != nil {
			h.slots.release()
			return nil, err
		}
		g.mu.Lock()
		now := g.now()
		next := h.last.Add(g.Interval(host))
		if h.last.IsZero() || !next.After(now) {
			h.last = now
			g.mu.Unlock()
			break
		}
		g.mu.Unlock()
		if err := g.sleep(ctx, next.Sub(now)); err != nil {
			h.slots.release()
			return nil, err
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.slots.release()
		})
	}, nil
}

func (g *Gate) push(host string, until time.Time) {
	h := g.host(host)
	g.mu.Lock()
	if until.After(h.last) {
		h.last = until
	}
	g.mu.Unlock()
}

func retryAfter(header http.Header, def float64) float64 {
	if v := strings.TrimSpace(header.Get("Retry-After")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func (g *Gate) Observe(ctx context.Context, host string, status int, header http.Header) (retry bool, err error) {
	now := g.now()
	switch status {
	case http.StatusTooManyRequests:
		delay := seconds(min(max(retryAfter(header, 60), 1), 60))
		g.push(host, now.Add(delay))
		if err := g.ledger.Block(ctx, host, time.Now().Add(delay), "HTTP 429", g.owner); err != nil {
			return false, err
		}
		return true, nil
	case http.StatusTeapot:
		delay := seconds(max(retryAfter(header, 120), 60))
		g.push(host, now.Add(delay))
		until := time.Now().Add(delay)
		g.mu.Lock()
		if until.After(g.banned[host]) {
			g.banned[host] = until
		}
		g.mu.Unlock()
		blockErr := g.ledger.Block(ctx, host, until, "HTTP 418", g.owner)
		return false, errors.Join(&BannedError{Host: host, Until: until}, blockErr)
	}
	if host == "api.binance.com" || host == "fapi.binance.com" {
		if used := strings.TrimSpace(header.Get("X-MBX-USED-WEIGHT-1M")); used != "" {
			n, err := strconv.ParseInt(used, 10, 64)
			if err != nil || n < 0 {
				return false, fmt.Errorf("invalid provider used-weight header")
			}
			limit, err := g.policies.DocumentedWeight(host)
			if err != nil {
				return false, err
			}
			if float64(n) >= limit {
				g.push(host, now.Add(time.Minute))
				if err := g.ledger.Block(ctx, host, time.Now().Add(time.Minute), "X-MBX-USED-WEIGHT-1M "+used, g.owner); err != nil {
					return false, err
				}
			}
		}
	}
	return false, nil
}

type priorityGate struct {
	mu       sync.Mutex
	capacity int
	active   int
	waiters  waiterHeap
	sequence uint64
}

type waiter struct {
	priority int
	sequence uint64
	ready    chan struct{}
	granted  bool
	canceled bool
}

type waiterHeap []*waiter

func (h waiterHeap) Len() int { return len(h) }
func (h waiterHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	return h[i].sequence < h[j].sequence
}
func (h waiterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *waiterHeap) Push(x any)   { *h = append(*h, x.(*waiter)) }
func (h *waiterHeap) Pop() any {
	old := *h
	w := old[len(old)-1]
	*h = old[:len(old)-1]
	return w
}

func newPriorityGate(capacity int) *priorityGate {
	return &priorityGate{capacity: max(1, capacity)}
}

func (p *priorityGate) enter(ctx context.Context, priority int) error {
	p.mu.Lock()
	if p.active < p.capacity {
		p.active++
		p.mu.Unlock()
		return nil
	}
	w := &waiter{priority: priority, sequence: p.sequence, ready: make(chan struct{})}
	p.sequence++
	heap.Push(&p.waiters, w)
	p.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		p.mu.Lock()
		granted := w.granted
		w.canceled = true
		p.mu.Unlock()
		if granted {
			p.release()
		}
		return ctx.Err()
	}
}

func (p *priorityGate) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.waiters.Len() > 0 {
		w := heap.Pop(&p.waiters).(*waiter)
		if w.canceled {
			continue
		}
		w.granted = true
		close(w.ready)
		return
	}
	p.active--
}
