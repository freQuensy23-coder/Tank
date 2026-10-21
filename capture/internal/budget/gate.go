package budget

import (
	"container/heap"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

// HostInFlight is HOST_IN_FLIGHT: requests in flight per host (Kraken's counter is serial).
const HostInFlight = 4

// Interval is the request start spacing per host of MarketService._request_once.
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

// InFlight is the PriorityGate capacity per host.
func InFlight(host string) int {
	if host == "api.kraken.com" {
		return 1
	}
	return HostInFlight
}

// UsedWeightLimit is the whole-IP X-MBX-USED-WEIGHT-1M at which a Binance host is
// closed for a minute (the documented limit; Tank keeps itself to half of it).
func UsedWeightLimit(host string) int {
	if host == "fapi.binance.com" {
		return 2400
	}
	return 6000
}

// BannedError is a 418: the host is closed until Until and the request is never retried.
type BannedError struct {
	Host  string
	Until time.Time
}

func (e *BannedError) Error() string {
	return fmt.Sprintf("%s: HTTP 418 IP ban, host closed until %s", e.Host, pytime.ISO(e.Until))
}

type hostGate struct {
	slots *priorityGate
	last  time.Time // _last_request[host]: the latest reserved start or response
}

// Gate is the per-process admission in front of the shared ledger: one PriorityGate per
// host, the host's start spacing, and SharedRequestBudgets' urgent yielding.
type Gate struct {
	ledger Ledger
	owner  string

	// Interval and InFlight are the start spacing and concurrency per host.
	Interval func(host string) time.Duration
	InFlight func(host string) int

	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error

	mu     sync.Mutex
	hosts  map[string]*hostGate
	urgent map[string]int       // bucket -> waiting requests of negative priority
	banned map[string]time.Time // host -> end of a 418 ban this process received
}

// NewGate is admission through ledger l for owner (the process named in the ledger).
func NewGate(l Ledger, owner string) *Gate {
	return &Gate{
		ledger: l, owner: owner, Interval: Interval, InFlight: InFlight,
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
		h = &hostGate{slots: newPriorityGate(g.InFlight(host))}
		g.hosts[host] = h
	}
	return h
}

// Admit is admit_budget: every bucket of the request, in order, from the shared ledger.
// It holds no host slot while it waits. A host under a 418 ban this process received
// fails at once with its BannedError; other processes' blocks are waited out.
func (g *Gate) Admit(ctx context.Context, host, path string, params map[string]string, payload map[string]any, priority int) error {
	buckets, err := Constraints(host, path, params, payload)
	if err != nil {
		return err
	}
	g.mu.Lock()
	until, banned := g.banned[host]
	g.mu.Unlock()
	if banned && time.Now().Before(until) {
		return &BannedError{Host: host, Until: until}
	}
	for _, b := range buckets {
		if err := g.acquire(ctx, host, b, priority); err != nil {
			return err
		}
	}
	return nil
}

// acquire is SharedRequestBudgets.acquire on the ledger: while a request with a negative
// priority waits for a bucket, requests of priority >= 0 leave its capacity to it.
func (g *Gate) acquire(ctx context.Context, host string, b Bucket, priority int) error {
	if err := valid(b.Capacity, b.Weight); err != nil {
		return err
	}
	name := b.Name()
	urgent := priority < 0
	if urgent {
		g.mu.Lock()
		g.urgent[name]++
		g.mu.Unlock()
		defer func() {
			g.mu.Lock()
			if g.urgent[name]--; g.urgent[name] <= 0 {
				delete(g.urgent, name)
			}
			g.mu.Unlock()
		}()
	}
	for {
		g.mu.Lock()
		yielding := !urgent && g.urgent[name] > 0
		g.mu.Unlock()
		wait := min(50*time.Millisecond, seconds(b.WindowS))
		if !yielding {
			w, err := g.ledger.Acquire(ctx, name, b.Capacity, b.WindowS, b.Weight, g.owner, host)
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

// Enter takes a slot of host (lowest priority number first) and waits for the host's next
// start: each start is reserved before sleeping, so concurrent holders never burst.
// release records the response time and frees the slot.
func (g *Gate) Enter(ctx context.Context, host string, priority int) (release func(), err error) {
	h := g.host(host)
	if err := h.slots.enter(ctx, priority); err != nil {
		return nil, err
	}
	g.mu.Lock()
	now := g.now()
	start := now
	if next := h.last.Add(g.Interval(host)); !h.last.IsZero() && next.After(start) {
		start = next
	}
	h.last = start
	g.mu.Unlock()
	if start.After(now) {
		if err := g.sleep(ctx, start.Sub(now)); err != nil {
			h.slots.release()
			return nil, err
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if t := g.now(); t.After(h.last) {
				h.last = t
			}
			g.mu.Unlock()
			h.slots.release()
		})
	}, nil
}

// push moves the host's next start to at least until (plus its spacing).
func (g *Gate) push(host string, until time.Time) {
	h := g.host(host)
	g.mu.Lock()
	if until.After(h.last) {
		h.last = until
	}
	g.mu.Unlock()
}

// retryAfter is float(Retry-After) seconds, def when absent or unreadable.
func retryAfter(header http.Header, def float64) float64 {
	if v := strings.TrimSpace(header.Get("Retry-After")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// Observe applies a response to host. A 429 closes the host, here and for every process,
// for Retry-After clamped to [1, 60] s and asks for a retry. A 418 closes it for
// Retry-After (120 s when absent, at least 60 s) and is never retried. A Binance
// X-MBX-USED-WEIGHT-1M at the venue limit closes the host for a minute; that response
// is still used. Ledger failures leave the local block in place.
func (g *Gate) Observe(ctx context.Context, host string, status int, header http.Header) (retry bool, err error) {
	now := g.now()
	switch status {
	case http.StatusTooManyRequests:
		delay := seconds(min(max(retryAfter(header, 60), 1), 60))
		g.push(host, now.Add(delay))
		_ = g.ledger.Block(ctx, host, time.Now().Add(delay), "HTTP 429", g.owner)
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
		_ = g.ledger.Block(ctx, host, until, "HTTP 418", g.owner)
		return false, &BannedError{Host: host, Until: until}
	}
	if used := strings.TrimSpace(header.Get("X-MBX-USED-WEIGHT-1M")); used != "" {
		if n, err := strconv.Atoi(used); err == nil && n >= UsedWeightLimit(host) {
			g.push(host, now.Add(time.Minute))
			_ = g.ledger.Block(ctx, host, time.Now().Add(time.Minute), "X-MBX-USED-WEIGHT-1M "+used, g.owner)
		}
	}
	return false, nil
}

// priorityGate is rate_gate.PriorityGate: at most capacity holders; waiters are admitted
// lowest priority number first, in arrival order within a priority.
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
			p.release() // the slot had passed to this waiter: hand it on
		}
		return ctx.Err()
	}
}

// release passes the slot to the next live waiter, or frees it.
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
