package budget

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
)

type vectorBucket struct {
	Key      string
	Name     string
	Capacity float64
	WindowS  float64 `json:"window_s"`
	Weight   float64
}

type vectorPlan struct {
	Host    string
	Path    string
	Params  map[string]string
	Payload json.RawMessage
	Buckets []vectorBucket
	Error   string
}

type vectorWeight struct {
	Host   string
	Path   string
	Params map[string]string
	Weight *float64
	Error  string
}

type vectorName struct {
	Key      string
	Capacity float64
	WindowS  float64 `json:"window_s"`
	Name     string
}

type vectors struct {
	Weights []vectorWeight
	Plans   []vectorPlan
	Names   []vectorName
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	data, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result vectors
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatal("vector fixture must contain one JSON document")
	}
	for _, weight := range result.Weights {
		if (weight.Weight == nil) == (weight.Error == "") {
			t.Fatal("vector requires either expected weight or error")
		}
	}
	for _, plan := range result.Plans {
		if len(plan.Payload) == 0 || plan.Params == nil {
			t.Fatal("vector requires raw request payload and query mapping")
		}
	}
	return result
}

func TestBinanceWeightMatchesPython(t *testing.T) {
	for _, w := range loadVectors(t).Weights {
		got, err := BinanceWeight(w.Host, w.Path, w.Params)
		if w.Error != "" {
			if err == nil || err.Error() != w.Error {
				t.Errorf("%s %s %v: error %v, want %q", w.Host, w.Path, w.Params, err, w.Error)
			}
			continue
		}
		if w.Host == "fapi.binance.com" && w.Path == "/fapi/v1/depth" && len(w.Params) == 0 {
			if w.Weight == nil || *w.Weight != 5 || err != nil || got != 10 {
				t.Fatalf("retained wrong default5 must become documented500-depth weight10: retained=%v got=%v err=%v", w.Weight, got, err)
			}
			continue
		}
		if err != nil || got != *w.Weight {
			t.Errorf("%s %s %v: %v %v, want %v", w.Host, w.Path, w.Params, got, err, *w.Weight)
		}
	}
}

func TestConstraintsMatchPython(t *testing.T) {
	policies := fixturePolicies(t)
	for _, p := range loadVectors(t).Plans {
		body := p.Payload
		method := "GET"
		if p.Host == "api.hyperliquid.xyz" {
			method = "POST"
		}
		got, err := policies.Plan(budgetpolicy.Request{Method: method, Host: p.Host, Path: p.Path, Query: p.Params, Body: body})
		if assertDocumentedPlanTransition(t, p, got, err, body) {
			continue
		}
		if p.Error != "" {
			if err == nil || err.Error() != p.Error {
				t.Errorf("%s %s: error %v, want %q", p.Host, p.Path, err, p.Error)
			}
			continue
		}
		if err != nil || len(got) != len(p.Buckets) {
			t.Errorf("%s %s %v: %v %v, want %v", p.Host, p.Path, p.Payload, got, err, p.Buckets)
			continue
		}
		for i, b := range p.Buckets {
			g := got[i]
			if g.Key != b.Key || g.Capacity != b.Capacity || g.WindowS != b.WindowS || g.Weight != b.Weight || g.Name() != b.Name {
				t.Errorf("%s %s bucket %d: %+v (%s), want %+v", p.Host, p.Path, i, g, g.Name(), b)
			}
		}
	}
}

func TestBucketNameIsPythonRepr(t *testing.T) {
	for _, n := range loadVectors(t).Names {
		if got := BucketName(n.Key, n.WindowS, n.Capacity); got != n.Name {
			t.Errorf("BucketName(%q, %v, %v) = %q, want %q", n.Key, n.WindowS, n.Capacity, got, n.Name)
		}
	}
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestMemoryLedgerRollingWindow(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	l := NewMemoryLedger(c.now)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if wait, err := l.Acquire(ctx, "b", 3, 1, 1, "test", ""); wait != 0 || err != nil {
			t.Fatalf("acquire %d: %v %v", i, wait, err)
		}
		c.advance(100 * time.Millisecond)
	}

	if wait, _ := l.Acquire(ctx, "b", 3, 1, 1, "test", ""); wait != 700*time.Millisecond {
		t.Fatalf("wait %v, want 700ms", wait)
	}
	if wait, _ := l.Acquire(ctx, "b", 3, 1, 2, "test", ""); wait != 700*time.Millisecond {
		t.Fatalf("weighted wait %v", wait)
	}
	c.advance(700 * time.Millisecond)
	if wait, _ := l.Acquire(ctx, "b", 3, 1, 1, "test", ""); wait != 0 {
		t.Fatalf("after the window: wait %v", wait)
	}
	if used := l.Used("b", 1); used != 3 {
		t.Fatalf("used %v, want 3", used)
	}
	if _, err := l.Acquire(ctx, "b", 3, 1, 4, "test", ""); !errors.Is(err, ErrInvalidBudget) {
		t.Fatalf("weight above capacity: %v", err)
	}
	if _, err := l.Acquire(ctx, "b", 0, 1, 1, "test", ""); !errors.Is(err, ErrInvalidBudget) {
		t.Fatalf("zero capacity: %v", err)
	}
}

func TestMemoryLedgerHostBlocks(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	l := NewMemoryLedger(c.now)
	ctx := context.Background()
	_ = l.Block(ctx, "api.binance.com", c.now().Add(30*time.Second), "HTTP 429", "core")
	_ = l.Block(ctx, "api.binance.com", c.now().Add(10*time.Second), "HTTP 429", "capture")
	if wait, _ := l.Acquire(ctx, "b", 10, 60, 1, "test", "api.binance.com"); wait != 30*time.Second {
		t.Fatalf("blocked wait %v", wait)
	}
	if wait, _ := l.Acquire(ctx, "b", 10, 60, 1, "test", "fapi.binance.com"); wait != 0 {
		t.Fatalf("other host waits %v", wait)
	}
	c.advance(30 * time.Second)
	if wait, _ := l.Acquire(ctx, "b", 10, 60, 1, "test", "api.binance.com"); wait != 0 {
		t.Fatalf("after the block: %v", wait)
	}
}

func testGate(t *testing.T, l Ledger, c *clock) (*Gate, *[]time.Duration) {
	g := NewGate(l, "test", fixturePolicies(t))
	var mu sync.Mutex
	var slept []time.Duration
	g.now = c.now
	g.sleep = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
		c.advance(d)
		return nil
	}
	return g, &slept
}

func TestGateAdmitWaitsForTheLedger(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	l := NewMemoryLedger(c.now)
	g, slept := testGate(t, l, c)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := g.Admit(ctx, "GET", "api.kraken.com", "/0/public/Trades", nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	if len(*slept) != 2 || (*slept)[0] != time.Second || (*slept)[1] != time.Second {
		t.Fatalf("slept %v, want two 1 s waits", *slept)
	}
	if err := g.Admit(ctx, "GET", "fapi.binance.com", "/fapi/v1/unknown", nil, nil, 0); err == nil {
		t.Fatal("an unconfigured Binance endpoint must fail closed")
	}
	if used := l.Used("binance.usdm.funding|300.0|250.0", 300); used != 0 {
		t.Fatal(used)
	}
	if err := g.Admit(ctx, "GET", "fapi.binance.com", "/fapi/v1/fundingInfo", nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if used := l.Used("binance.usdm.funding|300.0|250.0", 300); used != 1 {
		t.Fatalf("funding bucket %v", used)
	}
	if used := l.Used("binance.usdm|60.0|1200.0", 60); used != 0 {
		t.Fatalf("a weight-0 endpoint charged the weight bucket: %v", used)
	}
}

func TestGateUrgentYielding(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	l := NewMemoryLedger(c.now)
	g, slept := testGate(t, l, c)
	b := Bucket{"kraken", 1, 1, 1}
	g.urgent[b.Name()] = 1
	ctx, cancel := context.WithCancel(context.Background())
	sleeps := 0
	g.sleep = func(ctx context.Context, d time.Duration) error {
		if sleeps++; sleeps == 5 {
			cancel()
		}
		*slept = append(*slept, d)
		return ctx.Err()
	}
	if err := g.acquire(ctx, "api.kraken.com", b, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire: %v", err)
	}
	if used := l.Used(b.Name(), 1); used != 0 {
		t.Fatalf("a yielding request took %v of the bucket", used)
	}
	for _, d := range *slept {
		if d != 50*time.Millisecond {
			t.Fatalf("yield delay %v, want min(0.05, window)", d)
		}
	}

	if err := g.acquire(context.Background(), "api.kraken.com", b, -1); err != nil {
		t.Fatal(err)
	}
	if used := l.Used(b.Name(), 1); used != 1 {
		t.Fatalf("urgent request not admitted: %v", used)
	}
	delete(g.urgent, b.Name())
	g.sleep = func(ctx context.Context, d time.Duration) error { c.advance(d); return nil }
	if err := g.acquire(context.Background(), "api.kraken.com", b, 0); err != nil {
		t.Fatal(err)
	}
}

func TestGatePriorityOrder(t *testing.T) {
	g := NewGate(NewMemoryLedger(nil), "test", fixturePolicies(t))
	g.Interval = func(string) time.Duration { return 0 }
	ctx := context.Background()
	hold, err := g.Enter(ctx, "api.kraken.com", 0)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	h := g.host("api.kraken.com")
	for i, priority := range []int{2, 0, 1, 0, -1} {
		wg.Add(1)
		go func(i, priority int) {
			defer wg.Done()
			release, err := g.Enter(ctx, "api.kraken.com", priority)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			release()
		}(i, priority)

		for {
			h.slots.mu.Lock()
			n := h.slots.waiters.Len()
			h.slots.mu.Unlock()
			if n == i+1 {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	hold()
	wg.Wait()
	want := []int{4, 1, 3, 2, 0}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("admission order %v, want %v", order, want)
		}
	}
}

func TestGateEnterCancelledFreesTheSlot(t *testing.T) {
	g := NewGate(NewMemoryLedger(nil), "test", fixturePolicies(t))
	g.Interval = func(string) time.Duration { return 0 }
	hold, _ := g.Enter(context.Background(), "api.kraken.com", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := g.Enter(ctx, "api.kraken.com", 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("enter: %v", err)
	}
	hold()
	release, err := g.Enter(context.Background(), "api.kraken.com", 0)
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
	if h := g.host("api.kraken.com"); h.slots.active != 0 {
		t.Fatalf("active %d after release", h.slots.active)
	}
}

func TestGateStartSpacing(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	g, slept := testGate(t, NewMemoryLedger(c.now), c)
	ctx := context.Background()

	first, _ := g.Enter(ctx, "api.exchange.coinbase.com", 0)
	second, _ := g.Enter(ctx, "api.exchange.coinbase.com", 0)
	if len(*slept) != 1 || (*slept)[0] != 210*time.Millisecond {
		t.Fatalf("slept %v", *slept)
	}
	first()
	second()
	c.advance(time.Second)
	third, _ := g.Enter(ctx, "api.exchange.coinbase.com", 0)
	third()
	if len(*slept) != 1 {
		t.Fatalf("a start after the spacing slept: %v", *slept)
	}

	k1, _ := g.Enter(ctx, "api.kraken.com", 0)
	k1()
	k2, _ := g.Enter(ctx, "api.kraken.com", 0)
	k2()
	if got := (*slept)[len(*slept)-1]; got != time.Second {
		t.Fatalf("kraken spacing %v", got)
	}
}

func TestObserve429RetriesAfterRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{{"5", 5 * time.Second}, {"0", time.Second}, {"120", 60 * time.Second}, {"", 60 * time.Second}, {"soon", 60 * time.Second}} {
		c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
		l := NewMemoryLedger(nil)
		g, slept := testGate(t, l, c)
		header := http.Header{}
		if tc.header != "" {
			header.Set("Retry-After", tc.header)
		}
		retry, err := g.Observe(context.Background(), "api.bybit.com", 429, header)
		if !retry || err != nil {
			t.Fatalf("429: retry %v err %v", retry, err)
		}
		until, ok := l.Blocked("api.bybit.com")
		if !ok || time.Until(until) > tc.want || time.Until(until) < tc.want-time.Second {
			t.Fatalf("Retry-After %q: ledger block until %v", tc.header, until)
		}

		release, _ := g.Enter(context.Background(), "api.bybit.com", 0)
		release()
		if len(*slept) != 1 || (*slept)[0] != tc.want+Interval("api.bybit.com") {
			t.Fatalf("Retry-After %q: slept %v, want %v", tc.header, *slept, tc.want)
		}
	}
}

func TestObserve418IsNeverRetried(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{{"", 120 * time.Second}, {"300", 300 * time.Second}, {"10", 60 * time.Second}} {
		l := NewMemoryLedger(nil)
		g := NewGate(l, "test", fixturePolicies(t))
		header := http.Header{}
		if tc.header != "" {
			header.Set("Retry-After", tc.header)
		}
		retry, err := g.Observe(context.Background(), "api.binance.com", 418, header)
		var banned *BannedError
		if retry || !errors.As(err, &banned) || banned.Host != "api.binance.com" {
			t.Fatalf("418: retry %v err %v", retry, err)
		}
		until, ok := l.Blocked("api.binance.com")
		if left := time.Until(until); !ok || left > tc.want || left < tc.want-time.Second {
			t.Fatalf("Retry-After %q: blocked %v", tc.header, left)
		}

		err = g.Admit(context.Background(), "GET", "api.binance.com", "/api/v3/klines", map[string]string{"limit": "3"}, nil, 0)
		if !errors.As(err, &banned) {
			t.Fatalf("admitted during a ban: %v", err)
		}
		other := NewGate(l, "core", fixturePolicies(t))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		err = other.Admit(ctx, "GET", "api.binance.com", "/api/v3/klines", map[string]string{"limit": "3"}, nil, 0)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("another process admitted during a ban: %v", err)
		}
	}
}

func TestObserveUsedWeightHeader(t *testing.T) {
	for _, tc := range []struct {
		host   string
		used   string
		closed bool
	}{
		{"fapi.binance.com", "2400", true}, {"fapi.binance.com", "2399", false},
		{"api.binance.com", "6000", true}, {"api.binance.com", "5999", false}, {"api.binance.com", "", false},
	} {
		l := NewMemoryLedger(nil)
		g := NewGate(l, "test", fixturePolicies(t))
		header := http.Header{}
		if tc.used != "" {
			header.Set("X-MBX-USED-WEIGHT-1M", tc.used)
		}
		retry, err := g.Observe(context.Background(), tc.host, 200, header)
		if retry || err != nil {
			t.Fatalf("%v %v", retry, err)
		}
		until, ok := l.Blocked(tc.host)
		if ok != tc.closed {
			t.Fatalf("%s %s: blocked %v", tc.host, tc.used, ok)
		}
		if ok && (time.Until(until) > time.Minute || time.Until(until) < 59*time.Second) {
			t.Fatalf("block %v", time.Until(until))
		}
	}
}
