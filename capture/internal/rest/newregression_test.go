package rest

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

// Observe a real ledger's blocked attempts without changing admission or accounting.
type observedWaitingLedger struct {
	budget.Ledger
	waiting chan float64
}

func (l observedWaitingLedger) Acquire(ctx context.Context, name string, capacity, windowS, weight float64, owner, host string) (time.Duration, error) {
	wait, err := l.Ledger.Acquire(ctx, name, capacity, windowS, weight, owner, host)
	if err == nil && wait > 0 {
		select {
		case l.waiting <- weight:
		default:
		}
	}
	return wait, err
}

func TestBlockedWeightedRequestsLeaveHostSlotsForBaseline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ledger := budget.NewMemoryLedger(nil)
	name := budget.BucketName("binance.spot", 60, 3000)
	if wait, err := ledger.Acquire(ctx, name, 3000, 60, 2990, "fixture", "api.binance.com"); wait != 0 || err != nil {
		t.Fatalf("prefill %v %v", wait, err)
	}
	waiting := make(chan float64, budget.HostInFlight)
	gate := budget.NewGate(observedWaitingLedger{ledger, waiting}, "test")
	gate.Interval = func(string) time.Duration { return 0 }
	client := NewClient("TankV0/test", gate, func() time.Time { return now })
	exchange := &exchange{responses: []canned{ok("[]")}}
	client.HTTP = &http.Client{Transport: exchange}

	optionalCtx, cancelOptional := context.WithCancel(ctx)
	defer cancelOptional()
	results := make(chan error, budget.HostInFlight)
	for range budget.HostInFlight {
		go func() {
			_, err := client.Do(optionalCtx, "GET", Binance+"/api/v3/depth", map[string]string{"limit": "1000"}, nil, 2)
			results <- err
		}()
	}
	for range budget.HostInFlight {
		select {
		case weight := <-waiting:
			if weight != 50 {
				t.Fatalf("blocked weight %v", weight)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// Four genuinely blocked weight-50 repairs cannot consume all four HTTP slots.
	if _, err := client.Do(ctx, "GET", Binance+"/api/v3/klines", map[string]string{"limit": "3"}, nil, 0); err != nil {
		t.Fatalf("baseline request blocked by optional budget waits: %v", err)
	}
	cancelOptional()
	for range budget.HostInFlight {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled optional request: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	exchange.mu.Lock()
	requests := append([]recorded(nil), exchange.requests...)
	exchange.mu.Unlock()
	if len(requests) != 1 || !strings.HasSuffix(requests[0].URL, "/api/v3/klines") {
		t.Fatalf("requests %v, want only the baseline request", requests)
	}
	if used := ledger.Used(name, 60); used != 2992 {
		t.Fatalf("used %v: blocked repairs must not spend, baseline costs 2", used)
	}
}

func TestFundingHistoryCacheSeparatesRangesWithinSettlementHour(t *testing.T) {
	fundingA := `[{"coin":"BTC","fundingRate":"0.0000125","time":1792574400000}]`
	fundingB := `[{"coin":"BTC","fundingRate":"0.000025","time":1792574400000}]`
	client, exchange, _ := newClient(t, ok(fundingA), ok(fundingB))
	clock := now
	client.Now = func() time.Time { return clock }
	client.monotonic = func() time.Time { return clock }
	ctx := context.Background()
	first := map[string]any{"type": "fundingHistory", "coin": "BTC", "startTime": int64(0)}
	valueA, err := client.Do(ctx, "POST", Hyperliquid, nil, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	observedA := pyval.Map(pyval.List(valueA)[0])["_capture_observed_at"]
	clock = clock.Add(15 * time.Minute) // Still the same 12:00 settlement hour.
	cachedA, err := client.Do(ctx, "POST", Hyperliquid, nil, first, 0)
	if err != nil || len(exchange.requests) != 1 || pyval.Map(pyval.List(cachedA)[0])["_capture_observed_at"] != observedA {
		t.Fatalf("same-range cache changed its observation: %v %v", cachedA, err)
	}
	second := map[string]any{"type": "fundingHistory", "coin": "BTC", "startTime": int64(3600000)}
	valueB, err := client.Do(ctx, "POST", Hyperliquid, nil, second, 0)
	if err != nil || len(exchange.requests) != 2 {
		t.Fatalf("different range reused cache: requests=%d err=%v", len(exchange.requests), err)
	}
	if pyval.Map(pyval.List(valueA)[0])["fundingRate"] != "0.0000125" || pyval.Map(pyval.List(valueB)[0])["fundingRate"] != "0.000025" {
		t.Fatalf("range values aliased: %v %v", valueA, valueB)
	}
	again, err := client.Do(ctx, "POST", Hyperliquid, nil, first, 0)
	if err != nil || len(exchange.requests) != 2 || pyval.Map(pyval.List(again)[0])["_capture_observed_at"] != observedA {
		t.Fatalf("returning to the original range changed cache: %v %v", again, err)
	}
}
