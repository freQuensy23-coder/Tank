package rest

import (
	"context"
	"strings"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

// Source discovery goes through Fetch, so it must charge the same venue ledger
// as live market capture rather than an independent request-count allowance.
func TestSourceDiscoveryFetchChargesSharedMarketWeights(t *testing.T) {
	cases := []struct {
		name, url, bucket, otherBucket string
		weight                         float64
	}{
		{"spot", Binance + "/api/v3/exchangeInfo", "binance.spot", "binance.usdm", 20},
		{"usdm", BinanceUSDM + "/fapi/v1/exchangeInfo", "binance.usdm", "binance.spot", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, exchange, ledger := newClient(t, ok("{}"))
			got, err := client.Fetch(context.Background(), "GET", tc.url, nil, nil, nil, -1)
			if err != nil || got == nil || got.Status != 200 || string(got.Body) != "{}" {
				t.Fatalf("source discovery response: %+v %v", got, err)
			}
			capacity := float64(3000)
			if tc.bucket == "binance.usdm" {
				capacity = 1200
			}
			name := budget.BucketName(tc.bucket, 60, capacity)
			if used := ledger.Used(name, 60); used != tc.weight {
				t.Fatalf("source discovery charged %v in %s, want %v", used, name, tc.weight)
			}
			otherCapacity := float64(3000)
			if tc.otherBucket == "binance.usdm" {
				otherCapacity = 1200
			}
			if used := ledger.Used(budget.BucketName(tc.otherBucket, 60, otherCapacity), 60); used != 0 {
				t.Fatalf("source discovery spent the other venue's budget: %v", used)
			}
			if len(exchange.requests) != 1 || exchange.requests[0].Method != "GET" || exchange.requests[0].URL != tc.url {
				t.Fatalf("source discovery requests: %+v", exchange.requests)
			}
		})
	}
}

// The strict exchange API client retains the previous unknown-weight refusal.
// Raw HTTP_FETCH additionally supports announcement pages, whose unconfigured
// paths use the documented conservative venue bucket instead of running free.
func TestSourceDiscoveryUnknownWeightsRemainBounded(t *testing.T) {
	t.Run("strict API refusal", func(t *testing.T) {
		client, exchange, ledger := newClient(t)
		_, err := client.Do(context.Background(), "GET", Binance+"/unconfigured-api-path", nil, nil, -1)
		if err == nil || !strings.Contains(err.Error(), "Unconfigured Binance request weight") {
			t.Fatalf("unknown API weight was accepted: %v", err)
		}
		if len(exchange.requests) != 0 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 0 {
			t.Fatalf("rejected API request spent budget or reached HTTP: %+v", exchange.requests)
		}
	})
	t.Run("raw fetch conservative shared bucket", func(t *testing.T) {
		client, exchange, ledger := newClient(t, ok("announcement"))
		got, err := client.Fetch(context.Background(), "GET", Binance+"/unconfigured-api-path", nil, nil, nil, -1)
		if err != nil || got == nil || string(got.Body) != "announcement" {
			t.Fatalf("raw source response: %+v %v", got, err)
		}
		if used := ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60); used != 20 {
			t.Fatalf("unlisted raw source charged %v, want conservative shared weight 20", used)
		}
		if len(exchange.requests) != 1 {
			t.Fatalf("raw source requests: %+v", exchange.requests)
		}
	})
}
