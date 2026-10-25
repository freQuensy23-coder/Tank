package rest

import (
	"context"
	"strings"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

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
			name := budget.BucketName(tc.bucket, 60)
			if used := ledger.Used(name, 60); used != tc.weight {
				t.Fatalf("source discovery charged %v in %s, want %v", used, name, tc.weight)
			}
			if used := ledger.Used(budget.BucketName(tc.otherBucket, 60), 60); used != 0 {
				t.Fatalf("source discovery spent the other venue's budget: %v", used)
			}
			if len(exchange.requests) != 1 || exchange.requests[0].Method != "GET" || exchange.requests[0].URL != tc.url {
				t.Fatalf("source discovery requests: %+v", exchange.requests)
			}
		})
	}
}

func TestSourceDiscoveryUnknownWeightsRemainBounded(t *testing.T) {
	t.Run("strict API refusal", func(t *testing.T) {
		client, exchange, ledger := newClient(t)
		_, err := client.Do(context.Background(), "GET", Binance+"/unconfigured-api-path", nil, nil, -1)
		if err == nil || !strings.Contains(err.Error(), "Unconfigured Binance request weight") {
			t.Fatalf("unknown API weight was accepted: %v", err)
		}
		if len(exchange.requests) != 0 || ledger.Used(budget.BucketName("binance.spot", 60), 60) != 0 {
			t.Fatalf("rejected API request spent budget or reached HTTP: %+v", exchange.requests)
		}
	})
	t.Run("raw fetch refuses unconfigured API weight", func(t *testing.T) {
		client, exchange, ledger := newClient(t, ok("announcement"))
		got, err := client.Fetch(context.Background(), "GET", Binance+"/unconfigured-api-path", nil, nil, nil, -1)
		if err == nil || !strings.Contains(err.Error(), "Unconfigured Binance request weight") || got != nil {
			t.Fatalf("unknown raw source weight was accepted: %+v %v", got, err)
		}
		if used := ledger.Used(budget.BucketName("binance.spot", 60), 60); used != 0 {
			t.Fatalf("rejected raw source consumed budget: %v", used)
		}
		if len(exchange.requests) != 0 {
			t.Fatalf("rejected raw source reached HTTP: %+v", exchange.requests)
		}
	})
}

func TestFetchPlannerErrorsCannotBecomeGuessedAdmission(t *testing.T) {
	for _, rawURL := range []string{
		Binance + "/api/v3/depth?limit=invalid",
		"https://www.okx.com/unconfigured-endpoint",
		"https://api.hyperliquid.xyz/info",
		"https://unconfigured.invalid/announcement",
		"https://data-api.binance.vision/api/v3/klines",
	} {
		t.Run(rawURL, func(t *testing.T) {
			client, exchange, _ := newClient(t, ok("{}"))
			got, err := client.Fetch(context.Background(), "GET", rawURL, nil, nil, nil, 0)
			if err == nil || got != nil {
				t.Fatalf("planner failure became a request: %+v %v", got, err)
			}
			if len(exchange.requests) != 0 {
				t.Fatalf("planner failure reached HTTP: %+v", exchange.requests)
			}
		})
	}
}
