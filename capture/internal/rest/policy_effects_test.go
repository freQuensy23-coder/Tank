package rest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

func TestDocumentedUSDMDefaultDepthConsumesTenWeight(t *testing.T) {
	client, exchange, ledger := newClient(t, ok("{}"))
	if _, err := client.Fetch(context.Background(), "GET", BinanceUSDM+"/fapi/v1/depth?symbol=BTCUSDT", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if used := ledger.Used(budget.BucketName("binance.usdm", 60), 60); used != 10 {
		t.Fatalf("documented default500 undercharged: %v", used)
	}
	if len(exchange.requests) != 1 {
		t.Fatal("default-depth request did not reach transport")
	}
}

func TestConfiguredPreviouslyMissingOperationsHaveActualAdmissionEffects(t *testing.T) {
	for _, test := range []struct {
		name, method, rawURL, body, bucket string
		window, capacity, weight           float64
	}{
		{"configured mark price", "GET", "https://www.okx.com/api/v5/public/mark-price?instId=BTC-USDT-SWAP", "", "okx:/api/v5/public/mark-price:BTC-USDT-SWAP", 2, 5, 1},
		{"configured L2 snapshot", "POST", "https://api.hyperliquid.xyz/info", "{\"type\":\"l2Book\",\"coin\":\"BTC\"}", "hyperliquid", 60, 600, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, exchange, ledger := newClient(t, ok("{}"))
			if _, err := client.Fetch(context.Background(), test.method, test.rawURL, nil, nil, json.RawMessage(test.body), 0); err != nil {
				t.Fatal(err)
			}
			if used := ledger.Used(budget.BucketName(test.bucket, test.window), test.window); used != test.weight {
				t.Fatalf("configured operation debit=%v", used)
			}
			if len(exchange.requests) != 1 {
				t.Fatal("configured operation did not reach transport")
			}
		})
	}
}

func TestInvalidScopeRangeAndMethodCannotSpendOrSend(t *testing.T) {
	for _, test := range []struct{ name, method, rawURL, body string }{
		{"instrument scope absent", "GET", "https://www.okx.com/api/v5/public/open-interest?instType=SWAP", ""},
		{"instrument type absent", "GET", "https://www.okx.com/api/v5/public/instruments", ""},
		{"scope ambiguous", "GET", "https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP&instId=ETH-USDT-SWAP", ""},
		{"candle request absent", "POST", "https://api.hyperliquid.xyz/info", "{\"type\":\"candleSnapshot\"}"},
		{"candle start absent", "POST", "https://api.hyperliquid.xyz/info", "{\"type\":\"candleSnapshot\",\"req\":{\"coin\":\"BTC\",\"interval\":\"1m\"}}"},
		{"candle range inverted one millisecond", "POST", "https://api.hyperliquid.xyz/info", "{\"type\":\"candleSnapshot\",\"req\":{\"coin\":\"BTC\",\"interval\":\"1m\",\"startTime\":1000,\"endTime\":999}}"},
		{"candle range inverted minutes", "POST", "https://api.hyperliquid.xyz/info", "{\"type\":\"candleSnapshot\",\"req\":{\"coin\":\"BTC\",\"interval\":\"1m\",\"startTime\":180000,\"endTime\":0}}"},
		{"non-decimal limit text", "GET", BinanceUSDM + "/fapi/v1/depth?limit=1_000", ""},
		{"fractional limit text", "GET", BinanceUSDM + "/fapi/v1/depth?limit=100.0", ""},
		{"fractional candle timestamp", "POST", "https://api.hyperliquid.xyz/info", "{\"type\":\"candleSnapshot\",\"req\":{\"coin\":\"BTC\",\"interval\":\"1m\",\"startTime\":1.5,\"endTime\":1000}}"},
		{"unsupported USD-M depth limit", "GET", BinanceUSDM + "/fapi/v1/depth?limit=51", ""},
		{"weighted endpoint wrong method", "POST", Binance + "/api/v3/depth", ""},
		{"hyperliquid wrong method", "GET", "https://api.hyperliquid.xyz/info", "{\"type\":\"meta\"}"},
		{"hyperliquid wrong path", "POST", "https://api.hyperliquid.xyz/not-info", "{\"type\":\"meta\"}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, exchange, ledger := newClient(t, ok("{}"))
			if result, err := client.Fetch(context.Background(), test.method, test.rawURL, nil, nil, json.RawMessage(test.body), 0); err == nil || result != nil {
				t.Fatalf("unproven request admitted: result=%+v err=%v", result, err)
			}
			if len(exchange.requests) != 0 {
				t.Fatal("unproven request reached HTTP")
			}
			for _, name := range []string{budget.BucketName("hyperliquid", 60), budget.BucketName("binance.spot", 60), budget.BucketName("binance.usdm", 60)} {
				if ledger.Used(name, 60) != 0 {
					t.Fatal("rejected request consumed quota")
				}
			}
		})
	}
}

func TestProviderGETAndCaseInsensitiveHostUseActualEffectiveQuery(t *testing.T) {
	client, exchange, ledger := newClient(t, ok("{}"))
	if _, err := client.Do(context.Background(), "GET", "https://API.BINANCE.COM/api/v3/depth?limit=5000", nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if used := ledger.Used(budget.BucketName("binance.spot", 60), 60); used != 250 {
		t.Fatalf("provider GET URL query undercharged:%v", used)
	}
	if len(exchange.requests) != 1 {
		t.Fatal("valid case-insensitive DNS host failed to reach HTTP")
	}
}

func TestConfiguredUnboundedResponseWeightRefusesBeforeQuotaAndHTTP(t *testing.T) {
	client, exchange, ledger := newClient(t, ok("[]"))
	result, err := client.Fetch(context.Background(), "POST", "https://api.hyperliquid.xyz/info", nil, nil, []byte("{\"type\":\"recentTrades\",\"coin\":\"BTC\"}"), 0)
	if !errors.Is(err, budget.ErrUnboundedResponseWeight) || result != nil {
		t.Fatalf("unbounded configured operation misclassified:result=%+v err=%v", result, err)
	}
	if len(exchange.requests) != 0 || ledger.Used(budget.BucketName("hyperliquid", 60), 60) != 0 {
		t.Fatal("unbounded operation spent quota or reached HTTP")
	}
}
