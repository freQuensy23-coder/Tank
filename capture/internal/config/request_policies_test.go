package config

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

var errObservedQuotaWait = errors.New("observed configured quota wait")

type quotaObserver struct {
	*budget.MemoryLedger
	wait time.Duration
}

func (l *quotaObserver) AcquireGroup(ctx context.Context, buckets []budget.Bucket, owner, host string) (time.Duration, error) {
	wait, err := l.MemoryLedger.AcquireGroup(ctx, buckets, owner, host)
	if err != nil {
		return 0, err
	}
	if wait > 0 {
		l.wait = wait
		return 0, errObservedQuotaWait
	}
	return 0, nil
}

type policyTransport struct{ requests int }

func (t *policyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.requests++
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}

func replaceProviderPolicy(t *testing.T, data []byte, provider, before, after string) []byte {
	t.Helper()
	prefix := "- provider: " + provider + "\n"
	source := string(data)
	start := strings.Index(source, prefix)
	if start < 0 {
		t.Fatal("policy provider fixture not found")
	}
	end := len(source)
	if next := strings.Index(source[start+len(prefix):], "\n- provider: "); next >= 0 {
		end = start + len(prefix) + next
	}
	block := source[start:end]
	if strings.Count(block, before) != 1 {
		t.Fatalf("policy mutation requires one exact field: %s", provider)
	}
	return []byte(source[:start] + strings.Replace(block, before, after, 1) + source[end:])
}

func TestChangedConfiguredCapsControlActualFetchAdmission(t *testing.T) {
	source, err := os.ReadFile(filepath.Join(repoConfig(), "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		provider string
		before   string
		after    string
		request  budgetpolicy.Request
		window   time.Duration
	}{
		{"coinbase", "coinbase", "tank_max_requests_per_s: 5", "tank_max_requests_per_s: 2", budgetpolicy.Request{Method: "GET", Host: "api.exchange.coinbase.com", Path: "/products"}, time.Second},
		{"kraken", "kraken", "tank_max_requests_per_s: 1", "tank_max_requests_per_s: 2", budgetpolicy.Request{Method: "GET", Host: "api.kraken.com", Path: "/0/public/AssetPairs"}, time.Second},
		{"spot", "binance.spot", "tank_max_weight_per_min: 3000", "tank_max_weight_per_min: 40", budgetpolicy.Request{Method: "GET", Host: "api.binance.com", Path: "/api/v3/exchangeInfo"}, time.Minute},
		{"usdm", "binance.usdm", "tank_max_weight_per_min: 1200", "tank_max_weight_per_min: 2", budgetpolicy.Request{Method: "GET", Host: "fapi.binance.com", Path: "/fapi/v1/exchangeInfo"}, time.Minute},
		{"funding protection", "binance.usdm", "tank_request_budget: 250", "tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "fapi.binance.com", Path: "/fapi/v1/fundingInfo"}, 5 * time.Minute},
		{"statistics protection", "binance.usdm", "tank_request_budget: 500", "tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "fapi.binance.com", Path: "/futures/data/openInterestHist"}, 5 * time.Minute},
		{"bybit", "bybit", "tank_request_budget: 300", "tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "api.bybit.com", Path: "/v5/market/instruments-info"}, 5 * time.Second},
		{"funding configured window", "binance.usdm", "period_s: 300\n    tank_request_budget: 250", "period_s: 120\n    tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "fapi.binance.com", Path: "/fapi/v1/fundingInfo"}, 2 * time.Minute},
		{"bybit configured window", "bybit", "period_s: 5\n  tank_request_budget: 300", "period_s: 7\n  tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "api.bybit.com", Path: "/v5/market/instruments-info"}, 7 * time.Second},
		{"okx configured endpoint", "okx", "path: /api/v5/market/books\n    documented_requests: 40\n    period_s: 2\n    tank_request_budget: 20", "path: /api/v5/market/books\n    documented_requests: 40\n    period_s: 2\n    tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "www.okx.com", Path: "/api/v5/market/books", Query: map[string]string{"instId": "BTC-USDT"}}, 2 * time.Second},
		{"hyperliquid", "hyperliquid", "tank_max_weight_per_min: 600", "tank_max_weight_per_min: 40", budgetpolicy.Request{Method: "POST", Host: "api.hyperliquid.xyz", Path: "/info", Body: []byte("{\"type\":\"meta\"}")}, time.Minute},
		{"public page", "binance.www", "tank_request_budget: 60", "tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "www.binance.com", Path: "/announcement"}, time.Minute},
		{"public archive", "binance.vision", "tank_request_budget: 10", "tank_request_budget: 2", budgetpolicy.Request{Method: "GET", Host: "data.binance.vision", Path: "/public/archive"}, time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "api_limits.yaml")
			if err := os.WriteFile(path, replaceProviderPolicy(t, source, test.provider, test.before, test.after), 0o600); err != nil {
				t.Fatal(err)
			}
			policies, err := LoadRequestPolicies(path)
			if err != nil {
				t.Fatal(err)
			}
			clock := func() time.Time { return time.Unix(100, 0) }
			ledger := &quotaObserver{MemoryLedger: budget.NewMemoryLedger(clock)}
			gate := budget.NewGate(ledger, "configured-policy-test", policies)
			gate.Interval = func(string) time.Duration { return 0 }
			client := rest.NewClient("TankV0/synthetic-policy-test", gate, clock)
			transport := &policyTransport{}
			client.HTTP = &http.Client{Transport: transport}
			for range 2 {
				if _, err := client.Fetch(context.Background(), test.request.Method, "https://"+test.request.Host+test.request.Path, test.request.Query, nil, test.request.Body, 0); err != nil {
					t.Fatal(err)
				}
			}
			_, err = client.Fetch(context.Background(), test.request.Method, "https://"+test.request.Host+test.request.Path, test.request.Query, nil, test.request.Body, 0)
			if !errors.Is(err, errObservedQuotaWait) || ledger.wait != test.window || transport.requests != 2 {
				t.Fatalf("declared cap failed to govern real HTTP admission: requests=%d wait=%v err=%v", transport.requests, ledger.wait, err)
			}
		})
	}
}

func TestConfiguredOKXScopePreservesAliasesAndPartitionBoundaries(t *testing.T) {
	source, err := os.ReadFile(filepath.Join(repoConfig(), "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, path, field, first, second, before, after string
		partitioned                                     bool
	}{
		{"IP", "/api/v5/market/books", "instId", "BTC-USDT", "ETH-USDT", "path: /api/v5/market/books\n    documented_requests: 40\n    period_s: 2\n    tank_request_budget: 20", "path: /api/v5/market/books\n    documented_requests: 40\n    period_s: 2\n    tank_request_budget: 2", false},
		{"instrument", "/api/v5/public/funding-rate", "instId", "BTC-USDT-SWAP", "ETH-USDT-SWAP", "path: /api/v5/public/funding-rate\n    documented_requests: 10\n    period_s: 2\n    tank_request_budget: 5", "path: /api/v5/public/funding-rate\n    documented_requests: 10\n    period_s: 2\n    tank_request_budget: 2", true},
		{"instrument type", "/api/v5/public/instruments", "instType", "SPOT", "SWAP", "path: /api/v5/public/instruments\n    documented_requests: 20\n    period_s: 2\n    tank_request_budget: 10", "path: /api/v5/public/instruments\n    documented_requests: 20\n    period_s: 2\n    tank_request_budget: 2", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "api_limits.yaml")
			if err := os.WriteFile(path, replaceProviderPolicy(t, source, "okx", test.before, test.after), 0o600); err != nil {
				t.Fatal(err)
			}
			policies, err := LoadRequestPolicies(path)
			if err != nil {
				t.Fatal(err)
			}
			clock := func() time.Time { return time.Unix(100, 0) }
			ledger := &quotaObserver{MemoryLedger: budget.NewMemoryLedger(clock)}
			gate := budget.NewGate(ledger, "scope-policy-test", policies)
			gate.Interval = func(string) time.Duration { return 0 }
			client := rest.NewClient("TankV0/synthetic-scope-test", gate, clock)
			transport := &policyTransport{}
			client.HTTP = &http.Client{Transport: transport}
			fetch := func(host, value string) error {
				_, err := client.Fetch(context.Background(), "GET", "https://"+host+test.path, map[string]string{test.field: value}, nil, nil, 0)
				return err
			}
			if err := fetch("www.okx.com", test.first); err != nil {
				t.Fatal(err)
			}
			if err := fetch("openapi.okx.com", test.first); err != nil {
				t.Fatal(err)
			}
			if err := fetch("www.okx.com", test.first); !errors.Is(err, errObservedQuotaWait) {
				t.Fatalf("host aliases split shared quota:%v", err)
			}
			err = fetch("openapi.okx.com", test.second)
			if test.partitioned {
				if err != nil || transport.requests != 3 {
					t.Fatalf("declared scope was not independent:requests=%d err=%v", transport.requests, err)
				}
			} else if !errors.Is(err, errObservedQuotaWait) || transport.requests != 2 {
				t.Fatalf("IP scope split by irrelevant instrument:requests=%d err=%v", transport.requests, err)
			}
		})
	}
}
