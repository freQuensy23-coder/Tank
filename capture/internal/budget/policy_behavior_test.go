package budget

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/policyfixture"
)

func TestConfiguredDocumentedWeightControlsSharedProviderBlock(t *testing.T) {
	input, err := policyfixture.Load("testdata/policy-input.json")
	if err != nil {
		t.Fatal(err)
	}
	for index := range input.Weighted {
		if input.Weighted[index].Key == "binance.spot" {
			input.Weighted[index].Documented = 8000
		}
	}
	policies, err := NewPolicies(input)
	if err != nil {
		t.Fatal(err)
	}
	ledger := NewMemoryLedger(time.Now)
	gate := NewGate(ledger, "synthetic-header-policy", policies)
	for _, test := range []struct {
		used    string
		blocked bool
	}{{"7000", false}, {"8000", true}} {
		header := make(http.Header)
		header.Set("X-MBX-USED-WEIGHT-1M", test.used)
		if retry, err := gate.Observe(context.Background(), "api.binance.com", http.StatusOK, header); retry || err != nil {
			t.Fatalf("header observation: retry=%v err=%v", retry, err)
		}
		if _, blocked := ledger.Blocked("api.binance.com"); blocked != test.blocked {
			t.Fatalf("configured threshold ignored at%s: blocked=%v", test.used, blocked)
		}
	}
}

func TestCompiledPolicyCannotBeRelabelledByMutableInput(t *testing.T) {
	input, err := policyfixture.Load("testdata/policy-input.json")
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicies(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Hyperliquid.Weights["meta"] = 1
	input.USDMProtection[0].Paths[0] = "/mutated-after-compilation"
	input.Bybit.Capacity = 1
	ledger := NewMemoryLedger(time.Now)
	gate := NewGate(ledger, "immutable-compiled-policy", policies)
	for _, request := range []struct{ method, host, path, body string }{
		{"POST", "api.hyperliquid.xyz", "/info", "{\"type\":\"meta\"}"},
		{"GET", "fapi.binance.com", "/fapi/v1/fundingInfo", ""},
		{"GET", "api.bybit.com", "/v5/market/instruments-info", ""},
		{"GET", "api.bybit.com", "/v5/market/instruments-info", ""},
	} {
		buckets, err := gate.PlanFetch(request.method, request.host, request.path, nil, []byte(request.body))
		if err != nil {
			t.Fatal(err)
		}
		if err := gate.AdmitBuckets(context.Background(), request.host, buckets, 0); err != nil {
			t.Fatal(err)
		}
	}
	if used := ledger.Used(BucketName("hyperliquid", 60, 600), 60); used != 20 {
		t.Fatalf("mutable operation index changed actual debit:%v", used)
	}
	if used := ledger.Used(BucketName("binance.usdm.funding", 300, 250), 300); used != 1 {
		t.Fatalf("mutable operation group changed actual debit:%v", used)
	}
	if used := ledger.Used(BucketName("bybit", 5, 300), 5); used != 2 {
		t.Fatalf("mutable provider record changed actual debit:%v", used)
	}
}
