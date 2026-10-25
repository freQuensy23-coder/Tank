package budget

import (
	"encoding/json"
	"strings"
	"testing"
)

func assertDocumentedPlanTransition(t *testing.T, plan vectorPlan, got []Bucket, planErr error, body json.RawMessage) bool {
	t.Helper()
	if plan.Host == "data-api.binance.vision" && plan.Path == "/api/v3/klines" {
		if plan.Error != "" || len(plan.Buckets) != 0 || planErr == nil || planErr.Error() != "unconfigured fetch request budget: data-api.binance.vision" || len(got) != 0 {
			t.Fatalf("retained unconfigured host cannot admit without constraints: got=%+v err=%v", got, planErr)
		}
		return true
	}
	if plan.Host == "www.okx.com" && plan.Path == "/api/v5/public/open-interest" && plan.Params["instId"] == "" {
		if len(plan.Buckets) != 1 || !strings.HasSuffix(plan.Buckets[0].Key, ":ALL") || planErr == nil || planErr.Error() != "configured instrument scope requires instId" || len(got) != 0 {
			t.Fatalf("unproven retained ALL instrument scope must refuse: retained=%+v got=%+v err=%v", plan.Buckets, got, planErr)
		}
		return true
	}
	if plan.Host == "www.okx.com" && plan.Path == "/api/v5/public/instruments" && plan.Params["instType"] == "" {
		if len(plan.Buckets) != 1 || !strings.HasSuffix(plan.Buckets[0].Key, ":ALL") || planErr == nil || planErr.Error() != "configured instrument-type scope requires instType" || len(got) != 0 {
			t.Fatalf("missing retained instrument-type scope must refuse: got=%+v err=%v", got, planErr)
		}
		return true
	}
	if plan.Host == "www.okx.com" && plan.Path == "/api/v5/public/mark-price" {
		if plan.Error != "Unconfigured OKX request limit: /api/v5/public/mark-price" || planErr != nil || len(got) != 1 ||
			got[0] != (Bucket{Key: "okx:/api/v5/public/mark-price:BTC-USDT-SWAP", Capacity: 5, WindowS: 2, Weight: 1}) {
			t.Fatalf("captured configured mark-price limit must activate: got=%+v err=%v", got, planErr)
		}
		return true
	}
	if plan.Host != "api.hyperliquid.xyz" {
		return false
	}
	var operation struct{ Type string }
	if err := json.Unmarshal(body, &operation); err != nil {
		t.Fatal(err)
	}
	if operation.Type == "l2Book" {
		if plan.Error != "Unconfigured Hyperliquid request weight: l2Book" || planErr != nil || len(got) != 1 ||
			got[0] != (Bucket{Key: "hyperliquid", Capacity: 600, WindowS: 60, Weight: 2}) {
			t.Fatalf("captured configured l2Book weight must activate: got=%+v err=%v", got, planErr)
		}
		return true
	}
	if operation.Type != "candleSnapshot" {
		return false
	}
	var request hyperCandleBudgetRequest
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	if request.Req == nil || request.Req.Start == nil {
		if len(plan.Buckets) != 1 || plan.Buckets[0].Weight != 104 || planErr == nil || planErr.Error() != "candle budget requires explicit coin, interval and millisecond range" || len(got) != 0 {
			t.Fatalf("missing candle request evidence must refuse instead of guessed maximum: got=%+v err=%v", got, planErr)
		}
		return true
	}
	if request.Req.End != nil && *request.Req.End < *request.Req.Start {
		if len(plan.Buckets) != 1 || (plan.Buckets[0].Weight != 20 && plan.Buckets[0].Weight != 21) ||
			planErr == nil || planErr.Error() != "candle budget interval is inverted" || len(got) != 0 {
			t.Fatalf("inverted retained candle range must refuse: got=%+v err=%v", got, planErr)
		}
		return true
	}
	return false
}
