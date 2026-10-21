// Package budget is one request budget per provider bucket and IP: the buckets
// tank.market.rate_gate and MarketService._request_once charge, a ledger shared with
// Tank core through PostgreSQL (ops.request_budget_acquire, migration 046), and the
// per-process admission in front of it (rate_gate.PriorityGate, the host start spacing
// and SharedRequestBudgets' urgent yielding).
package budget

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

// Bucket is one SharedRequestBudgets.acquire(key, capacity, window_s, weight=weight).
type Bucket struct {
	Key               string
	Capacity, WindowS float64
	Weight            float64
}

// Name is the bucket's ledger name.
func (b Bucket) Name() string { return BucketName(b.Key, b.WindowS, b.Capacity) }

// BucketName is how core names a SharedRequestBudgets bucket (key, float(window_s),
// float(capacity)) in the shared ledger: key|repr(float(window_s))|repr(float(capacity)).
func BucketName(key string, windowS, capacity float64) string {
	return key + "|" + floatRepr(windowS) + "|" + floatRepr(capacity)
}

func floatRepr(f float64) string {
	s, err := pyjson.FloatRepr(f)
	if err != nil {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return s
}

// ErrInvalidBudget is SharedRequestBudgets.acquire's ValueError.
var ErrInvalidBudget = errors.New("Invalid configured request budget")

func valid(capacity, weight float64) error {
	if capacity <= 0 || weight <= 0 || weight > capacity || math.IsNaN(capacity) || math.IsNaN(weight) {
		return ErrInvalidBudget
	}
	return nil
}

// BinanceWeight is tank.market.rate_gate.binance_request_weight: public endpoint weights
// from the official Binance contracts; unknown endpoints fail closed.
func BinanceWeight(host, path string, params map[string]string) (float64, error) {
	limit, err := intParam(params, "limit", 100)
	if err != nil {
		return 0, err
	}
	switch host {
	case "api.binance.com":
		switch path {
		case "/api/v3/exchangeInfo":
			return 20, nil
		case "/api/v3/aggTrades":
			return 4, nil
		case "/api/v3/klines":
			return 2, nil
		case "/api/v3/depth":
			switch {
			case limit <= 100:
				return 5, nil
			case limit <= 500:
				return 25, nil
			case limit <= 1000:
				return 50, nil
			}
			return 250, nil
		case "/api/v3/ticker/24hr":
			if params["symbol"] != "" {
				return 2, nil
			}
			return 80, nil
		}
	case "fapi.binance.com":
		switch path {
		case "/fapi/v1/exchangeInfo", "/fapi/v1/openInterest":
			return 1, nil
		case "/fapi/v1/aggTrades":
			return 20, nil
		case "/futures/data/openInterestHist", "/fapi/v1/fundingRate", "/fapi/v1/fundingInfo":
			return 0, nil
		case "/fapi/v1/klines":
			limit, err := intParam(params, "limit", 500)
			if err != nil {
				return 0, err
			}
			switch {
			case limit < 100:
				return 1, nil
			case limit < 500:
				return 2, nil
			case limit <= 1000:
				return 5, nil
			}
			return 10, nil
		case "/fapi/v1/depth":
			switch {
			case limit <= 50:
				return 2, nil
			case limit <= 100:
				return 5, nil
			case limit <= 500:
				return 10, nil
			}
			return 20, nil
		case "/fapi/v1/ticker/24hr":
			if params["symbol"] != "" {
				return 1, nil
			}
			return 40, nil
		case "/fapi/v1/premiumIndex":
			if params["symbol"] != "" {
				return 1, nil
			}
			return 10, nil
		}
	}
	return 0, errors.New("Unconfigured Binance request weight: " + path)
}

// intParam is int(params.get(name, def)).
func intParam(params map[string]string, name string, def int64) (int64, error) {
	value, ok := params[name]
	if !ok {
		return def, nil
	}
	n, err := pyval.Int(value)
	if err != nil {
		return 0, fmt.Errorf("invalid literal for int() with base 10: %q", value)
	}
	return n, nil
}

// okxCaps are public_request_constraints' OKX endpoint budgets and their scopes.
var okxCaps = map[string]struct {
	capacity float64
	scope    string
}{
	"/api/v5/market/books":                {20, "IP"},
	"/api/v5/market/trades":               {50, "IP"},
	"/api/v5/market/history-trades":       {10, "IP"},
	"/api/v5/market/candles":              {20, "IP"},
	"/api/v5/market/history-candles":      {10, "IP"},
	"/api/v5/public/open-interest":        {10, "instrument"},
	"/api/v5/public/funding-rate":         {5, "instrument"},
	"/api/v5/public/funding-rate-history": {5, "instrument"},
	"/api/v5/public/instruments":          {10, "instrument_type"},
	"/api/v5/market/ticker":               {10, "IP"},
	"/api/v5/market/tickers":              {10, "IP"},
}

// PublicConstraints is tank.market.rate_gate.public_request_constraints: verified public
// provider budgets whose keys include the documented scope.
func PublicConstraints(host, path string, params map[string]string, payload map[string]any) ([]Bucket, error) {
	switch host {
	case "api.bybit.com":
		return []Bucket{{"bybit", 300, 5, 1}}, nil
	case "www.okx.com", "openapi.okx.com":
		limit, ok := okxCaps[path]
		if !ok {
			return nil, errors.New("Unconfigured OKX request limit: " + path)
		}
		suffix := "IP"
		switch limit.scope {
		case "instrument":
			suffix = paramOr(params, "instId", "ALL")
		case "instrument_type":
			suffix = paramOr(params, "instType", "ALL")
		}
		return []Bucket{{"okx:" + path + ":" + suffix, limit.capacity, 2, 1}}, nil
	case "api.hyperliquid.xyz":
		kind := payload["type"]
		var weight int64
		switch kind {
		case "meta", "metaAndAssetCtxs":
			weight = 20
		case "fundingHistory":
			weight = 45 // 20 + ceil(documented 500 time-range response rows / 20)
		case "candleSnapshot":
			request := pyval.Map(payload["req"])
			rows := int64(5000)
			if request["interval"] == "1m" && pyval.Has(request, "endTime") && pyval.Has(request, "startTime") {
				end, err := pyval.Int(request["endTime"])
				if err != nil {
					return nil, err
				}
				start, err := pyval.Int(request["startTime"])
				if err != nil {
					return nil, err
				}
				rows = min(5000, max(0, floorDiv(end-start, 60000)+2))
			}
			weight = 20 + (rows+59)/60
		default:
			return nil, errors.New("Unconfigured Hyperliquid request weight: " + pyval.Str(kind))
		}
		return []Bucket{{"hyperliquid", 600, 60, float64(weight)}}, nil
	}
	return nil, nil
}

func paramOr(params map[string]string, name, def string) string {
	if v, ok := params[name]; ok {
		return v
	}
	return def
}

// floorDiv is Python's integer floor division.
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// Constraints is the admit_budget plan of MarketService._request_once for one request:
// the Kraken and Coinbase request rates, the Binance weights and their funding and
// statistics buckets, then the public endpoint constraints, in the order acquired.
func Constraints(host, path string, params map[string]string, payload map[string]any) ([]Bucket, error) {
	var out []Bucket
	switch host {
	case "api.kraken.com":
		out = append(out, Bucket{"kraken", 1, 1, 1})
	case "api.exchange.coinbase.com":
		out = append(out, Bucket{"coinbase", 5, 1, 1})
	case "api.binance.com", "fapi.binance.com":
		weight, err := BinanceWeight(host, path, params)
		if err != nil {
			return nil, err
		}
		if weight != 0 {
			if host == "api.binance.com" {
				out = append(out, Bucket{"binance.spot", 3000, 60, weight})
			} else {
				out = append(out, Bucket{"binance.usdm", 1200, 60, weight})
			}
		}
		if host == "fapi.binance.com" && (path == "/fapi/v1/fundingRate" || path == "/fapi/v1/fundingInfo") {
			out = append(out, Bucket{"binance.usdm.funding", 250, 300, 1})
		}
		if host == "fapi.binance.com" && path == "/futures/data/openInterestHist" {
			out = append(out, Bucket{"binance.usdm.statistics", 500, 300, 1})
		}
	}
	public, err := PublicConstraints(host, path, params, payload)
	if err != nil {
		return nil, err
	}
	return append(out, public...), nil
}
