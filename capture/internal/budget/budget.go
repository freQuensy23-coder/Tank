package budget

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

type Bucket struct {
	Key               string
	Capacity, WindowS float64
	Weight            float64
}

func (b Bucket) Name() string { return BucketName(b.Key, b.WindowS, b.Capacity) }

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

var ErrInvalidBudget = errors.New("Invalid configured request budget")

func valid(capacity, weight float64) error {
	if capacity <= 0 || weight <= 0 || weight > capacity || math.IsNaN(capacity) || math.IsNaN(weight) || math.IsInf(capacity, 0) || math.IsInf(weight, 0) {
		return ErrInvalidBudget
	}
	return nil
}

func (b Bucket) Validate() error {
	if b.Key == "" || b.WindowS <= 0 || math.IsNaN(b.WindowS) || math.IsInf(b.WindowS, 0) || b.WindowS >= float64(math.MaxInt64)/1e9 {
		return ErrInvalidBudget
	}
	return valid(b.Capacity, b.Weight)
}

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
			weight = 45
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

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

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

func FetchConstraints(host, path string, params map[string]string, payload map[string]any) ([]Bucket, error) {
	switch host {
	case "www.binance.com":
		return []Bucket{{"binance.www", 60, 60, 1}}, nil
	case "data.binance.vision":
		return []Bucket{{"binance.vision", 10, 1, 1}}, nil
	}
	buckets, err := Constraints(host, path, params, payload)
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, errors.New("Unconfigured fetch request budget: " + host)
	}
	return buckets, nil
}
