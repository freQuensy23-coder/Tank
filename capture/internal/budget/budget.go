package budget

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

type Bucket struct {
	Key               string
	Capacity, WindowS float64
	Weight            float64
}

func (b Bucket) Name() string { return BucketName(b.Key, b.WindowS) }

func BucketName(key string, windowS float64) string {
	return key + "|" + floatRepr(windowS)
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
			limit, err := intParam(params, "limit", 100)
			if err != nil {
				return 0, err
			}
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
			limit, err := intParam(params, "limit", 500)
			if err != nil {
				return 0, err
			}
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
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid literal for int() with base 10: %q", value)
	}
	return n, nil
}

func RequestQueryFields(host, path string) []string {
	switch host {
	case "api.binance.com":
		switch path {
		case "/api/v3/depth":
			return []string{"limit"}
		case "/api/v3/ticker/24hr":
			return []string{"symbol"}
		}
	case "fapi.binance.com":
		switch path {
		case "/fapi/v1/depth", "/fapi/v1/klines":
			return []string{"limit"}
		case "/fapi/v1/ticker/24hr", "/fapi/v1/premiumIndex":
			return []string{"symbol"}
		}

	}
	return nil
}
