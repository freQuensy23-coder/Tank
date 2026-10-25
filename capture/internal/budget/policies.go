package budget

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
)

type Policies struct {
	rates       map[string]budgetpolicy.RatePolicy
	weighted    map[string]budgetpolicy.WeightedPolicy
	bybit       *budgetpolicy.WindowPolicy
	okx         map[string]budgetpolicy.EndpointPolicy
	protection  map[string]budgetpolicy.ProtectedOperationPolicy
	public      map[string]budgetpolicy.PublicHostPolicy
	hyperliquid *budgetpolicy.HyperliquidPolicy
}

func NewPolicies(input budgetpolicy.PolicyInput) (Policies, error) {
	result := Policies{
		rates: make(map[string]budgetpolicy.RatePolicy), weighted: make(map[string]budgetpolicy.WeightedPolicy),
		okx: make(map[string]budgetpolicy.EndpointPolicy), protection: make(map[string]budgetpolicy.ProtectedOperationPolicy),
		public: make(map[string]budgetpolicy.PublicHostPolicy),
	}
	for _, rule := range input.Rates {
		if rule.Key != "coinbase" && rule.Key != "kraken" {
			return Policies{}, fmt.Errorf("unsupported capture request-rate provider")
		}
		if _, exists := result.rates[rule.Key]; exists {
			return Policies{}, fmt.Errorf("duplicate capture request-rate provider")
		}
		if err := (Bucket{Key: rule.Key, Capacity: rule.Capacity, WindowS: 1, Weight: 1}).Validate(); err != nil {
			return Policies{}, err
		}
		result.rates[rule.Key] = rule
	}
	for _, rule := range input.Weighted {
		if rule.Key != "binance.spot" && rule.Key != "binance.usdm" && rule.Key != "hyperliquid" {
			return Policies{}, fmt.Errorf("unsupported capture weighted provider")
		}
		if _, exists := result.weighted[rule.Key]; exists {
			return Policies{}, fmt.Errorf("duplicate capture weighted provider")
		}
		if err := (Bucket{Key: rule.Key, Capacity: rule.Capacity, WindowS: 60, Weight: 1}).Validate(); err != nil {
			return Policies{}, err
		}
		if math.IsNaN(rule.Documented) || math.IsInf(rule.Documented, 0) || rule.Documented < 2*rule.Capacity {
			return Policies{}, ErrInvalidBudget
		}
		result.weighted[rule.Key] = rule
	}
	if input.Bybit != nil {
		rule := *input.Bybit
		if rule.Key != "bybit" {
			return Policies{}, ErrInvalidBudget
		}
		if err := (Bucket{Key: rule.Key, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}).Validate(); err != nil {
			return Policies{}, err
		}
		result.bybit = &rule
	}
	for _, rule := range input.OKX {
		if _, exists := result.okx[rule.Path]; exists {
			return Policies{}, fmt.Errorf("duplicate capture endpoint policy")
		}
		if !strings.HasPrefix(rule.Path, "/api/v5/") || strings.ContainsAny(rule.Path, "?#") {
			return Policies{}, fmt.Errorf("invalid capture endpoint policy path")
		}
		switch rule.Scope {
		case "IP", "IP_INSTRUMENT", "IP_INSTRUMENT_TYPE":
		default:
			return Policies{}, fmt.Errorf("unsupported capture endpoint policy scope")
		}
		if err := (Bucket{Key: "okx:" + rule.Path, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}).Validate(); err != nil {
			return Policies{}, err
		}
		result.okx[rule.Path] = rule
	}
	for _, rule := range input.USDMProtection {
		if _, exists := result.protection[rule.Key]; exists {
			return Policies{}, fmt.Errorf("duplicate capture protection policy")
		}
		if rule.Key != "binance.usdm.funding" && rule.Key != "binance.usdm.statistics" {
			return Policies{}, fmt.Errorf("unsupported capture protection policy")
		}
		if err := (Bucket{Key: rule.Key, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}).Validate(); err != nil {
			return Policies{}, err
		}
		expected := []string{"/fapi/v1/fundingRate", "/fapi/v1/fundingInfo"}
		if rule.Key == "binance.usdm.statistics" {
			expected = []string{"/futures/data/openInterestHist"}
		}
		paths := slices.Clone(rule.Paths)
		slices.Sort(paths)
		slices.Sort(expected)
		if !slices.Equal(paths, expected) {
			return Policies{}, fmt.Errorf("capture protection operation group differs")
		}
		rule.Paths = paths
		result.protection[rule.Key] = rule
	}
	if _, enabled := result.weighted["binance.usdm"]; enabled && len(result.protection) != 2 {
		return Policies{}, fmt.Errorf("USD-M requires both protection groups")
	}
	for _, rule := range input.Public {
		if _, exists := result.public[rule.Host]; exists {
			return Policies{}, fmt.Errorf("duplicate public-host policy")
		}
		if (rule.Key != "binance.www" || rule.Host != "www.binance.com") && (rule.Key != "binance.vision" || rule.Host != "data.binance.vision") {
			return Policies{}, fmt.Errorf("unsupported public-host policy")
		}
		if err := (Bucket{Key: rule.Key, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}).Validate(); err != nil {
			return Policies{}, err
		}
		result.public[rule.Host] = rule
	}
	_, hyperEnabled := result.weighted["hyperliquid"]
	if hyperEnabled != (input.Hyperliquid != nil) {
		return Policies{}, fmt.Errorf("hyperliquid requires explicit weighted operation policy")
	}
	if input.Hyperliquid != nil {
		h := &budgetpolicy.HyperliquidPolicy{Weights: make(map[string]int64), ResponseWeights: make(map[string]budgetpolicy.ResponseWeightPolicy)}
		if len(input.Hyperliquid.Weights) == 0 || len(input.Hyperliquid.ResponseWeights) == 0 {
			return Policies{}, ErrInvalidBudget
		}
		for operation, weight := range input.Hyperliquid.Weights {
			if strings.TrimSpace(operation) == "" || weight <= 0 || weight >= 1<<53 {
				return Policies{}, ErrInvalidBudget
			}
			h.Weights[operation] = weight
		}
		for operation, rule := range input.Hyperliquid.ResponseWeights {
			if strings.TrimSpace(operation) == "" || rule.Weight <= 0 || rule.Weight >= 1<<53 || rule.PerItems <= 0 {
				return Policies{}, ErrInvalidBudget
			}
			h.ResponseWeights[operation] = rule
		}
		result.hyperliquid = h
	}
	return result, nil
}

func (p Policies) QueryFields(host, path string) []string {
	if host == "www.okx.com" || host == "openapi.okx.com" {
		rule, exists := p.okx[path]
		if !exists {
			return nil
		}
		switch rule.Scope {
		case "IP_INSTRUMENT":
			return []string{"instId"}
		case "IP_INSTRUMENT_TYPE":
			return []string{"instType"}
		default:
			return nil
		}
	}
	return RequestQueryFields(host, path)
}

func (p Policies) DocumentedWeight(host string) (float64, error) {
	key := ""
	switch host {
	case "api.binance.com":
		key = "binance.spot"
	case "fapi.binance.com":
		key = "binance.usdm"
	default:
		return 0, fmt.Errorf("host has no configured documented weight")
	}
	rule, exists := p.weighted[key]
	if !exists {
		return 0, fmt.Errorf("missing configured documented weight")
	}
	return rule.Documented, nil
}

func (p Policies) plan(request budgetpolicy.Request) ([]Bucket, error) {
	method, host, path, params, body := request.Method, request.Host, request.Path, request.Query, request.Body
	if method != "GET" && method != "POST" {
		return nil, fmt.Errorf("unsupported capture request method")
	}
	if (host == "api.binance.com" || host == "fapi.binance.com" || host == "www.okx.com" || host == "openapi.okx.com") && method != "GET" {
		return nil, fmt.Errorf("configured endpoint weight requires GET")
	}
	if host == "api.hyperliquid.xyz" && (method != "POST" || path != "/info") {
		return nil, fmt.Errorf("configured hyperliquid operation requires POST /info")
	}
	switch host {
	case "www.binance.com", "data.binance.vision":
		rule, exists := p.public[host]
		if !exists {
			return nil, fmt.Errorf("missing configured public-host policy")
		}
		return []Bucket{{Key: rule.Key, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}}, nil
	case "api.exchange.coinbase.com", "api.kraken.com":
		key := "coinbase"
		if host == "api.kraken.com" {
			key = "kraken"
		}
		rule, exists := p.rates[key]
		if !exists {
			return nil, fmt.Errorf("missing configured request-rate policy: %s", key)
		}
		return []Bucket{{Key: rule.Key, Capacity: rule.Capacity, WindowS: 1, Weight: 1}}, nil
	case "api.binance.com", "fapi.binance.com":
		key := "binance.spot"
		if host == "fapi.binance.com" {
			key = "binance.usdm"
		}
		rule, exists := p.weighted[key]
		if !exists {
			return nil, fmt.Errorf("missing configured weighted policy: %s", key)
		}
		if err := validateBinanceCostQuery(host, path, params); err != nil {
			return nil, err
		}
		weight, err := BinanceWeight(host, path, params)
		if err != nil {
			return nil, err
		}
		var out []Bucket
		if weight != 0 {
			out = append(out, Bucket{Key: rule.Key, Capacity: rule.Capacity, WindowS: 60, Weight: weight})
		}
		if host == "fapi.binance.com" {
			for _, key := range []string{"binance.usdm.funding", "binance.usdm.statistics"} {
				protection, exists := p.protection[key]
				if !exists {
					return nil, fmt.Errorf("missing configured USD-M protection")
				}
				if slices.Contains(protection.Paths, path) {
					out = append(out, Bucket{Key: protection.Key, Capacity: protection.Capacity, WindowS: protection.WindowS, Weight: 1})
				}
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("request has no configured weighted constraints")
		}
		return out, nil
	case "api.bybit.com":
		if p.bybit == nil {
			return nil, fmt.Errorf("missing configured Bybit policy")
		}
		rule := p.bybit
		return []Bucket{{Key: rule.Key, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}}, nil
	case "www.okx.com", "openapi.okx.com":
		rule, exists := p.okx[path]
		if !exists {
			return nil, errors.New("Unconfigured OKX request limit: " + path)
		}
		scope := "IP"
		switch rule.Scope {
		case "IP_INSTRUMENT":
			scope = params["instId"]
			if scope == "" {
				return nil, fmt.Errorf("configured instrument scope requires instId")
			}
		case "IP_INSTRUMENT_TYPE":
			scope = params["instType"]
			if scope == "" {
				return nil, fmt.Errorf("configured instrument-type scope requires instType")
			}
		}
		return []Bucket{{Key: "okx:" + path + ":" + scope, Capacity: rule.Capacity, WindowS: rule.WindowS, Weight: 1}}, nil
	case "api.hyperliquid.xyz":
		return p.hyperPlan(body)
	default:
		return nil, fmt.Errorf("unconfigured fetch request budget: %s", host)
	}
}

func (p Policies) Plan(request budgetpolicy.Request) ([]Bucket, error) {
	buckets, err := p.plan(request)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if err := bucket.Validate(); err != nil {
			return nil, err
		}
	}
	return buckets, nil
}

func validateBinanceCostQuery(host, path string, params map[string]string) error {
	switch {
	case host == "api.binance.com" && path == "/api/v3/depth":
		limit, err := intParam(params, "limit", 100)
		if err != nil {
			return err
		}
		if limit <= 0 {
			return fmt.Errorf("spot depth limit must be positive")
		}
	case host == "fapi.binance.com" && path == "/fapi/v1/depth":
		limit, err := intParam(params, "limit", 500)
		if err != nil {
			return err
		}
		switch limit {
		case 5, 10, 20, 50, 100, 500, 1000:
		default:
			return fmt.Errorf("unsupported USD-M depth limit")
		}
	case host == "fapi.binance.com" && path == "/fapi/v1/klines":
		limit, err := intParam(params, "limit", 500)
		if err != nil {
			return err
		}
		if limit <= 0 || limit > 1500 {
			return fmt.Errorf("unsupported USD-M kline limit")
		}
	}
	return nil
}
