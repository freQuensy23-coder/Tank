package budgetpolicy

import "encoding/json"

type RatePolicy struct {
	Key      string
	Capacity float64
}

type WeightedPolicy struct {
	Key        string
	Documented float64
	Capacity   float64
}

type WindowPolicy struct {
	Key      string
	Capacity float64
	WindowS  float64
}

type EndpointPolicy struct {
	Path     string
	Capacity float64
	WindowS  float64
	Scope    string
}

type ProtectedOperationPolicy struct {
	WindowPolicy
	Paths []string
}

type PublicHostPolicy struct {
	WindowPolicy
	Host string
}

type ResponseWeightPolicy struct {
	Weight   int64
	PerItems int64
}

type HyperliquidPolicy struct {
	Weights         map[string]int64
	ResponseWeights map[string]ResponseWeightPolicy
}

type PolicyInput struct {
	Rates          []RatePolicy
	Weighted       []WeightedPolicy
	Bybit          *WindowPolicy
	OKX            []EndpointPolicy
	USDMProtection []ProtectedOperationPolicy
	Public         []PublicHostPolicy
	Hyperliquid    *HyperliquidPolicy
}

type Request struct {
	Method string
	Host   string
	Path   string
	Query  map[string]string
	Body   json.RawMessage
}
