package config

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type budgetEvidence struct {
	Basis           string   `yaml:"basis"`
	SourceRef       string   `yaml:"source_ref"`
	SourceContent   string   `yaml:"source_content_sha256"`
	VerifiedAt      string   `yaml:"verified_at"`
	InferenceLimits []string `yaml:"inference_limits"`
}

func (e budgetEvidence) validate(requireCapture, requireBasis bool) error {
	if strings.TrimSpace(e.Basis) == "" && strings.TrimSpace(e.SourceRef) == "" {
		return fmt.Errorf("request budget evidence requires basis or source_ref")
	}
	if requireBasis && strings.TrimSpace(e.Basis) == "" {
		return fmt.Errorf("request budget requires explicit policy basis")
	}
	if e.VerifiedAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, e.VerifiedAt); err != nil {
			return fmt.Errorf("invalid request budget evidence clock: %w", err)
		}
	}
	if requireCapture && (e.SourceRef == "" || e.VerifiedAt == "") {
		return fmt.Errorf("request budget requires captured documentation reference and clock")
	}
	return nil
}

type rateBudgetWire struct {
	Provider       string   `yaml:"provider"`
	Documented     *float64 `yaml:"documented_requests_per_s"`
	Assigned       float64  `yaml:"tank_max_requests_per_s"`
	budgetEvidence `yaml:",inline"`
}

type weightBudgetWire struct {
	Provider       string  `yaml:"provider"`
	Documented     float64 `yaml:"documented_weight_per_min"`
	Assigned       float64 `yaml:"tank_max_weight_per_min"`
	budgetEvidence `yaml:",inline"`
}

type spotBudgetWire struct {
	weightBudgetWire `yaml:",inline"`
	ObservedHeader   *string `yaml:"observed_header"`
}

type clientProtectionWire struct {
	Bucket              string    `yaml:"bucket"`
	Paths               []string  `yaml:"paths"`
	PolicyKind          string    `yaml:"policy_kind"`
	DocumentationStatus string    `yaml:"documentation_status"`
	Documented          yaml.Node `yaml:"documented_requests"`
	PeriodS             float64   `yaml:"period_s"`
	Assigned            float64   `yaml:"tank_request_budget"`
	Scope               string    `yaml:"scope"`
	Basis               string    `yaml:"basis"`
}

type usdmBudgetWire struct {
	weightBudgetWire `yaml:",inline"`
	Protection       []clientProtectionWire `yaml:"client_protection_limits"`
}

type endpointBudgetWire struct {
	Path       string  `yaml:"path"`
	Documented float64 `yaml:"documented_requests"`
	PeriodS    float64 `yaml:"period_s"`
	Assigned   float64 `yaml:"tank_request_budget"`
	Scope      string  `yaml:"scope"`
}

type okxBudgetWire struct {
	Provider       string `yaml:"provider"`
	budgetEvidence `yaml:",inline"`
	Endpoints      []endpointBudgetWire `yaml:"endpoint_limits"`
}

type bybitBudgetWire struct {
	Provider       string `yaml:"provider"`
	budgetEvidence `yaml:",inline"`
	Documented     float64 `yaml:"documented_requests"`
	PeriodS        float64 `yaml:"period_s"`
	Assigned       float64 `yaml:"tank_request_budget"`
	Scope          string  `yaml:"scope"`
}

type hyperliquidBudgetWire struct {
	weightBudgetWire `yaml:",inline"`
	Scope            string             `yaml:"scope"`
	Weights          map[string]float64 `yaml:"request_weights"`
	ExtraWeights     map[string]string  `yaml:"response_size_extra_weights"`
}

type publicClientBudgetWire struct {
	Provider            string    `yaml:"provider"`
	Host                string    `yaml:"host"`
	PolicyKind          string    `yaml:"policy_kind"`
	DocumentationStatus string    `yaml:"documentation_status"`
	Documented          yaml.Node `yaml:"documented_requests"`
	Assigned            float64   `yaml:"tank_request_budget"`
	PeriodS             float64   `yaml:"period_s"`
	Scope               string    `yaml:"scope"`
	Basis               string    `yaml:"basis"`
}

type etherscanBudgetWire struct {
	Provider        string `yaml:"provider"`
	budgetEvidence  `yaml:",inline"`
	Tier            string  `yaml:"tier"`
	Scope           string  `yaml:"scope"`
	DocumentedRPS   float64 `yaml:"documented_requests_per_s"`
	DocumentedDaily float64 `yaml:"documented_requests_per_day"`
	AssignedRPS     float64 `yaml:"tank_max_requests_per_s"`
	AssignedDaily   float64 `yaml:"tank_max_requests_per_day"`
}

type duckduckgoBudgetWire struct {
	Provider       string `yaml:"provider"`
	budgetEvidence `yaml:",inline"`
	Client         string    `yaml:"client"`
	Backend        string    `yaml:"backend"`
	Spacing        float64   `yaml:"minimum_request_spacing_s"`
	RetryDelays    []float64 `yaml:"retry_delays_s"`
	Cost           *float64  `yaml:"cost_usd"`
	KeyRequired    *bool     `yaml:"api_key_required"`
}

type apiLimitsWire struct {
	Version string      `yaml:"version"`
	Policy  string      `yaml:"policy"`
	Limits  []yaml.Node `yaml:"limits"`
}

func decodeBudgetNode[T any](node *yaml.Node) (T, error) {
	var result T
	data, err := yaml.Marshal(node)
	if err != nil {
		return result, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return result, fmt.Errorf("request budget must contain exactly one YAML document")
	}
	return result, nil
}

func positiveBudget(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func halfBudget(documented, assigned float64) error {
	if !positiveBudget(documented) || !positiveBudget(assigned) || assigned > documented/2 {
		return fmt.Errorf("request budget requires positive documented and assigned half-capacity")
	}
	return nil
}

type validatedBudgetDocument struct {
	Version     string
	Policy      string
	Rates       []rateBudgetWire
	Spot        *spotBudgetWire
	USDM        *usdmBudgetWire
	OKX         *okxBudgetWire
	Bybit       *bybitBudgetWire
	Hyperliquid *hyperliquidBudgetWire
	Public      []publicClientBudgetWire
	Etherscan   *etherscanBudgetWire
	Duckduckgo  *duckduckgoBudgetWire
}

func budgetProvider(node *yaml.Node) (string, error) {
	if node.Kind != yaml.MappingNode {
		return "", fmt.Errorf("request budget entry must be a mapping")
	}
	provider := ""
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value != "provider" {
			continue
		}
		value := node.Content[i+1]
		if provider != "" || value.Kind != yaml.ScalarNode || value.Tag != "!!str" || value.Value == "" {
			return "", fmt.Errorf("request budget requires one explicit provider")
		}
		provider = value.Value
	}
	if provider == "" {
		return "", fmt.Errorf("request budget requires provider")
	}
	return provider, nil
}

func validateClientProtection(rule clientProtectionWire) error {
	if rule.PolicyKind != "CLIENT_PROTECTION" || rule.DocumentationStatus != "NOT_CAPTURED" || !declaredNull(rule.Documented) ||
		rule.Scope != "IP" || !positiveIntegerBudget(rule.PeriodS) || !positiveIntegerBudget(rule.Assigned) || strings.TrimSpace(rule.Basis) == "" {
		return fmt.Errorf("invalid declared client-protection request budget")
	}
	var paths []string
	switch rule.Bucket {
	case "binance.usdm.funding":
		paths = []string{"/fapi/v1/fundingRate", "/fapi/v1/fundingInfo"}
	case "binance.usdm.statistics":
		paths = []string{"/futures/data/openInterestHist"}
	default:
		return fmt.Errorf("unsupported USDM client-protection bucket")
	}
	if len(paths) != len(rule.Paths) {
		return fmt.Errorf("USDM client-protection operation group differs")
	}
	seen := make(map[string]bool, len(paths))
	for _, path := range rule.Paths {
		if seen[path] {
			return fmt.Errorf("duplicate client-protection operation")
		}
		seen[path] = true
	}
	for _, path := range paths {
		if !seen[path] {
			return fmt.Errorf("USDM client-protection operation group differs")
		}
	}
	return nil
}

func parseAPILimits(data []byte) (validatedBudgetDocument, error) {
	var output validatedBudgetDocument
	var wire apiLimitsWire
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&wire); err != nil {
		return output, err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return output, fmt.Errorf("request budgets require exactly one YAML document")
	}
	if strings.TrimSpace(wire.Version) == "" || strings.TrimSpace(wire.Policy) == "" || len(wire.Limits) == 0 {
		return output, fmt.Errorf("request budgets require version, policy and limits")
	}
	output.Version, output.Policy = wire.Version, wire.Policy
	seen := make(map[string]bool, len(wire.Limits))
	for i := range wire.Limits {
		node := &wire.Limits[i]
		provider, err := budgetProvider(node)
		if err != nil {
			return output, err
		}
		if seen[provider] {
			return output, fmt.Errorf("duplicate request budget provider: %s", provider)
		}
		seen[provider] = true
		switch provider {
		case "sec.gov", "coinbase", "kraken":
			rule, err := decodeBudgetNode[rateBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(false, true); err != nil {
				return output, err
			}
			if !positiveBudget(rule.Assigned) {
				return output, fmt.Errorf("invalid assigned request rate")
			}
			if provider == "sec.gov" && rule.Documented == nil {
				return output, fmt.Errorf("SEC requires documented request rate")
			}
			if rule.Documented != nil {
				if !positiveIntegerBudget(*rule.Documented) {
					return output, fmt.Errorf("documented request rate must be a positive integer")
				}
				if err := halfBudget(*rule.Documented, rule.Assigned); err != nil {
					return output, err
				}
			}
			output.Rates = append(output.Rates, rule)
		case "binance.spot":
			rule, err := decodeBudgetNode[spotBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(false, true); err != nil {
				return output, err
			}
			if err := integerHalfBudget(rule.Documented, rule.Assigned); err != nil {
				return output, err
			}
			if rule.ObservedHeader != nil && *rule.ObservedHeader == "" {
				return output, fmt.Errorf("observed weight header must not be empty")
			}
			output.Spot = &rule
		case "binance.usdm":
			rule, err := decodeBudgetNode[usdmBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(false, true); err != nil {
				return output, err
			}
			if err := integerHalfBudget(rule.Documented, rule.Assigned); err != nil {
				return output, err
			}
			if len(rule.Protection) != 2 {
				return output, fmt.Errorf("USDM requires both client-protection groups")
			}
			groups := make(map[string]bool, 2)
			for _, group := range rule.Protection {
				if err := validateClientProtection(group); err != nil {
					return output, err
				}
				if groups[group.Bucket] {
					return output, fmt.Errorf("duplicate USDM protection group")
				}
				groups[group.Bucket] = true
			}
			output.USDM = &rule
		case "okx":
			rule, err := decodeBudgetNode[okxBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(true, true); err != nil {
				return output, err
			}
			decodedSHA, shaErr := hex.DecodeString(rule.SourceContent)
			if shaErr != nil || len(decodedSHA) != 32 || strings.ToLower(rule.SourceContent) != rule.SourceContent || len(rule.Endpoints) == 0 {
				return output, fmt.Errorf("OKX requires captured endpoint limits")
			}
			paths := make(map[string]bool, len(rule.Endpoints))
			for _, endpoint := range rule.Endpoints {
				if endpoint.Path == "" || paths[endpoint.Path] || !positiveIntegerBudget(endpoint.PeriodS) {
					return output, fmt.Errorf("invalid OKX endpoint limit")
				}
				paths[endpoint.Path] = true
				switch endpoint.Scope {
				case "IP", "IP_INSTRUMENT", "IP_INSTRUMENT_TYPE":
				default:
					return output, fmt.Errorf("unsupported OKX limit scope")
				}
				if err := integerHalfBudget(endpoint.Documented, endpoint.Assigned); err != nil {
					return output, err
				}
			}
			output.OKX = &rule
		case "bybit":
			rule, err := decodeBudgetNode[bybitBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(true, true); err != nil {
				return output, err
			}
			if rule.Scope != "IP" || !positiveIntegerBudget(rule.PeriodS) {
				return output, fmt.Errorf("invalid Bybit period or scope")
			}
			if err := integerHalfBudget(rule.Documented, rule.Assigned); err != nil {
				return output, err
			}
			output.Bybit = &rule
		case "hyperliquid":
			rule, err := decodeBudgetNode[hyperliquidBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(true, true); err != nil {
				return output, err
			}
			if err := integerHalfBudget(rule.Documented, rule.Assigned); err != nil {
				return output, err
			}
			if rule.Scope != "IP" || len(rule.Weights) == 0 || len(rule.ExtraWeights) == 0 {
				return output, fmt.Errorf("hyperliquid requires operation and response-size policies")
			}
			for operation, weight := range rule.Weights {
				if strings.TrimSpace(operation) == "" || !positiveBudget(weight) || math.Trunc(weight) != weight {
					return output, fmt.Errorf("invalid Hyperliquid operation weight")
				}
			}
			for operation, declaration := range rule.ExtraWeights {
				if strings.TrimSpace(operation) == "" || strings.TrimSpace(declaration) == "" {
					return output, fmt.Errorf("invalid hyperliquid response-size declaration")
				}
			}
			output.Hyperliquid = &rule
		case "binance.www", "binance.vision":
			rule, err := decodeBudgetNode[publicClientBudgetWire](node)
			if err != nil {
				return output, err
			}
			expectedHost := "www.binance.com"
			if provider == "binance.vision" {
				expectedHost = "data.binance.vision"
			}
			if rule.Host != expectedHost || rule.PolicyKind != "CLIENT_PROTECTION" || rule.DocumentationStatus != "NOT_CAPTURED" ||
				!declaredNull(rule.Documented) || rule.Scope != "IP" || !positiveIntegerBudget(rule.Assigned) || !positiveIntegerBudget(rule.PeriodS) || strings.TrimSpace(rule.Basis) == "" {
				return output, fmt.Errorf("invalid public-host client policy")
			}
			output.Public = append(output.Public, rule)
		case "etherscan":
			rule, err := decodeBudgetNode[etherscanBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(true, false); err != nil {
				return output, err
			}
			if rule.Scope != "API_KEY" || rule.Tier != "FREE" {
				return output, fmt.Errorf("invalid Etherscan tier or scope")
			}
			if !positiveIntegerBudget(rule.DocumentedRPS) {
				return output, fmt.Errorf("documented Etherscan rate must be a positive integer")
			}
			if err := halfBudget(rule.DocumentedRPS, rule.AssignedRPS); err != nil {
				return output, err
			}
			if err := integerHalfBudget(rule.DocumentedDaily, rule.AssignedDaily); err != nil {
				return output, err
			}
			output.Etherscan = &rule
		case "duckduckgo":
			rule, err := decodeBudgetNode[duckduckgoBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(false, true); err != nil {
				return output, err
			}
			if strings.TrimSpace(rule.Client) == "" || rule.Backend != "duckduckgo" || rule.Spacing < 0 || math.IsNaN(rule.Spacing) || math.IsInf(rule.Spacing, 0) || math.Trunc(rule.Spacing) != rule.Spacing ||
				rule.Cost == nil || *rule.Cost != 0 || rule.KeyRequired == nil || *rule.KeyRequired || len(rule.RetryDelays) == 0 {
				return output, fmt.Errorf("invalid DuckDuckGo client policy")
			}
			for _, delay := range rule.RetryDelays {
				if !positiveBudget(delay) || math.Trunc(delay) != delay {
					return output, fmt.Errorf("invalid DuckDuckGo retry delay")
				}
			}
			output.Duckduckgo = &rule
		default:
			return output, fmt.Errorf("unsupported configured request budget provider: %s", provider)
		}
	}
	return output, nil
}

func positiveIntegerBudget(value float64) bool {
	return positiveBudget(value) && math.Trunc(value) == value && value < 1<<53
}

func integerHalfBudget(documented, assigned float64) error {
	if !positiveIntegerBudget(documented) || !positiveIntegerBudget(assigned) {
		return fmt.Errorf("documented request counts and weights require exactly representable positive integers")
	}
	return halfBudget(documented, assigned)
}

func declaredNull(node yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Tag == "!!null" &&
		(node.Value == "" || node.Value == "~" || strings.EqualFold(node.Value, "null"))
}
