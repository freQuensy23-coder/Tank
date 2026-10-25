package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

type budgetEvidence struct {
	Basis           string   `json:"basis"`
	SourceRef       string   `json:"source_ref"`
	SourceContent   string   `json:"source_content_sha256"`
	VerifiedAt      string   `json:"verified_at"`
	InferenceLimits []string `json:"inference_limits"`
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
	Provider   string  `json:"provider"`
	Documented *int64  `json:"documented_requests_per_s"`
	Assigned   float64 `json:"tank_max_requests_per_s"`
	budgetEvidence
}

type weightBudgetWire struct {
	Provider   string `json:"provider"`
	Documented int64  `json:"documented_weight_per_min"`
	Assigned   int64  `json:"tank_max_weight_per_min"`
	budgetEvidence
}

type spotBudgetWire struct {
	weightBudgetWire
	ObservedHeader *string `json:"observed_header"`
}

type clientProtectionWire struct {
	Bucket              string          `json:"bucket"`
	Paths               []string        `json:"paths"`
	PolicyKind          string          `json:"policy_kind"`
	DocumentationStatus string          `json:"documentation_status"`
	Documented          json.RawMessage `json:"documented_requests"`
	PeriodS             int64           `json:"period_s"`
	Assigned            int64           `json:"tank_request_budget"`
	Scope               string          `json:"scope"`
	Basis               string          `json:"basis"`
}

type usdmBudgetWire struct {
	weightBudgetWire
	Protection []clientProtectionWire `json:"client_protection_limits"`
}

type endpointBudgetWire struct {
	Path       string `json:"path"`
	Documented int64  `json:"documented_requests"`
	PeriodS    int64  `json:"period_s"`
	Assigned   int64  `json:"tank_request_budget"`
	Scope      string `json:"scope"`
}

type okxBudgetWire struct {
	Provider string `json:"provider"`
	budgetEvidence
	Endpoints []endpointBudgetWire `json:"endpoint_limits"`
}

type bybitBudgetWire struct {
	Provider string `json:"provider"`
	budgetEvidence
	Documented int64  `json:"documented_requests"`
	PeriodS    int64  `json:"period_s"`
	Assigned   int64  `json:"tank_request_budget"`
	Scope      string `json:"scope"`
}

type hyperliquidBudgetWire struct {
	weightBudgetWire
	Scope        string            `json:"scope"`
	Weights      map[string]int64  `json:"request_weights"`
	ExtraWeights map[string]string `json:"response_size_extra_weights"`
}

type publicClientBudgetWire struct {
	Provider            string          `json:"provider"`
	Host                string          `json:"host"`
	PolicyKind          string          `json:"policy_kind"`
	DocumentationStatus string          `json:"documentation_status"`
	Documented          json.RawMessage `json:"documented_requests"`
	Assigned            int64           `json:"tank_request_budget"`
	PeriodS             int64           `json:"period_s"`
	Scope               string          `json:"scope"`
	Basis               string          `json:"basis"`
}

type etherscanBudgetWire struct {
	Provider string `json:"provider"`
	budgetEvidence
	Tier            string  `json:"tier"`
	Scope           string  `json:"scope"`
	DocumentedRPS   int64   `json:"documented_requests_per_s"`
	DocumentedDaily int64   `json:"documented_requests_per_day"`
	AssignedRPS     float64 `json:"tank_max_requests_per_s"`
	AssignedDaily   int64   `json:"tank_max_requests_per_day"`
}

type duckduckgoBudgetWire struct {
	Provider string `json:"provider"`
	budgetEvidence
	Client      string  `json:"client"`
	Backend     string  `json:"backend"`
	Spacing     *int64  `json:"minimum_request_spacing_s"`
	RetryDelays []int64 `json:"retry_delays_s"`
	Cost        *int64  `json:"cost_usd"`
	KeyRequired *bool   `json:"api_key_required"`
}

type apiLimitsWire struct {
	Version string            `json:"version"`
	Policy  string            `json:"policy"`
	Limits  []json.RawMessage `json:"limits"`
}

func decodeBudgetRecord[T any](data []byte) (T, error) {
	var result T
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return result, fmt.Errorf("request budget requires exactly one JSON record")
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

func budgetProvider(data json.RawMessage) (string, error) {
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return "", fmt.Errorf("request budget entry must be an object")
	}
	var discriminator struct {
		Provider *string `json:"provider"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return "", err
	}
	if discriminator.Provider == nil || *discriminator.Provider == "" {
		return "", fmt.Errorf("request budget requires explicit provider")
	}
	return *discriminator.Provider, nil
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
	if err := validateBudgetYAML(data); err != nil {
		return output, err
	}
	raw, err := SafeLoad(data)
	if err != nil {
		return output, err
	}
	encoded, err := pyjson.Dumps(raw, false)
	if err != nil {
		return output, err
	}
	wire, err := decodeBudgetRecord[apiLimitsWire](encoded)
	if err != nil {
		return output, err
	}
	if strings.TrimSpace(wire.Version) == "" || strings.TrimSpace(wire.Policy) == "" || len(wire.Limits) == 0 {
		return output, fmt.Errorf("request budgets require version, policy and limits")
	}
	output.Version, output.Policy = wire.Version, wire.Policy
	seen := make(map[string]bool, len(wire.Limits))
	for i := range wire.Limits {
		node := wire.Limits[i]
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
			rule, err := decodeBudgetRecord[rateBudgetWire](node)
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
				if err := halfBudget(float64(*rule.Documented), rule.Assigned); err != nil {
					return output, err
				}
			}
			output.Rates = append(output.Rates, rule)
		case "binance.spot":
			rule, err := decodeBudgetRecord[spotBudgetWire](node)
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
			rule, err := decodeBudgetRecord[usdmBudgetWire](node)
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
			rule, err := decodeBudgetRecord[okxBudgetWire](node)
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
			rule, err := decodeBudgetRecord[bybitBudgetWire](node)
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
			rule, err := decodeBudgetRecord[hyperliquidBudgetWire](node)
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
				if strings.TrimSpace(operation) == "" || !positiveIntegerBudget(weight) {
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
			rule, err := decodeBudgetRecord[publicClientBudgetWire](node)
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
			rule, err := decodeBudgetRecord[etherscanBudgetWire](node)
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
			if err := halfBudget(float64(rule.DocumentedRPS), rule.AssignedRPS); err != nil {
				return output, err
			}
			if err := integerHalfBudget(rule.DocumentedDaily, rule.AssignedDaily); err != nil {
				return output, err
			}
			output.Etherscan = &rule
		case "duckduckgo":
			rule, err := decodeBudgetRecord[duckduckgoBudgetWire](node)
			if err != nil {
				return output, err
			}
			if err := rule.validate(false, true); err != nil {
				return output, err
			}
			if strings.TrimSpace(rule.Client) == "" || rule.Backend != "duckduckgo" || rule.Spacing == nil || *rule.Spacing < 0 ||
				rule.Cost == nil || *rule.Cost != 0 || rule.KeyRequired == nil || *rule.KeyRequired || len(rule.RetryDelays) == 0 {
				return output, fmt.Errorf("invalid DuckDuckGo client policy")
			}
			for _, delay := range rule.RetryDelays {
				if !positiveIntegerBudget(delay) {
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

func positiveIntegerBudget(value int64) bool {
	return value > 0 && value < 1<<53
}

func integerHalfBudget(documented, assigned int64) error {
	if !positiveIntegerBudget(documented) || !positiveIntegerBudget(assigned) {
		return fmt.Errorf("documented request counts and weights require exactly representable positive integers")
	}
	return halfBudget(float64(documented), float64(assigned))
}

func declaredNull(value json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func validateBudgetYAML(data []byte) error {
	var root yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&root); err != nil {
		return err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("request budgets require exactly one YAML document")
	}
	return validateBudgetYAMLNode(&root, make(map[*yaml.Node]bool))
}

func validateBudgetYAMLNode(node *yaml.Node, active map[*yaml.Node]bool) error {
	if node == nil || active[node] {
		return fmt.Errorf("cyclic or missing request policy YAML node")
	}
	active[node] = true
	defer delete(active, node)
	if node.Kind == yaml.AliasNode {
		return validateBudgetYAMLNode(node.Alias, active)
	}
	if node.Kind == yaml.MappingNode {
		if len(node.Content)%2 != 0 {
			return fmt.Errorf("invalid request policy YAML mapping")
		}
		keys := make(map[string]bool)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || keys[key.Value] {
				return fmt.Errorf("duplicate or non-scalar request policy key")
			}
			keys[key.Value] = true
		}
	}
	for _, child := range node.Content {
		if err := validateBudgetYAMLNode(child, active); err != nil {
			return err
		}
	}
	return nil
}
