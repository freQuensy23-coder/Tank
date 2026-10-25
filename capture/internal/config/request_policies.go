package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
)

var responseWeightPattern = regexp.MustCompile("^([1-9][0-9]*) per([1-9][0-9]*)returned items$")

func LoadRequestPolicies(path string) (budget.Policies, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return budget.Policies{}, err
	}
	document, err := parseAPILimits(data)
	if err != nil {
		return budget.Policies{}, err
	}
	return document.requestPolicies()
}

func (c *Config) RequestPolicies() (budget.Policies, error) {
	return c.APILimits.requestPolicies()
}

func (document validatedBudgetDocument) requestPolicies() (budget.Policies, error) {
	var input budgetpolicy.PolicyInput
	for _, rule := range document.Rates {
		switch rule.Provider {
		case "coinbase", "kraken":
			input.Rates = append(input.Rates, budgetpolicy.RatePolicy{Key: rule.Provider, Capacity: rule.Assigned})
		}
	}
	if rule := document.Spot; rule != nil {
		input.Weighted = append(input.Weighted, budgetpolicy.WeightedPolicy{Key: rule.Provider, Documented: float64(rule.Documented), Capacity: float64(rule.Assigned)})
	}
	if rule := document.USDM; rule != nil {
		input.Weighted = append(input.Weighted, budgetpolicy.WeightedPolicy{Key: rule.Provider, Documented: float64(rule.Documented), Capacity: float64(rule.Assigned)})
		for _, group := range rule.Protection {
			input.USDMProtection = append(input.USDMProtection, budgetpolicy.ProtectedOperationPolicy{
				WindowPolicy: budgetpolicy.WindowPolicy{Key: group.Bucket, Capacity: float64(group.Assigned), WindowS: float64(group.PeriodS)},
				Paths:        group.Paths,
			})
		}
	}
	if rule := document.OKX; rule != nil {
		for _, endpoint := range rule.Endpoints {
			input.OKX = append(input.OKX, budgetpolicy.EndpointPolicy{Path: endpoint.Path, Capacity: float64(endpoint.Assigned), WindowS: float64(endpoint.PeriodS), Scope: endpoint.Scope})
		}
	}
	if rule := document.Bybit; rule != nil {
		input.Bybit = &budgetpolicy.WindowPolicy{Key: rule.Provider, Capacity: float64(rule.Assigned), WindowS: float64(rule.PeriodS)}
	}
	if rule := document.Hyperliquid; rule != nil {
		input.Weighted = append(input.Weighted, budgetpolicy.WeightedPolicy{Key: rule.Provider, Documented: float64(rule.Documented), Capacity: float64(rule.Assigned)})
		input.Hyperliquid = &budgetpolicy.HyperliquidPolicy{Weights: make(map[string]int64), ResponseWeights: make(map[string]budgetpolicy.ResponseWeightPolicy)}
		for operation, weight := range rule.Weights {
			if !positiveIntegerBudget(weight) {
				return budget.Policies{}, fmt.Errorf("hyperliquid operation weight is not representable")
			}
			input.Hyperliquid.Weights[operation] = weight
		}
		for operation, declaration := range rule.ExtraWeights {
			parts := responseWeightPattern.FindStringSubmatch(declaration)
			if len(parts) != 3 {
				return budget.Policies{}, fmt.Errorf("unsupported configured response-size weight declaration")
			}
			weight, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return budget.Policies{}, err
			}
			perItems, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return budget.Policies{}, err
			}
			input.Hyperliquid.ResponseWeights[operation] = budgetpolicy.ResponseWeightPolicy{Weight: weight, PerItems: perItems}
		}
	}
	for _, rule := range document.Public {
		input.Public = append(input.Public, budgetpolicy.PublicHostPolicy{
			WindowPolicy: budgetpolicy.WindowPolicy{Key: rule.Provider, Capacity: float64(rule.Assigned), WindowS: float64(rule.PeriodS)},
			Host:         rule.Host,
		})
	}
	return budget.NewPolicies(input)
}
