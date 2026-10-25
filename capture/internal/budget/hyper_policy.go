package budget

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
)

var ErrUnboundedResponseWeight = errors.New("unbounded response-size weight requires explicit post-response admission")

type requestMillis int64

func (n *requestMillis) UnmarshalJSON(data []byte) error {
	value, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64)
	if err != nil {
		return fmt.Errorf("request timestamp requires an exact integer")
	}
	*n = requestMillis(value)
	return nil
}

type hyperCandleRequestWire struct {
	Coin     string         `json:"coin"`
	Interval string         `json:"interval"`
	Start    *requestMillis `json:"startTime"`
	End      *requestMillis `json:"endTime"`
}

type hyperCandleBudgetRequest struct {
	Req *hyperCandleRequestWire `json:"req"`
}

type candleBudgetRange struct {
	Interval    string
	StartMillis int64
	EndMillis   int64
}

func decodeCandleBudgetRange(body json.RawMessage) (candleBudgetRange, error) {
	var request hyperCandleBudgetRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return candleBudgetRange{}, fmt.Errorf("invalid candle budget request: %w", err)
	}
	if request.Req == nil || request.Req.Coin == "" || request.Req.Interval == "" || request.Req.Start == nil || request.Req.End == nil ||
		*request.Req.Start < 0 || *request.Req.End < 0 {
		return candleBudgetRange{}, fmt.Errorf("candle budget requires explicit coin, interval and millisecond range")
	}
	switch request.Req.Interval {
	case "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M":
	default:
		return candleBudgetRange{}, fmt.Errorf("unsupported documented candle interval")
	}
	start, end := int64(*request.Req.Start), int64(*request.Req.End)
	if end < start {
		return candleBudgetRange{}, fmt.Errorf("candle budget interval is inverted")
	}
	return candleBudgetRange{Interval: request.Req.Interval, StartMillis: start, EndMillis: end}, nil
}

func (p Policies) hyperPlan(body json.RawMessage) ([]Bucket, error) {
	if p.hyperliquid == nil {
		return nil, fmt.Errorf("missing configured hyperliquid operation policy")
	}
	weighted, exists := p.weighted["hyperliquid"]
	if !exists {
		return nil, fmt.Errorf("missing configured hyperliquid weight budget")
	}
	var operation struct {
		Type *string `json:"type"`
	}
	if err := json.Unmarshal(body, &operation); err != nil {
		return nil, fmt.Errorf("invalid hyperliquid budget request: %w", err)
	}
	if operation.Type == nil {
		return nil, errors.New("Unconfigured Hyperliquid request weight: None")
	}
	kind := *operation.Type
	base, exists := p.hyperliquid.Weights[kind]
	if !exists {
		return nil, errors.New("Unconfigured Hyperliquid request weight: " + kind)
	}
	weight := float64(base)
	switch kind {
	case "meta", "metaAndAssetCtxs", "l2Book", "allMids", "userRole":
	case "recentTrades":
		return nil, ErrUnboundedResponseWeight
	case "fundingHistory":
		extra, exists := p.hyperliquid.ResponseWeights[kind]
		if !exists {
			return nil, fmt.Errorf("missing configured funding response weight")
		}
		extraWeight, err := responseWeightUpperBound(500, extra)
		if err != nil {
			return nil, err
		}
		weight += extraWeight
	case "candleSnapshot":
		request, err := decodeCandleBudgetRange(body)
		if err != nil {
			return nil, err
		}
		rowsUpperBound := int64(5000)
		if request.Interval == "1m" {
			rowsUpperBound = min(5000, (request.EndMillis-request.StartMillis)/60000+2)
		}
		extra, exists := p.hyperliquid.ResponseWeights[kind]
		if !exists {
			return nil, fmt.Errorf("missing configured candle response weight")
		}
		extraWeight, err := responseWeightUpperBound(rowsUpperBound, extra)
		if err != nil {
			return nil, err
		}
		weight += extraWeight
	default:
		return nil, errors.New("Unconfigured Hyperliquid request weight: " + kind)
	}
	bucket := Bucket{Key: weighted.Key, Capacity: weighted.Capacity, WindowS: 60, Weight: weight}
	if err := bucket.Validate(); err != nil {
		return nil, err
	}
	return []Bucket{bucket}, nil
}

func responseWeightUpperBound(rowsUpperBound int64, rule budgetpolicy.ResponseWeightPolicy) (float64, error) {
	units := rowsUpperBound/rule.PerItems + boolInt(rowsUpperBound%rule.PerItems != 0)
	if units <= 0 || rule.Weight > (1<<53)/units {
		return 0, ErrInvalidBudget
	}
	return float64(units * rule.Weight), nil
}

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
