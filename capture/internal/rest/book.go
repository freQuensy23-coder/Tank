package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

func (a *Adapter) DepthSnapshot(ctx context.Context) (map[string]any, error) {
	var base, path string
	switch a.key {
	case "binance.spot":
		base, path = Binance, "/api/v3/depth"
	case "binance.usdm":
		base, path = BinanceUSDM, "/fapi/v1/depth"
	default:
		return nil, fmt.Errorf("no listed REST book snapshot for %s", a.key)
	}
	body, err := a.Client.Do(ctx, "GET", base+path, map[string]string{"symbol": a.Inst.Symbol, "limit": "1000"}, nil, a.Priority)
	if err != nil {
		return nil, err
	}
	return object(body, "depth snapshot")
}

func (a *Adapter) IndependentBook(ctx context.Context, stream map[string]any, now func() time.Time) (map[string]any, error) {
	if now == nil {
		now = a.now
	}
	var body any
	var err error
	switch a.key {
	case "binance.spot":
		body, err = a.Client.Do(ctx, "GET", Binance+"/api/v3/depth", map[string]string{"symbol": a.Inst.Symbol, "limit": "5"}, nil, 2)
	case "coinbase.spot":
		body, err = a.Client.Do(ctx, "GET", Coinbase+"/products/"+a.Inst.Symbol+"/book", map[string]string{"level": "2"}, nil, 2)
	default:
		return map[string]any{"second_path_confirmed": false, "second_path_reason": "NO_LISTED_SECOND_PATH"}, nil
	}
	if err != nil {
		return nil, err
	}
	raw, err := object(body, "REST book")
	if err != nil {
		return nil, err
	}
	top := func(side string) (any, error) {
		levels, err := list(raw[side], "book "+side)
		if err != nil || len(levels) == 0 {
			return nil, fmt.Errorf("IndexError: list index out of range")
		}
		level, err := list(levels[0], "book level")
		if err != nil || len(level) == 0 {
			return nil, fmt.Errorf("IndexError: list index out of range")
		}
		return level[0], nil
	}
	bidRaw, err := top("bids")
	if err != nil {
		return nil, err
	}
	askRaw, err := top("asks")
	if err != nil {
		return nil, err
	}
	ts := pytime.ISO(now())
	bid, err := pyval.Decimal(bidRaw)
	if err != nil {
		return nil, err
	}
	ask, err := pyval.Decimal(askRaw)
	if err != nil {
		return nil, err
	}
	if bid.Sign() <= 0 || ask.Cmp(bid) <= 0 {
		return nil, pyval.Semantic("independent REST book is empty or crossed")
	}
	const prec = pydec.Precise
	mid := pydec.MustDiv(pydec.Add(bid, ask, prec), pydec.FromInt(2), prec)
	spread := pydec.Mul(pydec.MustDiv(pydec.Sub(ask, bid, prec), mid, prec), pydec.FromInt(10000), prec)
	streamMid, err := pyval.Decimal(stream["mid"])
	if err != nil {
		return nil, err
	}
	ratio, err := pydec.Div(mid, streamMid, prec)
	if err != nil {
		return nil, fmt.Errorf("decimal.DivisionByZero: stream mid is zero")
	}
	divergence := pydec.Mul(pydec.Sub(ratio, pydec.FromInt(1), prec).Abs(prec), pydec.FromInt(10000), prec)
	tickBPS := pydec.Zero
	if tick := a.Inst.TickSize; pyval.Truthy(tick) {
		d, err := pyval.Decimal(tick)
		if err != nil {
			return nil, err
		}
		tickBPS = pydec.Mul(pydec.MustDiv(d, mid, prec), pydec.FromInt(10000), prec)
	}
	streamSpread, err := pyval.Decimal(stream["spread_bps"])
	if err != nil {
		return nil, err
	}
	tolerance := pydec.Max(pydec.Max(spread, streamSpread), pydec.Mul(pydec.FromInt(2), tickBPS, prec))
	confirmed := divergence.Cmp(tolerance) <= 0
	var reason any
	if !confirmed {
		reason = "REST_STREAM_DISAGREEMENT"
	}
	return map[string]any{
		"second_path_confirmed": confirmed,
		"independent_rest": map[string]any{
			"best_bid":             bid.String(),
			"best_ask":             ask.String(),
			"mid":                  mid.String(),
			"spread_bps":           spread.String(),
			"ts":                   ts,
			"ts_prec":              pytime.CollectionOnly,
			"received_ts":          pytime.ISO(now()),
			"received_ts_prec":     pytime.CollectionOnly,
			"source":               "PUBLIC_REST",
			"price_divergence_bps": divergence.String(),
			"tolerance_bps":        tolerance.String(),
			"tolerance_method":     "MAX_OBSERVED_SPREAD_OR_TWO_TICKS",
		},
		"second_path_reason": reason,
	}, nil
}

func FXRate(ctx context.Context, c *Client, quote string) (string, error) {
	body, err := c.Do(ctx, "GET", Coinbase+"/products/"+quote+"-USD/stats", nil, nil, 0)
	if err != nil {
		return "", err
	}
	m, err := object(body, "stats")
	if err != nil {
		return "", err
	}
	last, err := need(m, "last")
	if err != nil {
		return "", err
	}
	rate, err := pyval.Decimal(last)
	if err != nil {
		return "", err
	}
	return rate.String(), nil
}

func floatRepr(f float64) string {
	s, err := pyjson.FloatRepr(f)
	if err != nil {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return s
}

func str(v any) string {
	if f, ok := v.(float64); ok {
		return floatRepr(f)
	}
	return pyval.Str(v)
}

func repr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case string:
		return reprString(x)
	case json.Number:
		if pyval.IsInt(x) {
			return pyval.Str(x)
		}
		return "Decimal(" + reprString(pyval.Str(x)) + ")"
	case float64:
		return floatRepr(x)
	case []any:
		parts := make([]string, len(x))
		for i, item := range x {
			parts[i] = repr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := sortedKeys(x)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = reprString(k) + ": " + repr(x[k])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return pyval.Str(v)
}

func reprString(s string) string {
	quote := byte('\'')
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == '\\':
			b.WriteString(`\\`)
		case r == rune(quote):
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}
