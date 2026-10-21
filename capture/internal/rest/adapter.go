package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const (
	Binance     = "https://api.binance.com"
	BinanceUSDM = "https://fapi.binance.com"
	OKX         = "https://www.okx.com"
	Bybit       = "https://api.bybit.com"
	Coinbase    = "https://api.exchange.coinbase.com"
	Kraken      = "https://api.kraken.com"
	Hyperliquid = "https://api.hyperliquid.xyz/info"
)

var ErrNoTradeHistory error = &pyval.SemanticError{Msg: "NO_LISTED_TRADE_BACKFILL"}

type Adapter struct {
	Inst     *venue.Instrument
	Client   *Client
	Priority int
	key      string
}

func NewAdapter(inst *venue.Instrument, c *Client, priority int) *Adapter {
	return &Adapter{Inst: inst, Client: c, Priority: priority, key: inst.Key()}
}

func (a *Adapter) now() time.Time { return a.Client.Now().UTC().Truncate(time.Microsecond) }

func (a *Adapter) get(ctx context.Context, base, path string, params map[string]string) (any, error) {
	if strings.HasPrefix(a.key, "kraken") {
		if pair, ok := params["pair"]; ok {
			params["pair"] = strings.ReplaceAll(pair, "/", "")
		}
	}
	return a.Client.Do(ctx, "GET", base+path, params, nil, a.Priority)
}

func (a *Adapter) info(ctx context.Context, payload map[string]any) (any, error) {
	return a.Client.Do(ctx, "POST", Hyperliquid, nil, payload, a.Priority)
}

func (a *Adapter) result(body any) (any, error) {
	switch {
	case strings.HasPrefix(a.key, "okx"):
		m, ok := body.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("OKX response is not an object")
		}
		if pyval.Str(m["code"]) != "0" {
			return nil, pyval.Semantic("OKX response error %s", pyval.Str(m["msg"]))
		}
		return need(m, "data")
	case strings.HasPrefix(a.key, "bybit"):
		m, ok := body.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Bybit response is not an object")
		}
		if !isZero(m["retCode"]) {
			return nil, pyval.Semantic("Bybit response error %s", pyval.Str(m["retMsg"]))
		}
		result, err := need(m, "result")
		if err != nil {
			return nil, err
		}
		r, ok := result.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Bybit result is not an object")
		}
		return need(r, "list")
	case strings.HasPrefix(a.key, "kraken"):
		m, ok := body.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Kraken response is not an object")
		}
		if pyval.Truthy(m["error"]) {
			return nil, pyval.Semantic("Kraken response error %s", repr(m["error"]))
		}
		return need(m, "result")
	}
	return body, nil
}

func isZero(v any) bool {
	switch x := v.(type) {
	case json.Number:
		d, err := pydec.Parse(string(x))
		return err == nil && d.IsZero()
	case bool:
		return !x
	case int:
		return x == 0
	case int64:
		return x == 0
	}
	return false
}

func need(m map[string]any, key string) (any, error) {
	v, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("KeyError: '%s'", key)
	}
	return v, nil
}

func list(v any, what string) ([]any, error) {
	l, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s is not a list", what)
	}
	return l, nil
}

func object(v any, what string) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s is not an object", what)
	}
	return m, nil
}

func first(v any, what string) (map[string]any, error) {
	l, err := list(v, what)
	if err != nil {
		return nil, err
	}
	if len(l) == 0 {
		return nil, fmt.Errorf("IndexError: list index out of range")
	}
	return object(l[0], what)
}

func pairValue(v any) (any, error) {
	m, err := object(v, "Kraken result")
	if err != nil {
		return nil, err
	}
	for _, key := range sortedKeys(m) {
		if key != "last" {
			return m[key], nil
		}
	}
	return nil, fmt.Errorf("Kraken result holds no pair")
}

func timestamp(t time.Time) float64 { return float64(t.UTC().UnixMicro()) / 1e6 }

func millis(t time.Time) int64 { return int64(timestamp(t) * 1000) }

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func (a *Adapter) category() string { return strings.SplitN(a.key, ".", 2)[1] }

func (a *Adapter) Discover(ctx context.Context) (map[string]any, error) {
	s := a.Inst.Symbol
	switch a.key {
	case "okx.swap":
		body, err := a.get(ctx, OKX, "/api/v5/public/instruments", map[string]string{"instType": "SWAP", "instId": s})
		if err != nil {
			return nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, err
		}
		r, err := first(data, "OKX instruments")
		if err != nil {
			return nil, err
		}
		fields := map[string]any{}
		for _, name := range []string{"ctValCcy", "ctVal", "tickSz", "lotSz", "state"} {
			if fields[name], err = need(r, name); err != nil {
				return nil, err
			}
		}
		if !pyval.Equal(fields["ctValCcy"], strings.Split(s, "-")[0]) {
			return nil, pyval.Semantic("OKX contract value currency not base")
		}
		value, err := pyval.Decimal(fields["ctVal"])
		if err != nil {
			return nil, err
		}
		mult := r["ctMult"]
		if !pyval.Truthy(mult) {
			mult = "1"
		}
		multiplier, err := pyval.Decimal(mult)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"tick_size":           fields["tickSz"],
			"lot_size":            fields["lotSz"],
			"active":              pyval.Equal(fields["state"], "live"),
			"metadata_status":     "LIVE_VERIFIED",
			"contract_multiplier": pydec.Mul(value, multiplier, pydec.DefaultPrec).String(),
		}, nil
	case "hyperliquid.perp":
		meta, _, err := a.metaAndContexts(ctx)
		if err != nil {
			return nil, err
		}
		universe, err := list(meta["universe"], "Hyperliquid universe")
		if err != nil {
			return nil, err
		}
		for _, x := range universe {
			r, err := object(x, "Hyperliquid asset")
			if err != nil {
				return nil, err
			}
			if r["name"] != s {
				continue
			}
			decimals, err := pyval.Int(r["szDecimals"])
			if err != nil {
				return nil, err
			}
			delisted := pyval.Truthy(pyval.GetOr(r, "isDelisted", false))
			return map[string]any{
				"lot_size":            lotSize(decimals),
				"contract_multiplier": "1",
				"funding_interval_h":  "1",
				"active":              !delisted,
				"metadata_status":     "LIVE_VERIFIED",
				"tick_size":           nil,
				"tick_size_reason":    "SIGNIFICANT_FIGURES_AND_SIZE_DEPENDENT",
			}, nil
		}
		return nil, fmt.Errorf("coroutine raised StopIteration")
	}

	return map[string]any{"contract_multiplier": "1", "metadata_status": "SEMANTICS_REGISTRY"}, nil
}

func lotSize(decimals int64) string {
	if decimals <= 0 {
		return itoa(int64(math.Pow(10, float64(-decimals))))
	}
	f, _ := strconv.ParseFloat("1e-"+itoa(decimals), 64)
	return floatRepr(f)
}

func (a *Adapter) metaAndContexts(ctx context.Context) (map[string]any, []any, error) {
	body, err := a.info(ctx, map[string]any{"type": "metaAndAssetCtxs"})
	if err != nil {
		return nil, nil, err
	}
	pair, err := list(body, "metaAndAssetCtxs")
	if err != nil || len(pair) != 2 {
		return nil, nil, fmt.Errorf("metaAndAssetCtxs is not [meta, contexts]")
	}
	meta, err := object(pair[0], "Hyperliquid meta")
	if err != nil {
		return nil, nil, err
	}
	contexts, err := list(pair[1], "Hyperliquid contexts")
	if err != nil {
		return nil, nil, err
	}
	return meta, contexts, nil
}

func (a *Adapter) Klines(ctx context.Context, start, end *time.Time) ([]any, error) {
	s := a.Inst.Symbol
	k := a.key
	params := map[string]string{}
	var body any
	var err error
	switch {
	case strings.HasPrefix(k, "binance"):
		base, path := Binance, "/api/v3/klines"
		if !strings.HasSuffix(k, "spot") {
			base, path = BinanceUSDM, "/fapi/v1/klines"
		}
		if start != nil {
			params["startTime"] = itoa(millis(*start))
		}
		if end != nil {
			params["endTime"] = itoa(millis(*end))
		}
		params["symbol"], params["interval"], params["limit"] = s, "1m", "3"
		if start != nil {
			params["limit"] = "1000"
		}
		body, err = a.get(ctx, base, path, params)
	case strings.HasPrefix(k, "okx"):
		if end != nil {
			params["after"] = itoa(millis(*end))
		}
		if start != nil {
			params["before"] = itoa(millis(*start))
		}
		path := "/api/v5/market/candles"
		params["instId"], params["bar"], params["limit"] = s, "1m", "3"
		if start != nil {
			path, params["limit"] = "/api/v5/market/history-candles", "300"
		}
		if body, err = a.get(ctx, OKX, path, params); err == nil {
			body, err = a.result(body)
		}
	case strings.HasPrefix(k, "bybit"):
		if start != nil {
			params["start"] = itoa(millis(*start))
		}
		if end != nil {
			params["end"] = itoa(millis(*end))
		}
		params["category"], params["symbol"], params["interval"], params["limit"] = a.category(), s, "1", "3"
		if start != nil {
			params["limit"] = "1000"
		}
		if body, err = a.get(ctx, Bybit, "/v5/market/kline", params); err == nil {
			body, err = a.result(body)
		}
	case strings.HasPrefix(k, "coinbase"):
		from := a.now().Add(-3 * time.Minute)
		if start != nil {
			from = *start
		}
		params["start"] = pytime.ISO(from)
		if end != nil {
			params["end"] = pytime.ISO(*end)
		}
		params["granularity"] = "60"
		body, err = a.get(ctx, Coinbase, "/products/"+s+"/candles", params)
	case strings.HasPrefix(k, "kraken"):
		from := a.now().Add(-3 * time.Minute)
		if start != nil {
			from = *start
		}
		params["since"] = itoa(int64(timestamp(from)))
		params["pair"], params["interval"] = s, "1"
		if body, err = a.get(ctx, Kraken, "/0/public/OHLC", params); err == nil {
			if body, err = a.result(body); err == nil {
				body, err = pairValue(body)
			}
		}
	default:
		from := a.now().Add(-3 * time.Minute)
		if start != nil {
			from = *start
		}
		to := a.now()
		if end != nil {
			to = *end
		}
		body, err = a.info(ctx, map[string]any{
			"type": "candleSnapshot",
			"req":  map[string]any{"coin": s, "interval": "1m", "startTime": millis(from), "endTime": millis(to)},
		})
	}
	if err != nil {
		return nil, err
	}
	return list(body, "klines response")
}

func (a *Adapter) Ticker(ctx context.Context) (map[string]any, error) {
	s := a.Inst.Symbol
	k := a.key
	switch {
	case strings.HasPrefix(k, "binance"):
		base, path := Binance, "/api/v3/ticker/24hr"
		if !strings.HasSuffix(k, "spot") {
			base, path = BinanceUSDM, "/fapi/v1/ticker/24hr"
		}
		body, err := a.get(ctx, base, path, map[string]string{"symbol": s})
		if err != nil {
			return nil, err
		}
		return object(body, "ticker")
	case strings.HasPrefix(k, "okx"):
		body, err := a.get(ctx, OKX, "/api/v5/market/ticker", map[string]string{"instId": s})
		if err != nil {
			return nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, err
		}
		return first(data, "OKX ticker")
	case strings.HasPrefix(k, "bybit"):
		body, err := a.get(ctx, Bybit, "/v5/market/tickers", map[string]string{"category": a.category(), "symbol": s})
		if err != nil {
			return nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, err
		}
		return first(data, "Bybit ticker")
	case strings.HasPrefix(k, "coinbase"):
		body, err := a.get(ctx, Coinbase, "/products/"+s+"/stats", map[string]string{})
		if err != nil {
			return nil, err
		}
		return object(body, "ticker")
	case strings.HasPrefix(k, "kraken"):
		body, err := a.get(ctx, Kraken, "/0/public/Ticker", map[string]string{"pair": s})
		if err != nil {
			return nil, err
		}
		result, err := a.result(body)
		if err != nil {
			return nil, err
		}
		m, err := object(result, "Kraken ticker")
		if err != nil {
			return nil, err
		}
		keys := sortedKeys(m)
		if len(keys) == 0 {
			return nil, fmt.Errorf("StopIteration")
		}
		return object(m[keys[0]], "Kraken ticker")
	}
	meta, contexts, err := a.metaAndContexts(ctx)
	if err != nil {
		return nil, err
	}
	universe, err := list(meta["universe"], "Hyperliquid universe")
	if err != nil {
		return nil, err
	}
	for i, x := range universe {
		if pyval.Map(x)["name"] == s {
			if i >= len(contexts) {
				return nil, fmt.Errorf("IndexError: list index out of range")
			}
			return object(contexts[i], "Hyperliquid context")
		}
	}
	return nil, fmt.Errorf("coroutine raised StopIteration")
}

type Derivatives struct {
	OI       []any
	Current  map[string]any
	Settled  []any
	Interval any
	Mark     any
}

func (a *Adapter) Derivatives(ctx context.Context) (*Derivatives, error) {
	s := a.Inst.Symbol
	switch a.key {
	case "binance.usdm":
		oi, err := a.get(ctx, BinanceUSDM, "/fapi/v1/openInterest", map[string]string{"symbol": s})
		if err != nil {
			return nil, err
		}
		history, err := a.get(ctx, BinanceUSDM, "/futures/data/openInterestHist", map[string]string{"symbol": s, "period": "5m", "limit": "2"})
		if err != nil {
			return nil, err
		}
		current, err := a.get(ctx, BinanceUSDM, "/fapi/v1/premiumIndex", map[string]string{"symbol": s})
		if err != nil {
			return nil, err
		}
		settled, err := a.get(ctx, BinanceUSDM, "/fapi/v1/fundingRate", map[string]string{"symbol": s, "limit": "1000"})
		if err != nil {
			return nil, err
		}
		info, err := a.get(ctx, BinanceUSDM, "/fapi/v1/fundingInfo", map[string]string{})
		if err != nil {
			return nil, err
		}
		points, err := list(history, "openInterestHist")
		if err != nil {
			return nil, err
		}
		cur, err := object(current, "premiumIndex")
		if err != nil {
			return nil, err
		}
		rows, err := list(settled, "fundingRate")
		if err != nil {
			return nil, err
		}
		infos, err := list(info, "fundingInfo")
		if err != nil {
			return nil, err
		}
		interval := any(int64(8))
		for _, r := range infos {
			if m := pyval.Map(r); m != nil && m["symbol"] == s {
				if interval, err = need(m, "fundingIntervalHours"); err != nil {
					return nil, err
				}
				break
			}
		}
		mark, err := need(cur, "markPrice")
		if err != nil {
			return nil, err
		}
		return &Derivatives{append([]any{oi}, points...), cur, rows, str(interval), mark}, nil
	case "okx.swap":
		body, err := a.get(ctx, OKX, "/api/v5/public/open-interest", map[string]string{"instType": "SWAP", "instId": s})
		if err != nil {
			return nil, err
		}
		oi, err := a.result(body)
		if err != nil {
			return nil, err
		}
		if body, err = a.get(ctx, OKX, "/api/v5/public/funding-rate", map[string]string{"instId": s}); err != nil {
			return nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, err
		}
		current, err := first(data, "OKX funding rate")
		if err != nil {
			return nil, err
		}
		if body, err = a.get(ctx, OKX, "/api/v5/public/funding-rate-history", map[string]string{"instId": s, "limit": "100"}); err != nil {
			return nil, err
		}
		settled, err := a.result(body)
		if err != nil {
			return nil, err
		}
		prev, following := current["prevFundingTime"], current["fundingTime"]
		if !pyval.Truthy(prev) || !pyval.Truthy(following) {
			return nil, pyval.Semantic("OKX funding interval timestamps absent")
		}
		p, err := pyval.Decimal(prev)
		if err != nil {
			return nil, err
		}
		f, err := pyval.Decimal(following)
		if err != nil {
			return nil, err
		}
		interval, err := pydec.Div(pydec.Sub(f, p, pydec.DefaultPrec), pydec.FromInt(3600000), pydec.DefaultPrec)
		if err != nil {
			return nil, err
		}
		oiRows, err := list(oi, "OKX open interest")
		if err != nil {
			return nil, err
		}
		settledRows, err := list(settled, "OKX funding history")
		if err != nil {
			return nil, err
		}

		return &Derivatives{oiRows, current, settledRows, interval.String(), nil}, nil
	case "bybit.linear":
		body, err := a.get(ctx, Bybit, "/v5/market/open-interest", map[string]string{
			"category": "linear", "symbol": s, "intervalTime": "5min", "limit": "200",
		})
		if err != nil {
			return nil, err
		}
		oi, err := a.result(body)
		if err != nil {
			return nil, err
		}
		current, err := a.Ticker(ctx)
		if err != nil {
			return nil, err
		}
		if body, err = a.get(ctx, Bybit, "/v5/market/funding/history", map[string]string{"category": "linear", "symbol": s, "limit": "200"}); err != nil {
			return nil, err
		}
		settled, err := a.result(body)
		if err != nil {
			return nil, err
		}
		interval := current["fundingIntervalHour"]
		if !pyval.Truthy(interval) {
			interval = a.Inst.FundingIntervalH
		}
		if interval == nil {
			return nil, pyval.Semantic("Bybit funding interval absent")
		}
		mark, err := need(current, "markPrice")
		if err != nil {
			return nil, err
		}
		oiRows, err := list(oi, "Bybit open interest")
		if err != nil {
			return nil, err
		}
		settledRows, err := list(settled, "Bybit funding history")
		if err != nil {
			return nil, err
		}
		return &Derivatives{oiRows, current, settledRows, str(interval), mark}, nil
	case "hyperliquid.perp":
		current, err := a.Ticker(ctx)
		if err != nil {
			return nil, err
		}
		hour := int64(timestamp(a.now())) / 3600
		settled, err := a.info(ctx, map[string]any{"type": "fundingHistory", "coin": s, "startTime": (hour - 7*24) * 3600000})
		if err != nil {
			return nil, err
		}
		rows, err := list(settled, "fundingHistory")
		if err != nil {
			return nil, err
		}
		mark, err := need(current, "markPx")
		if err != nil {
			return nil, err
		}
		return &Derivatives{[]any{current}, current, rows, "1", mark}, nil
	}
	return &Derivatives{OI: []any{}, Settled: []any{}}, nil
}

func (a *Adapter) Trades(ctx context.Context, start, end *time.Time, cursor any) ([]any, any, error) {
	s := a.Inst.Symbol
	k := a.key
	params := map[string]string{}
	switch {
	case strings.HasPrefix(k, "binance"):
		if start != nil {
			params["startTime"] = itoa(millis(*start))
		}
		if end != nil {
			params["endTime"] = itoa(millis(*end))
		}
		if cursor != nil {
			params = map[string]string{"fromId": pyval.Str(cursor)}
		}
		base, path := Binance, "/api/v3/aggTrades"
		if !strings.HasSuffix(k, "spot") {
			base, path = BinanceUSDM, "/fapi/v1/aggTrades"
		}
		params["symbol"], params["limit"] = s, "1000"
		body, err := a.get(ctx, base, path, params)
		if err != nil {
			return nil, nil, err
		}
		rows, err := list(body, "aggTrades")
		return rows, nil, err
	case strings.HasPrefix(k, "okx"):
		if pyval.Truthy(cursor) {
			params["after"] = pyval.Str(cursor)
		} else if end != nil {

			params["type"], params["after"] = "2", itoa(millis(*end)+1)
		}
		params["instId"], params["limit"] = s, "100"
		body, err := a.get(ctx, OKX, "/api/v5/market/history-trades", params)
		if err != nil {
			return nil, nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, nil, err
		}
		rows, err := list(data, "OKX trades")
		if err != nil || len(rows) == 0 {
			return rows, nil, err
		}
		last, err := object(rows[len(rows)-1], "OKX trade")
		if err != nil {
			return nil, nil, err
		}
		next, err := need(last, "tradeId")
		return rows, next, err
	case strings.HasPrefix(k, "bybit"):
		body, err := a.get(ctx, Bybit, "/v5/market/recent-trade", map[string]string{"category": a.category(), "symbol": s, "limit": "1000"})
		if err != nil {
			return nil, nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, nil, err
		}
		rows, err := list(data, "Bybit trades")
		return rows, nil, err
	case strings.HasPrefix(k, "coinbase"):
		if pyval.Truthy(cursor) {
			params["after"] = pyval.Str(cursor)
		}
		params["limit"] = "1000"
		body, err := a.get(ctx, Coinbase, "/products/"+s+"/trades", params)
		if err != nil {
			return nil, nil, err
		}
		rows, err := list(body, "Coinbase trades")
		if err != nil || len(rows) == 0 {
			return rows, nil, err
		}
		last, err := object(rows[len(rows)-1], "Coinbase trade")
		if err != nil {
			return nil, nil, err
		}
		next, err := need(last, "trade_id")
		return rows, next, err
	case strings.HasPrefix(k, "kraken"):
		if pyval.Truthy(cursor) {
			params["since"] = pyval.Str(cursor)
		} else if start != nil {
			params["since"] = itoa(int64(timestamp(*start) * 1_000_000_000))
		}
		params["pair"] = s
		body, err := a.get(ctx, Kraken, "/0/public/Trades", params)
		if err != nil {
			return nil, nil, err
		}
		result, err := a.result(body)
		if err != nil {
			return nil, nil, err
		}
		pair, err := pairValue(result)
		if err != nil {
			return nil, nil, err
		}
		rows, err := list(pair, "Kraken trades")
		if err != nil {
			return nil, nil, err
		}
		next, err := need(result.(map[string]any), "last")
		return rows, next, err
	}

	return nil, nil, ErrNoTradeHistory
}

func (a *Adapter) OIHistory(ctx context.Context, start, end time.Time, cursor string) ([]any, string, error) {
	if a.key != "bybit.linear" {
		return nil, "", pyval.Semantic("NO_LISTED_OI_HISTORY")
	}
	params := map[string]string{"startTime": itoa(millis(start)), "endTime": itoa(millis(end))}
	if cursor != "" {
		params["cursor"] = cursor
	}
	params["category"], params["symbol"], params["intervalTime"], params["limit"] = "linear", a.Inst.Symbol, "5min", "200"
	body, err := a.get(ctx, Bybit, "/v5/market/open-interest", params)
	if err != nil {
		return nil, "", err
	}
	data, err := a.result(body)
	if err != nil {
		return nil, "", err
	}
	rows, err := list(data, "Bybit open interest")
	if err != nil {
		return nil, "", err
	}
	next := pyval.Map(pyval.Map(body)["result"])["nextPageCursor"]
	if !pyval.Truthy(next) {
		return rows, "", nil
	}
	return rows, pyval.Str(next), nil
}

func (a *Adapter) RecentTrades(ctx context.Context, limit int) ([]any, error) {
	s := a.Inst.Symbol
	k := a.key
	n := strconv.Itoa(limit)
	switch {
	case strings.HasPrefix(k, "binance"):
		base, path := Binance, "/api/v3/aggTrades"
		if !strings.HasSuffix(k, "spot") {
			base, path = BinanceUSDM, "/fapi/v1/aggTrades"
		}
		body, err := a.get(ctx, base, path, map[string]string{"symbol": s, "limit": n})
		if err != nil {
			return nil, err
		}
		return list(body, "aggTrades")
	case strings.HasPrefix(k, "okx"):
		body, err := a.get(ctx, OKX, "/api/v5/market/trades", map[string]string{"instId": s, "limit": n})
		if err != nil {
			return nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, err
		}
		return list(data, "OKX trades")
	case strings.HasPrefix(k, "bybit"):
		body, err := a.get(ctx, Bybit, "/v5/market/recent-trade", map[string]string{"category": a.category(), "symbol": s, "limit": n})
		if err != nil {
			return nil, err
		}
		data, err := a.result(body)
		if err != nil {
			return nil, err
		}
		return list(data, "Bybit trades")
	case strings.HasPrefix(k, "coinbase"):
		body, err := a.get(ctx, Coinbase, "/products/"+s+"/trades", map[string]string{"limit": n})
		if err != nil {
			return nil, err
		}
		return list(body, "Coinbase trades")
	case strings.HasPrefix(k, "kraken"):

		body, err := a.get(ctx, Kraken, "/0/public/Trades", map[string]string{"pair": s, "count": n})
		if err != nil {
			return nil, err
		}
		result, err := a.result(body)
		if err != nil {
			return nil, err
		}
		pair, err := pairValue(result)
		if err != nil {
			return nil, err
		}
		return list(pair, "Kraken trades")
	}
	return nil, ErrNoTradeHistory
}
