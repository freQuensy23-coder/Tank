package stream

import (
	"sort"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const BinanceUSDMDepth = "wss://fstream.binance.com/public/stream"

func Connection(inst *venue.Instrument, depthOnly bool) (string, []map[string]any) {
	key := inst.Key()
	s := inst.Symbol
	perp := !strings.HasSuffix(key, "spot")
	switch {
	case strings.HasPrefix(key, "binance"):
		lower := strings.ToLower(s)
		channels := []string{lower + "@aggTrade", lower + "@depth@100ms"}
		root := "wss://data-stream.binance.vision/stream"
		if perp {
			if depthOnly {
				channels = []string{lower + "@depth@100ms"}
				root = BinanceUSDMDepth
			} else {
				channels = []string{lower + "@aggTrade", lower + "@markPrice@1s", lower + "@forceOrder"}
				root = "wss://fstream.binance.com/market/stream"
			}
		}
		return root + "?streams=" + strings.Join(channels, "/"), nil
	case strings.HasPrefix(key, "coinbase"):
		return "wss://ws-feed.exchange.coinbase.com", []map[string]any{{
			"type": "subscribe", "product_ids": []any{s}, "channels": []any{"matches", "level2_batch", "heartbeat"},
		}}
	case strings.HasPrefix(key, "okx"):
		var args []any
		for _, c := range []string{"trades", "books"} {
			args = append(args, map[string]any{"channel": c, "instId": s})
		}
		if perp {
			for _, c := range []string{"open-interest", "funding-rate", "mark-price"} {
				args = append(args, map[string]any{"channel": c, "instId": s})
			}
			args = append(args, map[string]any{"channel": "index-tickers", "instId": strings.TrimSuffix(s, "-SWAP")})
			args = append(args, map[string]any{"channel": "liquidation-orders", "instType": "SWAP"})
		}
		return "wss://ws.okx.com/ws/v5/public", []map[string]any{{"op": "subscribe", "args": args}}
	case strings.HasPrefix(key, "bybit"):
		args := []any{"publicTrade." + s, "orderbook.200." + s}
		if perp {
			args = append(args, "tickers."+s, "allLiquidation."+s)
		}
		return "wss://stream.bybit.com/v5/public/" + strings.SplitN(key, ".", 2)[1], []map[string]any{{"op": "subscribe", "args": args}}
	case strings.HasPrefix(key, "kraken"):
		symbol := inst.RoutingSymbol()
		return "wss://ws.kraken.com/v2", []map[string]any{
			{"method": "subscribe", "params": map[string]any{"channel": "trade", "symbol": []any{symbol}, "snapshot": false}},
			{"method": "subscribe", "params": map[string]any{"channel": "book", "symbol": []any{symbol}, "depth": int64(1000), "snapshot": true}},
		}
	}
	var subs []map[string]any
	for _, c := range []string{"trades", "l2Book", "activeAssetCtx"} {
		subs = append(subs, map[string]any{"method": "subscribe", "subscription": map[string]any{"type": c, "coin": s}})
	}
	return "wss://api.hyperliquid.xyz/ws", subs
}

func EndpointKey(inst *venue.Instrument) string {
	url, _ := Connection(inst, false)
	return strings.SplitN(url, "?", 2)[0]
}

func EndpointKeys(inst *venue.Instrument) []string {
	primary := EndpointKey(inst)
	if inst.Key() == "binance.usdm" {
		url, _ := Connection(inst, true)
		return []string{primary, strings.SplitN(url, "?", 2)[0]}
	}
	return []string{primary}
}

func SubscriptionMessages(instruments []*venue.Instrument, subscribe bool, endpoint string) []map[string]any {
	if len(instruments) == 0 {
		return nil
	}
	v := instruments[0].Key()
	action := "unsubscribe"
	if subscribe {
		action = "subscribe"
	}
	if strings.HasPrefix(v, "binance") {
		set := map[string]bool{}
		depthOnly := strings.HasSuffix(endpoint, "/public/stream")
		for _, inst := range instruments {
			url, _ := Connection(inst, depthOnly)
			for _, c := range strings.Split(strings.SplitN(url, "?streams=", 2)[1], "/") {
				set[c] = true
			}
		}
		channels := make([]string, 0, len(set))
		for c := range set {
			channels = append(channels, c)
		}
		sort.Strings(channels)
		params := make([]any, len(channels))
		for i, c := range channels {
			params[i] = c
		}
		return []map[string]any{{"method": strings.ToUpper(action), "params": params, "id": int64(1)}}
	}
	var messages []map[string]any
	for _, inst := range instruments {
		_, items := Connection(inst, false)
		for _, item := range items {
			msg := map[string]any{}
			for k, val := range item {
				msg[k] = val
			}
			switch {
			case msg["op"] != nil:
				msg["op"] = action
			case msg["method"] != nil:
				msg["method"] = action
			case msg["type"] != nil:
				msg["type"] = action
			}
			if strings.HasPrefix(v, "okx") {
				var kept []any
				for _, a := range pyval.List(msg["args"]) {
					if pyval.Map(a)["channel"] != "liquidation-orders" {
						kept = append(kept, a)
					}
				}
				msg["args"] = kept
			}
			messages = append(messages, msg)
		}
	}
	if strings.HasPrefix(v, "okx") || strings.HasPrefix(v, "bybit") {
		seen := map[string]bool{}
		var args []any
		for _, msg := range messages {
			for _, a := range pyval.List(msg["args"]) {
				k := string(pyjson.MustDumps(a, true))
				if !seen[k] {
					seen[k] = true
					args = append(args, a)
				}
			}
		}
		size := len(args)
		if strings.HasPrefix(v, "bybit") {
			size = 10
		}
		if size < 1 {
			size = 1
		}
		var out []map[string]any
		for i := 0; i < len(args); i += size {
			end := min(i+size, len(args))
			out = append(out, map[string]any{"op": action, "args": append([]any(nil), args[i:end]...)})
		}
		return out
	}
	if strings.HasPrefix(v, "coinbase") {
		var products []any
		for _, inst := range instruments {
			products = append(products, inst.Symbol)
		}
		return []map[string]any{{"type": action, "product_ids": products, "channels": []any{"matches", "level2_batch", "heartbeat"}}}
	}
	if strings.HasPrefix(v, "kraken") {
		var symbols []any
		for _, inst := range instruments {
			symbols = append(symbols, inst.RoutingSymbol())
		}
		var out []map[string]any
		for _, item := range messages[:min(2, len(messages))] {
			params := map[string]any{}
			for k, val := range pyval.Map(item["params"]) {
				params[k] = val
			}
			params["symbol"] = symbols
			out = append(out, map[string]any{"method": action, "params": params})
		}
		return out
	}
	return messages
}

type Routed struct {
	Inst    *venue.Instrument
	Payload any
}

func RouteIndexed(index map[string]*venue.Instrument, venueName string, raw any) []Routed {
	m := pyval.Map(raw)
	if m == nil {
		return nil
	}
	data := pyval.GetOr(m, "data", raw)
	switch venueName {
	case "binance":
		d := pyval.Map(data)
		if d == nil {
			return nil
		}
		symbol := pyval.GetOr(d, "s", pyval.Map(d["o"])["s"])
		if s, ok := symbol.(string); ok {
			if inst := index[s]; inst != nil {
				return []Routed{{inst, raw}}
			}
		}
		return nil
	case "coinbase":
		if s, ok := m["product_id"].(string); ok {
			if inst := index[s]; inst != nil {
				return []Routed{{inst, raw}}
			}
		}
		return nil
	case "bybit":
		topic, _ := m["topic"].(string)
		parts := strings.Split(topic, ".")
		if inst := index[parts[len(parts)-1]]; inst != nil {
			return []Routed{{inst, raw}}
		}
		return nil
	}
	rows := pyval.List(data)
	if rows == nil {
		if venueName == "hyperliquid" {
			if d := pyval.Map(data); d != nil {
				if s, ok := d["coin"].(string); ok {
					if inst := index[s]; inst != nil {
						return []Routed{{inst, raw}}
					}
				}
			}
		}
		return nil
	}
	var order []string
	groups := map[string][]any{}
	add := func(symbol string, row any) {
		if _, ok := groups[symbol]; !ok {
			order = append(order, symbol)
		}
		groups[symbol] = append(groups[symbol], row)
	}
	if venueName == "okx" {
		arg := pyval.Map(m["arg"])
		def := arg["instId"]
		for _, row := range rows {
			symbol := pyval.GetOr(pyval.Map(row), "instId", def)
			s, _ := symbol.(string)
			if arg["channel"] == "index-tickers" {
				s += "-SWAP"
			} else if symbol == nil {
				continue
			}
			add(s, row)
		}
	} else {
		field := "coin"
		if venueName == "kraken" {
			field = "symbol"
		}
		for _, row := range rows {
			if s, ok := pyval.Map(row)[field].(string); ok {
				add(s, row)
			}
		}
	}
	var out []Routed
	for _, symbol := range order {
		inst := index[symbol]
		if inst == nil {
			continue
		}
		payload := map[string]any{}
		for k, v := range m {
			payload[k] = v
		}
		payload["data"] = groups[symbol]
		out = append(out, Routed{inst, payload})
	}
	return out
}
