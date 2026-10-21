package venue

import (
	"strings"
)

type Instrument struct {
	ID         string
	TokenID    string
	Venue      string
	MarketType string
	Symbol     string
	QuoteAsset any
	WSSymbol   string
	Active     bool

	ContractMultiplier any
	FundingIntervalH   any
	TickSize           any
	QuoteUSDRate       any
	QuoteUSDRateAsOf   any
}

func (i *Instrument) Quote() string {
	s, _ := i.QuoteAsset.(string)
	return strings.ToUpper(s)
}

func (i *Instrument) QuoteValue() any { return i.QuoteAsset }

func Key(venueName, marketType string) string {
	v := strings.ToLower(venueName)
	mt := marketType
	if mt == "" {
		mt = "SPOT"
	}
	perp := strings.ToLower(mt) != "spot"
	switch {
	case strings.HasPrefix(v, "binance"):
		if perp || strings.HasSuffix(v, "usdm") {
			return "binance.usdm"
		}
		return "binance.spot"
	case strings.HasPrefix(v, "bybit"):
		if perp {
			return "bybit.linear"
		}
		return "bybit.spot"
	case strings.HasPrefix(v, "okx"):
		if perp {
			return "okx.swap"
		}
		return "okx.spot"
	case strings.HasPrefix(v, "hyperliquid"):
		return "hyperliquid.perp"
	}
	return strings.SplitN(v, ".", 2)[0] + ".spot"
}

func (i *Instrument) Key() string { return Key(i.Venue, i.MarketType) }

func (i *Instrument) VenueName() string { return strings.SplitN(i.Key(), ".", 2)[0] }

func (i *Instrument) Spot() bool { return strings.HasSuffix(i.Key(), ".spot") }

func (i *Instrument) RoutingSymbol() string {
	if strings.HasPrefix(i.Key(), "kraken") {
		if i.WSSymbol != "" {
			return i.WSSymbol
		}
		return strings.ReplaceAll(i.Symbol, "XBT", "BTC")
	}
	return i.Symbol
}

func (i *Instrument) Clone() *Instrument {
	c := *i
	return &c
}

func FromRegistry(m map[string]any) *Instrument {
	s := func(k string) string { v, _ := m[k].(string); return v }
	active := true
	if v, ok := m["active"].(bool); ok {
		active = v
	}
	return &Instrument{
		ID: s("instrument_id"), TokenID: s("token_id"), Venue: s("venue"), MarketType: s("market_type"),
		Symbol: s("venue_symbol"), QuoteAsset: m["quote_asset"], WSSymbol: s("ws_symbol"), Active: active,
		ContractMultiplier: m["contract_multiplier"], FundingIntervalH: m["funding_interval_h"], TickSize: m["tick_size"],
		QuoteUSDRate: m["quote_usd_rate"], QuoteUSDRateAsOf: m["quote_usd_rate_as_of"],
	}
}
