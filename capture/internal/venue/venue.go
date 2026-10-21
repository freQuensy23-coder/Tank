// Package venue holds instrument identities and the venue keys of
// tank.market.normalize.venue_key.
package venue

import (
	"strings"
)

// Instrument is one registry instrument plus the metadata capture learns at runtime
// (OKX ctVal, funding interval, the stablecoin USD sample). Values keep the Python types
// of the registry: strings, numbers or nil.
type Instrument struct {
	ID         string
	TokenID    string
	Venue      string
	MarketType string
	Symbol     string
	QuoteAsset any // string or nil
	WSSymbol   string
	Active     bool

	ContractMultiplier any
	FundingIntervalH   any
	TickSize           any
	QuoteUSDRate       any
	QuoteUSDRateAsOf   any
}

// Quote is instrument.get("quote_asset", "").upper().
func (i *Instrument) Quote() string {
	s, _ := i.QuoteAsset.(string)
	return strings.ToUpper(s)
}

// QuoteValue is instrument.get("quote_asset") (nil when absent).
func (i *Instrument) QuoteValue() any { return i.QuoteAsset }

// Key is tank.market.normalize.venue_key.
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

// Key is venue_key(instrument).
func (i *Instrument) Key() string { return Key(i.Venue, i.MarketType) }

// VenueName is venue_key(instrument).split(".")[0].
func (i *Instrument) VenueName() string { return strings.SplitN(i.Key(), ".", 2)[0] }

// Spot reports a spot market path.
func (i *Instrument) Spot() bool { return strings.HasSuffix(i.Key(), ".spot") }

// RoutingSymbol is streams.routing_symbol: the product identity frames carry.
func (i *Instrument) RoutingSymbol() string {
	if strings.HasPrefix(i.Key(), "kraken") {
		if i.WSSymbol != "" {
			return i.WSSymbol
		}
		return strings.ReplaceAll(i.Symbol, "XBT", "BTC")
	}
	return i.Symbol
}

// Clone is a copy safe to hand to another goroutine.
func (i *Instrument) Clone() *Instrument {
	c := *i
	return &c
}
