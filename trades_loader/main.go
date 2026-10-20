// trades_loader downloads public trades over REST from Binance, Coinbase, OKX and Kraken
// and writes them to files the main Tank service reads (one direction only).
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func set(csv string) map[string]bool {
	m := map[string]bool{}
	for _, v := range strings.Split(csv, ",") {
		if v = strings.TrimSpace(v); v != "" {
			m[v] = true
		}
	}
	return m
}

func main() {
	registry := flag.String("registry", "/app/config/registry.yaml", "Tank registry with the instruments")
	out := flag.String("out", "/data/trades_loader", "output directory")
	venues := flag.String("venues", "binance,coinbase,okx,kraken", "venues to load")
	markets := flag.String("markets", "SPOT", "market types to load (SPOT,PERP_LINEAR)")
	backfill := flag.Duration("backfill", 26*time.Hour, "how far back to start when there is no position")
	retain := flag.Duration("retain", 48*time.Hour, "how long hour files are kept")
	ua := flag.String("user-agent", "TankV0/1.0 (inbox@ai.com)", "User-Agent header")
	only := flag.String("only", "", "comma-separated instrument_ids (testing)")
	flag.Parse()

	ins, err := loadInstruments(*registry, set(strings.ToLower(*venues)), set(strings.ToUpper(*markets)))
	if err != nil {
		log.Fatal(err)
	}
	if *only != "" {
		keep := set(*only)
		filtered := ins[:0]
		for _, in := range ins {
			if keep[in.ID] {
				filtered = append(filtered, in)
			}
		}
		ins = filtered
	}
	client := &http.Client{Timeout: 30 * time.Second}
	// At most half of each venue's public limit, shared by all its instruments (02 §4).
	lim := map[string]*Limiter{
		"binance.spot":  NewLimiter(100 * time.Millisecond), // aggTrades weight 4; 6000/min
		"binance.usdm":  NewLimiter(1 * time.Second),        // aggTrades weight 20; 2400/min
		"coinbase.spot": NewLimiter(250 * time.Millisecond), // 10 req/s public
		"okx":           NewLimiter(250 * time.Millisecond), // trades/history-trades 20 per 2 s
		"kraken.spot":   NewLimiter(2 * time.Second),        // ~1 req/s public
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for _, in := range ins {
		perp := strings.ToUpper(in.MarketType) != "SPOT"
		var v Venue
		idle := 2 * time.Second
		switch strings.ToLower(in.Venue) {
		case "binance":
			if perp {
				v = &binance{h: &httpClient{client, lim["binance.usdm"], *ua}, base: "https://fapi.binance.com/fapi/v1"}
			} else {
				v = &binance{h: &httpClient{client, lim["binance.spot"], *ua}, base: "https://api.binance.com/api/v3"}
			}
		case "coinbase":
			v = &coinbase{h: &httpClient{client, lim["coinbase.spot"], *ua}}
		case "okx":
			v, idle = &okx{h: &httpClient{client, lim["okx"], *ua}}, 3*time.Second
		case "kraken":
			v, idle = &kraken{h: &httpClient{client, lim["kraken.spot"], *ua}}, 0
		default:
			continue
		}
		st, err := OpenStore(*out, in)
		if err != nil {
			log.Fatal(err)
		}
		j := &job{in: in, venue: v, store: st, backfill: *backfill, retain: *retain, idle: idle}
		go j.run(ctx)
	}
	log.Printf("trades_loader: %d instruments → %s", len(ins), *out)
	<-ctx.Done()
}
