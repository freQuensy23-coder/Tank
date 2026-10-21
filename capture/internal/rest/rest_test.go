package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type canned struct {
	status int
	header http.Header
	body   string
	err    error
}

type recorded struct {
	Method, URL string
	Params      map[string]string
	JSON        any
	UserAgent   string
}

type exchange struct {
	mu        sync.Mutex
	responses []canned
	requests  []recorded
}

func (e *exchange) RoundTrip(req *http.Request) (*http.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := recorded{Method: req.Method, URL: req.URL.Scheme + "://" + req.URL.Host + req.URL.Path, Params: map[string]string{}, UserAgent: req.Header.Get("User-Agent")}
	for k, v := range req.URL.Query() {
		r.Params[k] = v[0]
	}
	if req.Body != nil {
		data, _ := io.ReadAll(req.Body)
		if len(data) > 0 {
			r.JSON, _ = pyval.Decode(data)
		}
	}
	e.requests = append(e.requests, r)
	if len(e.responses) == 0 {
		return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("no canned response")), Header: http.Header{}, Request: req}, nil
	}
	next := e.responses[0]
	e.responses = e.responses[1:]
	if next.err != nil {
		return nil, next.err
	}
	status := next.status
	if status == 0 {
		status = 200
	}
	header := next.header
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader([]byte(next.body))), Request: req}, nil
}

var now = time.Date(2026, 10, 21, 12, 34, 56, 789012000, time.UTC)

func newClient(t *testing.T, responses ...canned) (*Client, *exchange, *budget.MemoryLedger) {
	t.Helper()
	ledger := budget.NewMemoryLedger(nil)
	gate := budget.NewGate(ledger, "test")
	gate.Interval = func(string) time.Duration { return 0 }
	c := NewClient("TankV0/1.0 (test)", gate, func() time.Time { return now })
	ex := &exchange{responses: responses}
	c.HTTP = &http.Client{Transport: ex}
	return c, ex, ledger
}

func ok(body string) canned { return canned{body: body} }

type vector struct {
	Name, Key, Call string
	Instrument      map[string]any
	Args            map[string]any
	Priority        int64
	Responses       []any
	Requests        []any
	Result          any
	Error           any
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	data, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pyval.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	var out []vector
	for _, item := range raw.(map[string]any)["cases"].([]any) {
		m := item.(map[string]any)
		priority, _ := pyval.Int(m["priority"])
		out = append(out, vector{
			Name: m["name"].(string), Key: m["key"].(string), Call: m["call"].(string),
			Instrument: m["instrument"].(map[string]any), Args: m["args"].(map[string]any), Priority: priority,
			Responses: m["responses"].([]any), Requests: m["requests"].([]any), Result: m["result"], Error: m["error"],
		})
	}
	return out
}

func timeArg(args map[string]any, name string) *time.Time {
	v, _ := args[name].(string)
	if v == "" {
		return nil
	}
	t := pytime.MustParse(v)
	return &t
}

func call(ctx context.Context, a *Adapter, v vector) (any, error) {
	start, end := timeArg(v.Args, "start"), timeArg(v.Args, "end")
	cursor := v.Args["cursor"]
	switch v.Call {
	case "klines":
		return a.Klines(ctx, start, end)
	case "ticker":
		return a.Ticker(ctx)
	case "derivatives":
		d, err := a.Derivatives(ctx)
		if err != nil {
			return nil, err
		}
		return []any{d.OI, d.Current, d.Settled, d.Interval, d.Mark}, nil
	case "trades":
		rows, next, err := a.Trades(ctx, start, end, cursor)
		if err != nil {
			return nil, err
		}
		return []any{rows, next}, nil
	case "oi_history":
		var s, e time.Time
		if start != nil {
			s, e = *start, *end
		}
		c, _ := cursor.(string)
		rows, next, err := a.OIHistory(ctx, s, e, c)
		if err != nil {
			return nil, err
		}
		var n any
		if next != "" {
			n = next
		}
		return []any{rows, n}, nil
	case "discover":
		return a.Discover(ctx)
	case "depth":
		return a.DepthSnapshot(ctx)
	case "independent_book":
		return a.IndependentBook(ctx, v.Args["stream"].(map[string]any), func() time.Time { return now })
	case "fx":
		return FXRate(ctx, a.Client, v.Args["quote"].(string))
	}
	return nil, fmt.Errorf("unknown call %s", v.Call)
}

func TestAdaptersMatchPython(t *testing.T) {
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			var responses []canned
			for _, r := range v.Responses {
				responses = append(responses, ok(r.(string)))
			}
			c, ex, ledger := newClient(t, responses...)
			a := NewAdapter(venue.FromRegistry(v.Instrument), c, int(v.Priority))
			result, err := call(context.Background(), a, v)
			if len(ex.requests) != len(v.Requests) {
				t.Fatalf("%d requests, want %d: %+v", len(ex.requests), len(v.Requests), ex.requests)
			}
			charged := map[string]float64{}
			windows := map[string]float64{}
			for i, want := range v.Requests {
				w := want.(map[string]any)
				got := ex.requests[i]
				params := map[string]string{}
				for k, x := range w["params"].(map[string]any) {
					params[k] = x.(string)
				}
				if got.Method != w["method"] || got.URL != w["url"] || params2(got.Params) != params2(params) {
					t.Errorf("request %d: %s %s %v, want %s %s %v", i, got.Method, got.URL, got.Params, w["method"], w["url"], params)
				}
				if pyjson.Canonical(got.JSON) != pyjson.Canonical(w["json"]) {
					t.Errorf("request %d json %s, want %s", i, pyjson.Canonical(got.JSON), pyjson.Canonical(w["json"]))
				}
				if got.UserAgent != "TankV0/1.0 (test)" {
					t.Errorf("User-Agent %q", got.UserAgent)
				}
				host := strings.SplitN(strings.TrimPrefix(got.URL, "https://"), "/", 2)[0]
				path := "/" + strings.SplitN(strings.TrimPrefix(got.URL, "https://"), "/", 2)[1]
				payload, _ := got.JSON.(map[string]any)
				buckets, err := budget.Constraints(host, path, got.Params, payload)
				if err != nil {
					t.Fatal(err)
				}
				for _, b := range buckets {
					charged[b.Name()] += b.Weight
					windows[b.Name()] = b.WindowS
				}
			}
			for name, weight := range charged {
				if used := ledger.Used(name, windows[name]); used != weight {
					t.Errorf("bucket %s charged %v, want %v", name, used, weight)
				}
			}
			if v.Error != nil {
				message := v.Error.(string)
				kind, text, _ := strings.Cut(message, ": ")
				if err == nil {
					t.Fatalf("no error, want %s", message)
				}
				if kind == "SemanticError" && !errors.Is(err, pyval.ErrSemantic) {
					t.Errorf("error %v is not a SemanticError", err)
				}
				if err.Error() != text && err.Error() != message {
					t.Errorf("error %q, want %q", err.Error(), message)
				}
				return
			}
			if err != nil {
				t.Fatalf("error %v", err)
			}
			if got, want := pyjson.Canonical(result), pyjson.Canonical(v.Result); got != want {
				t.Errorf("result\n%s\nwant\n%s", got, want)
			}
		})
	}
}

const hlMeta = `[{"universe":[{"name":"ETH","szDecimals":4},{"name":"BTC","szDecimals":5}]},[{"markPx":"3100.1","oraclePx":"3100.0"},{"markPx":"83210.0","oraclePx":"83205.0"}]]`

func hlInstrument(symbol string) *venue.Instrument {
	return &venue.Instrument{ID: "hyperliquid:perp_linear:" + symbol, TokenID: symbol, Venue: "hyperliquid", MarketType: "PERP_LINEAR", Symbol: symbol, Active: true}
}

func TestHyperliquidContextsAreShared(t *testing.T) {
	c, ex, ledger := newClient(t, ok(hlMeta), ok(hlMeta))
	mono := now
	c.monotonic = func() time.Time { return mono }
	ctx := context.Background()
	btc, err := NewAdapter(hlInstrument("BTC"), c, 0).Ticker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mono = mono.Add(9 * time.Second)
	eth, err := NewAdapter(hlInstrument("ETH"), c, 0).Ticker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ex.requests) != 1 {
		t.Fatalf("%d requests for two instruments within HL_CONTEXTS_S", len(ex.requests))
	}
	if btc["markPx"] != "83210.0" && pyval.Str(btc["markPx"]) != "83210.0" || pyval.Str(eth["markPx"]) != "3100.1" {
		t.Fatalf("contexts %v %v", btc, eth)
	}
	if btc["_capture_observed_at"] != "2026-10-21T12:34:56.789012Z" || eth["_capture_observed_at"] != btc["_capture_observed_at"] {
		t.Fatalf("observation time %v %v", btc["_capture_observed_at"], eth["_capture_observed_at"])
	}
	if used := ledger.Used("hyperliquid|60.0|600.0", 60); used != 20 {
		t.Fatalf("a cache hit took budget: %v", used)
	}

	btc["markPx"] = "changed"
	again, _ := NewAdapter(hlInstrument("BTC"), c, 0).Ticker(ctx)
	if pyval.Str(again["markPx"]) != "83210.0" {
		t.Fatal("cached response mutated")
	}
	mono = mono.Add(time.Second)
	if _, err := NewAdapter(hlInstrument("BTC"), c, 0).Ticker(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ex.requests) != 2 {
		t.Fatalf("expired contexts not refetched: %d requests", len(ex.requests))
	}
}

func TestHyperliquidFundingHistoryPerSettlementHour(t *testing.T) {
	funding := `[{"coin":"BTC","fundingRate":"0.0000125","time":1792574400000}]`
	c, ex, _ := newClient(t, ok(funding), ok(funding))
	clock := now
	c.Now = func() time.Time { return clock }
	ctx := context.Background()
	payload := map[string]any{"type": "fundingHistory", "coin": "BTC", "startTime": int64(1791979200000)}
	if _, err := c.Do(ctx, "POST", Hyperliquid, nil, payload, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "POST", Hyperliquid, nil, payload, 0); err != nil {
		t.Fatal(err)
	}
	if len(ex.requests) != 1 {
		t.Fatalf("same request and hour fetched %d times", len(ex.requests))
	}
	clock = clock.Add(30 * time.Minute)
	body, err := c.Do(ctx, "POST", Hyperliquid, nil, payload, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ex.requests) != 2 {
		t.Fatalf("a new settlement hour reused the cache: %d requests", len(ex.requests))
	}
	if pyval.Map(pyval.List(body)[0])["_capture_observed_at"] != "2026-10-21T13:04:56.789012Z" {
		t.Fatalf("observation %v", body)
	}
}

func TestRetryOn429AndNeverOn418(t *testing.T) {
	limited := canned{status: 429, header: http.Header{"Retry-After": []string{"1"}}, body: `{"code":-1003}`}
	c, ex, ledger := newClient(t, limited, ok(`{"symbol":"BTCUSDT"}`))
	started := time.Now()
	body, err := c.Do(context.Background(), "GET", Binance+"/api/v3/ticker/24hr", map[string]string{"symbol": "BTCUSDT"}, nil, 0)
	if err != nil || pyval.Map(body)["symbol"] != "BTCUSDT" {
		t.Fatalf("%v %v", body, err)
	}
	if len(ex.requests) != 2 || time.Since(started) < time.Second {
		t.Fatalf("%d requests after %v", len(ex.requests), time.Since(started))
	}
	if used := ledger.Used("binance.spot|60.0|3000.0", 60); used != 4 {
		t.Fatalf("each attempt is charged: %v", used)
	}

	banned := canned{status: 418, header: http.Header{"Retry-After": []string{"600"}}, body: `{"code":-1003,"msg":"banned"}`}
	c, ex, ledger = newClient(t, banned, ok(`{}`))
	_, err = c.Do(context.Background(), "GET", BinanceUSDM+"/fapi/v1/ticker/24hr", map[string]string{"symbol": "BTCUSDT"}, nil, 0)
	var ban *budget.BannedError
	if !errors.As(err, &ban) || len(ex.requests) != 1 {
		t.Fatalf("418: %v after %d requests", err, len(ex.requests))
	}
	if until, blocked := ledger.Blocked("fapi.binance.com"); !blocked || time.Until(until) < 599*time.Second {
		t.Fatalf("ban not shared: %v %v", until, blocked)
	}
	if _, err := c.Do(context.Background(), "GET", BinanceUSDM+"/fapi/v1/ticker/24hr", map[string]string{"symbol": "BTCUSDT"}, nil, 0); !errors.As(err, &ban) {
		t.Fatalf("a banned host was asked again: %v", err)
	}
	if len(ex.requests) != 1 {
		t.Fatalf("%d requests during the ban", len(ex.requests))
	}
}

func TestRateLimitedAfterBoundedRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out four Retry-After seconds")
	}
	limited := canned{status: 429, header: http.Header{"Retry-After": []string{"0"}}}
	c, ex, _ := newClient(t, limited, limited, limited, limited, ok(`{}`))
	_, err := c.Do(context.Background(), "GET", Bybit+"/v5/market/tickers", map[string]string{"category": "spot", "symbol": "BTCUSDT"}, nil, 0)
	if err == nil || err.Error() != "api.bybit.com: rate limited after bounded retries" || len(ex.requests) != Attempts {
		t.Fatalf("%v after %d requests", err, len(ex.requests))
	}
}

func TestHTTPAndTransportErrors(t *testing.T) {
	long := strings.Repeat("é", 250)
	c, _, _ := newClient(t, canned{status: 400, body: long}, canned{err: errors.New("connection reset by peer")}, ok(`not json`))
	ctx := context.Background()
	_, err := c.Do(ctx, "GET", Kraken+"/0/public/Ticker", map[string]string{"pair": "XBTUSD"}, nil, 0)
	if err == nil || err.Error() != "api.kraken.com: HTTP 400 "+strings.Repeat("é", 200) {
		t.Fatalf("HTTP error %v", err)
	}
	_, err = c.Do(ctx, "GET", Coinbase+"/products/BTC-USD/stats", nil, nil, 0)
	if err == nil || !strings.HasPrefix(err.Error(), "api.exchange.coinbase.com: ") {
		t.Fatalf("transport error %v", err)
	}
	if _, err = c.Do(ctx, "GET", Coinbase+"/products/BTC-USD/stats", nil, nil, 0); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	reads := c.TakeReads()
	if reads["api.kraken.com"] != [2]int{0, 1} || reads["api.exchange.coinbase.com"] != [2]int{0, 2} {
		t.Fatalf("reads %v", reads)
	}
	if len(c.TakeReads()) != 0 {
		t.Fatal("reads not reset")
	}
}

func TestUsedWeightHeaderClosesTheHost(t *testing.T) {
	heavy := canned{header: http.Header{"X-Mbx-Used-Weight-1m": []string{"2400"}}, body: `{"openInterest":"1"}`}
	c, _, ledger := newClient(t, heavy)
	body, err := c.Do(context.Background(), "GET", BinanceUSDM+"/fapi/v1/openInterest", map[string]string{"symbol": "BTCUSDT"}, nil, 0)
	if err != nil || pyval.Map(body)["openInterest"] != "1" {
		t.Fatalf("the response is still used: %v %v", body, err)
	}
	if _, blocked := ledger.Blocked("fapi.binance.com"); !blocked {
		t.Fatal("the whole-IP weight at the limit did not close the host")
	}
}

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestRecentTrades(t *testing.T) {
	for _, tc := range []struct {
		inst     map[string]any
		response string
		url      string
		params   map[string]string
		count    int
		bucket   string
	}{
		{map[string]any{"venue": "binance", "market_type": "SPOT", "venue_symbol": "BTCUSDT"}, `[{"a":1,"p":"1","q":"1","T":1792578000123,"m":true}]`,
			"https://api.binance.com/api/v3/aggTrades", map[string]string{"symbol": "BTCUSDT", "limit": "1"}, 1, "binance.spot|60.0|3000.0"},
		{map[string]any{"venue": "binance", "market_type": "PERP_LINEAR", "venue_symbol": "BTCUSDT"}, `[]`,
			"https://fapi.binance.com/fapi/v1/aggTrades", map[string]string{"symbol": "BTCUSDT", "limit": "1"}, 0, "binance.usdm|60.0|1200.0"},
		{map[string]any{"venue": "okx", "market_type": "SPOT", "venue_symbol": "BTC-USDT"}, fixture(t, "okx_recent_trades.json"),
			"https://www.okx.com/api/v5/market/trades", map[string]string{"instId": "BTC-USDT", "limit": "1"}, 2, "okx:/api/v5/market/trades:IP|2.0|50.0"},
		{map[string]any{"venue": "bybit", "market_type": "PERP_LINEAR", "venue_symbol": "BTCUSDT"}, `{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"execId":"x","price":"1","size":"1","side":"Buy","time":"1792578000123","seq":"1"}]}}`,
			"https://api.bybit.com/v5/market/recent-trade", map[string]string{"category": "linear", "symbol": "BTCUSDT", "limit": "1"}, 1, "bybit|5.0|300.0"},
		{map[string]any{"venue": "coinbase", "market_type": "SPOT", "venue_symbol": "BTC-USD"}, fixture(t, "coinbase_recent_trades.json"),
			"https://api.exchange.coinbase.com/products/BTC-USD/trades", map[string]string{"limit": "1"}, 2, "coinbase|1.0|5.0"},
		{map[string]any{"venue": "kraken", "market_type": "SPOT", "venue_symbol": "XBT/USD"}, fixture(t, "kraken_recent_trades.json"),
			"https://api.kraken.com/0/public/Trades", map[string]string{"pair": "XBTUSD", "count": "1"}, 3, "kraken|1.0|1.0"},
	} {
		c, ex, ledger := newClient(t, ok(tc.response))
		rows, err := NewAdapter(venue.FromRegistry(tc.inst), c, -1).RecentTrades(context.Background(), 1)
		if err != nil || len(rows) != tc.count {
			t.Fatalf("%v: %d rows %v", tc.inst, len(rows), err)
		}
		got := ex.requests[0]
		if got.URL != tc.url || params2(got.Params) != params2(tc.params) {
			t.Fatalf("request %s %v, want %s %v", got.URL, got.Params, tc.url, tc.params)
		}
		if ledger.Used(tc.bucket, 60) == 0 {
			t.Fatalf("%s not charged", tc.bucket)
		}
	}
	c, ex, _ := newClient(t)
	_, err := NewAdapter(hlInstrument("BTC"), c, 0).RecentTrades(context.Background(), 1)
	if !errors.Is(err, ErrNoTradeHistory) || !errors.Is(err, pyval.ErrSemantic) || err.Error() != "NO_LISTED_TRADE_BACKFILL" || len(ex.requests) != 0 {
		t.Fatalf("hyperliquid: %v", err)
	}
}

func TestReprMatchesPython(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  string
	}{
		{[]any{"EGeneral:Invalid arguments"}, "['EGeneral:Invalid arguments']"},
		{[]any{"it's", `say "x"`, `both ' "`}, `["it's", 'say "x"', 'both \' "']`},
		{[]any{num(`1`), num(`1.50`), nil, true}, "[1, Decimal('1.50'), None, True]"},
		{"tab\tnew\nline\\", `'tab\tnew\nline\\'`},
	} {
		if got := repr(tc.value); got != tc.want {
			t.Errorf("repr(%v) = %s, want %s", tc.value, got, tc.want)
		}
	}
}

func TestLotSizeIsPythonStr(t *testing.T) {
	for decimals, want := range map[int64]string{0: "1", 1: "0.1", 2: "0.01", 4: "0.0001", 5: "1e-05", 8: "1e-08", -2: "100"} {
		if got := lotSize(decimals); got != want {
			t.Errorf("lotSize(%d) = %s, want %s", decimals, got, want)
		}
	}
}

func num(s string) any {
	v, _ := pyval.Decode([]byte(s))
	return v
}

func params2(m map[string]string) string {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return pyjson.Canonical(out)
}
