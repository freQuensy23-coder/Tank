// Package restparity runs a recorded REST poll cycle (capture/testdata/rest) through
// tank-capture's runtime exactly as scripts/capture/rest_parity.py runs it through
// MarketService.poll_once: the same responses must give identical typed rows (klines,
// tickers, OI, funding, marks), fingerprints and REST-normalized trades.
package restparity

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/capture"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/parity"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// Exchange is one recorded request and its response.
type Exchange struct {
	Method string            `json:"method"`
	URL    string            `json:"url"`
	Params map[string]string `json:"params"`
	JSON   map[string]any    `json:"json"`
	Status int               `json:"status"`
	Body   string            `json:"body"`
}

// Payload is a recorded fixture.
type Payload struct {
	RecordedAt  string     `json:"recorded_at"`
	Fixture     string     `json:"fixture"`
	Instruments []any      `json:"instruments"`
	Exchanges   []Exchange `json:"exchanges"`
}

// Load reads capture/testdata/rest/<fixture>.json.gz.
func Load(path string) (*Payload, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	var p Payload
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// timeParams vary with the clock a request is built at; they never select a response.
var timeParams = map[string]bool{"startTime": true, "endTime": true, "start": true, "end": true, "since": true, "after": true, "before": true}

// Match is the selector of a request: method, URL without query, the non-time query
// parameters and the Hyperliquid request type and coin.
func Match(method, url string, params map[string]string, body map[string]any) string {
	var keys []string
	for k := range params {
		if !timeParams[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := []string{strings.ToUpper(method), url}
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}
	if body != nil {
		parts = append(parts, fmt.Sprint(body["type"]), fmt.Sprint(body["coin"]))
		if req, ok := body["req"].(map[string]any); ok {
			parts = append(parts, fmt.Sprint(req["coin"]))
		}
	}
	return strings.Join(parts, " ")
}

// transport answers requests with the recorded responses, in recorded order per selector.
type transport struct {
	mu     sync.Mutex
	byKey  map[string][]Exchange
	served map[string]int
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	params := map[string]string{}
	for k, v := range req.URL.Query() {
		params[k] = v[0]
	}
	var body map[string]any
	if req.Body != nil {
		data, _ := io.ReadAll(req.Body)
		if len(data) > 0 {
			_ = json.Unmarshal(data, &body)
		}
	}
	u := *req.URL
	u.RawQuery = ""
	key := Match(req.Method, u.String(), params, body)
	t.mu.Lock()
	options := t.byKey[key]
	n := t.served[key]
	t.served[key] = n + 1
	t.mu.Unlock()
	if len(options) == 0 {
		return nil, fmt.Errorf("no recorded response for %s", key)
	}
	ex := options[min(n, len(options)-1)]
	return &http.Response{
		StatusCode: ex.Status, Body: io.NopCloser(strings.NewReader(ex.Body)), Header: http.Header{},
		Request: req,
	}, nil
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time           { return c.t }
func (c fixedClock) Monotonic() time.Duration { return 0 }

type sink struct {
	mu   sync.Mutex
	rows []parity.RowOut
}

func (s *sink) Sink(r points.Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, parity.RowOut{Collection: r.Collection, Key: r.Key, Row: r.Record})
}

func (s *sink) Flush(context.Context) error { return nil }

type noStore struct{}

func (noStore) Get(context.Context, string, string) (map[string]any, error) { return nil, nil }
func (noStore) Range(context.Context, string, string, time.Time, time.Time, map[string]any) ([]map[string]any, error) {
	return nil, nil
}
func (noStore) Projection(context.Context, string, []string, string, time.Time, time.Time, []string) ([]map[string]any, error) {
	return nil, nil
}
func (noStore) LatestTrades(context.Context, []string, time.Time) (map[string]map[string]any, error) {
	return nil, nil
}
func (noStore) LatestRecords(context.Context, string, []string, string, []string) ([]map[string]any, error) {
	return nil, nil
}
func (noStore) HotTokens(context.Context) ([]map[string]any, error)              { return nil, nil }
func (noStore) BackfillRuns(context.Context, []string) ([]map[string]any, error) { return nil, nil }
func (noStore) ClaimRequest(context.Context, string, time.Time) (map[string]any, error) {
	return nil, nil
}
func (noStore) FinishRequest(context.Context, map[string]any) error { return nil }

// Run polls every fixture instrument once and normalizes one trades page of each.
func Run(p *Payload) (*parity.Output, error) {
	meta := map[string]any{"instruments": p.Instruments, "recorded_at": p.RecordedAt}
	instruments := parity.Instruments(meta)
	if len(instruments) == 0 {
		return nil, errors.New("fixture without instruments")
	}
	now := pytime.MustParse(p.RecordedAt)
	clock := fixedClock{now}
	tr := &transport{byKey: map[string][]Exchange{}, served: map[string]int{}}
	for _, ex := range p.Exchanges {
		key := Match(ex.Method, ex.URL, ex.Params, ex.JSON)
		tr.byKey[key] = append(tr.byKey[key], ex)
	}
	client := rest.NewClient("TankV0/1.0 (parity)", budget.NewGate(budget.NewMemoryLedger(time.Now), "parity"), clock.Now)
	client.HTTP = &http.Client{Transport: tr}
	venues := capture.RESTVenues(client)
	out := &parity.Output{Fingerprints: map[string]string{}, Watermarks: map[string]any{}, Trades: []map[string]any{}, Rows: []parity.RowOut{}, Bars: []map[string]any{}, Errors: [][]any{}}
	rows := &sink{}
	byID := map[string]*venue.Instrument{}
	var order []string
	for _, inst := range instruments {
		byID[inst.ID] = inst
		order = append(order, inst.ID)
	}
	svc := capture.New(capture.Options{
		Instruments: byID, Order: order, ConfigVersion: parity.ConfigVersion, CodeVersion: parity.CodeVersion,
	}, noStore{}, rows, venues, nil, clock)
	ctx := context.Background()
	for _, inst := range instruments {
		svc.PollOnce(ctx, inst.ID)
		page, _, err := venues.Venue(inst, 0).Trades(ctx, nil, nil, nil)
		if err != nil {
			continue // Hyperliquid lists no REST trades
		}
		fx := normalize.NewFX()
		for _, raw := range page {
			t, err := normalize.NormalizeTrade(inst, raw, now, "REST", fx)
			if err != nil {
				return nil, fmt.Errorf("%s trade: %w", inst.ID, err)
			}
			out.Trades = append(out.Trades, t.Record())
		}
	}
	for _, r := range rows.rows {
		if r.Collection == "mkt.capture_gaps" {
			// A gap's id is random; its kind and reason are the comparable part.
			out.Errors = append(out.Errors, []any{r.Row["instrument_id"], r.Row["kind"], r.Row["reason"]})
			continue
		}
		out.Rows = append(out.Rows, r)
		encoded, err := schema.Encode(r.Collection, r.Row)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", r.Collection, r.Key, err)
		}
		out.Fingerprints[r.Collection+"|"+r.Key] = schema.Fingerprint(encoded, r.Collection)
	}
	return out, nil
}
