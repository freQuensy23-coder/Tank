// Package rest is the public venue REST surface of tank.market: VenueAdapter
// (tank.market.adapters) and MarketService._request with its Hyperliquid response cache,
// over the shared request budget (internal/budget). Responses keep Python's view of JSON:
// numbers keep their exact text (json.loads(parse_float=Decimal)).
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

// HLContexts is HL_CONTEXTS_S: one metaAndAssetCtxs response serves every Hyperliquid
// instrument for this long. A fundingHistory response is shared for an hour.
const (
	HLContexts = 10 * time.Second
	HLFunding  = time.Hour
)

const hlContextsKey = "hyperliquid:contexts"

// Attempts is the number of tries a rate-limited (429) request gets.
const Attempts = 4

type cached struct {
	at   time.Time
	body any
}

// Client sends budgeted public REST requests.
type Client struct {
	HTTP      *http.Client
	UserAgent string
	Gate      *budget.Gate
	// Now is the collection clock (iso stamps); monotonic ages the response cache.
	Now       func() time.Time
	monotonic func() time.Time

	mu    sync.Mutex
	cache map[string]cached
	locks map[string]*sync.Mutex
	reads map[string][2]int
}

// NewClient is httpx.AsyncClient(timeout=30, headers={"User-Agent": ...}) behind gate,
// with clock as the collection clock (pytime.Wall when nil).
func NewClient(userAgent string, gate *budget.Gate, clock func() time.Time) *Client {
	if clock == nil {
		clock = pytime.Wall{}.Now
	}
	return &Client{
		HTTP:      &http.Client{Timeout: 30 * time.Second},
		UserAgent: userAgent,
		Gate:      gate,
		Now:       clock,
		monotonic: time.Now,
		cache:     map[string]cached{},
		locks:     map[string]*sync.Mutex{},
		reads:     map[string][2]int{},
	}
}

// TakeReads returns and resets the REST reads per host since the last call:
// [succeeded, failed] (the service's per-minute "rest reads" log).
func (c *Client) TakeReads() map[string][2]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	reads := c.reads
	c.reads = map[string][2]int{}
	return reads
}

// Do is MarketService._request: one shared response per Hyperliquid contexts or funding
// history request (cache hits take no budget and keep the original observation time),
// otherwise a live request. params are the query parameters, body a JSON payload.
func (c *Client) Do(ctx context.Context, method, rawURL string, params map[string]string, body map[string]any, priority int) (any, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()
	key, ttl, lockKey := "", time.Duration(0), ""
	if host == "api.hyperliquid.xyz" {
		switch body["type"] {
		case "metaAndAssetCtxs":
			key, ttl, lockKey = hlContextsKey, HLContexts, hlContextsKey
		case "fundingHistory":
			period := pytime.ISO(c.Now())[:13]
			key = "hyperliquid:funding:" + pyjson.SHA256(map[string]any{"request": body, "settlement_period": period})
			ttl, lockKey = HLFunding, "hyperliquid:funding:"+pyval.Str(body["coin"])
		}
	}
	c.expire()
	if key == "" {
		return c.live(ctx, method, u, host, params, body, priority)
	}
	lock := c.lock(lockKey)
	lock.Lock()
	defer lock.Unlock()
	c.mu.Lock()
	entry, ok := c.cache[key]
	c.mu.Unlock()
	if ok && c.monotonic().Sub(entry.at) < ttl {
		return copyValue(entry.body), nil
	}
	value, err := c.live(ctx, method, u, host, params, body, priority)
	if err != nil {
		return nil, err
	}
	observed := pytime.ISO(c.Now())
	rows := value
	if body["type"] == "metaAndAssetCtxs" {
		pair := pyval.List(value)
		if len(pair) < 2 {
			return nil, fmt.Errorf("%s: metaAndAssetCtxs is not [meta, contexts]", host)
		}
		rows = pair[1]
	}
	list, isList := rows.([]any)
	if !isList {
		return nil, fmt.Errorf("%s: %s response is not a list", host, pyval.Str(body["type"]))
	}
	for _, row := range list {
		m, isMap := row.(map[string]any)
		if !isMap {
			return nil, fmt.Errorf("%s: %s row is not an object", host, pyval.Str(body["type"]))
		}
		m["_capture_observed_at"] = observed
	}
	c.mu.Lock()
	c.cache[key] = cached{c.monotonic(), value}
	c.mu.Unlock()
	return copyValue(value), nil
}

func (c *Client) lock(key string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.locks[key]
	if l == nil {
		l = &sync.Mutex{}
		c.locks[key] = l
	}
	return l
}

// expire drops cached responses past their TTL.
func (c *Client) expire() {
	now := c.monotonic()
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.cache {
		ttl := HLFunding
		if key == hlContextsKey {
			ttl = HLContexts
		}
		if now.Sub(entry.at) >= ttl {
			delete(c.cache, key)
		}
	}
}

// live is _request_live: one budgeted request, counted per host.
func (c *Client) live(ctx context.Context, method string, u *url.URL, host string, params map[string]string, body map[string]any, priority int) (any, error) {
	value, err := c.once(ctx, method, u, host, params, body, priority)
	if ctx.Err() == nil || err == nil {
		c.mu.Lock()
		counts := c.reads[host]
		if err != nil {
			counts[1]++
		} else {
			counts[0]++
		}
		c.reads[host] = counts
		c.mu.Unlock()
	}
	return value, err
}

// once is _request_once: the budget first (without a host slot), then the host slot and
// its start spacing; a 429 is retried after its Retry-After, a 418 never.
func (c *Client) once(ctx context.Context, method string, u *url.URL, host string, params map[string]string, body map[string]any, priority int) (any, error) {
	for attempt := 0; attempt < Attempts; attempt++ {
		if err := c.Gate.Admit(ctx, host, u.Path, params, body, priority); err != nil {
			return nil, err
		}
		release, err := c.Gate.Enter(ctx, host, priority)
		if err != nil {
			return nil, err
		}
		status, header, data, err := c.send(ctx, method, u, params, body)
		if err != nil {
			release()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%s: %s", host, errorName(err))
		}
		retry, err := c.Gate.Observe(ctx, host, status, header)
		release()
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		if status >= 400 {
			// The venue's error code and message are the gap evidence (bodies carry no credentials).
			return nil, fmt.Errorf("%s: HTTP %d %s", host, status, prefix(string(data), 200))
		}
		value, err := pyval.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid JSON response: %v", host, err)
		}
		return value, nil
	}
	return nil, fmt.Errorf("%s: rate limited after bounded retries", host)
}

func (c *Client) send(ctx context.Context, method string, u *url.URL, params map[string]string, body map[string]any) (int, http.Header, []byte, error) {
	target := *u
	if len(params) > 0 {
		query := target.Query()
		for k, v := range params {
			query.Set(k, v)
		}
		target.RawQuery = query.Encode()
	}
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), payload)
	if err != nil {
		return 0, nil, nil, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, data, nil
}

// errorName is the httpx exception name a transport failure would carry.
func errorName(err error) string {
	var op *net.OpError
	dial := errors.As(err, &op) && op.Op == "dial"
	var timeout interface{ Timeout() bool }
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		if dial {
			return "ConnectTimeout"
		}
		return "ReadTimeout"
	}
	if dial {
		return "ConnectError"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return "RemoteProtocolError"
	}
	return "ReadError"
}

// prefix is text[:n] in characters.
func prefix(text string, n int) string {
	runes := []rune(text)
	if len(runes) > n {
		runes = runes[:n]
	}
	return string(runes)
}

// copyValue copies decoded JSON, so a shared cached response is never mutated.
func copyValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			out[k] = copyValue(item)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = copyValue(item)
		}
		return out
	}
	return v
}

// sortedKeys are m's keys in order.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// FetchLimit caps a raw fetch's response body (HTTP_FETCH capture requests).
const FetchLimit = 32 << 20

// ErrTooLarge is a raw fetch whose body exceeds FetchLimit.
var ErrTooLarge = errors.New("RESPONSE_TOO_LARGE")

// Fetched is one raw HTTP response.
type Fetched struct {
	Status      int
	ContentType string
	Body        []byte
}

// Fetch is one raw budgeted request for a capture request: the venue budget
// (budget.FetchConstraints), the host slot and spacing, 429 retried after Retry-After and
// 418 never, like Do; the body is returned whatever its status, at most FetchLimit bytes.
func (c *Client) Fetch(ctx context.Context, method, rawURL string, params, headers map[string]string, body any, priority int) (*Fetched, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()
	var payload map[string]any
	if m, ok := body.(map[string]any); ok {
		payload = m
	}
	for attempt := 0; attempt < Attempts; attempt++ {
		if err := c.Gate.AdmitBuckets(ctx, host, budget.FetchConstraints(host, u.Path, params, payload), priority); err != nil {
			return nil, err
		}
		release, err := c.Gate.Enter(ctx, host, priority)
		if err != nil {
			return nil, err
		}
		got, err := c.raw(ctx, method, u, params, headers, body)
		if err != nil {
			release()
			c.count(host, false)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, ErrTooLarge) {
				return nil, err
			}
			return nil, fmt.Errorf("%s: %s", host, errorName(err))
		}
		retry, err := c.Gate.Observe(ctx, host, got.Status, got.header)
		release()
		if err != nil {
			c.count(host, false)
			return nil, err
		}
		if retry {
			continue
		}
		c.count(host, got.Status < 400)
		return &got.Fetched, nil
	}
	return nil, fmt.Errorf("%s: rate limited after bounded retries", host)
}

type rawResponse struct {
	Fetched
	header http.Header
}

func (c *Client) raw(ctx context.Context, method string, u *url.URL, params, headers map[string]string, body any) (*rawResponse, error) {
	target := *u
	if len(params) > 0 {
		query := target.Query()
		for k, v := range params {
			query.Set(k, v)
		}
		target.RawQuery = query.Encode()
	}
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), payload)
	if err != nil {
		return nil, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, FetchLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > FetchLimit {
		return nil, ErrTooLarge
	}
	return &rawResponse{Fetched{resp.StatusCode, resp.Header.Get("Content-Type"), data}, resp.Header}, nil
}

func (c *Client) count(host string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := c.reads[host]
	if ok {
		counts[0]++
	} else {
		counts[1]++
	}
	c.reads[host] = counts
}
