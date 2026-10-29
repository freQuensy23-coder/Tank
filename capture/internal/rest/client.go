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

const (
	HLContexts = 10 * time.Second
	HLFunding  = time.Hour
)

const hlContextsKey = "hyperliquid:contexts"

const Attempts = 4

type cached struct {
	at   time.Time
	body any
}

type Client struct {
	HTTP      *http.Client
	UserAgent string
	Gate      *budget.Gate

	Now       func() time.Time
	monotonic func() time.Time

	mu    sync.Mutex
	cache map[string]cached
	locks map[string]*sync.Mutex
	reads map[string][2]int
}

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

func (c *Client) TakeReads() map[string][2]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	reads := c.reads
	c.reads = map[string][2]int{}
	return reads
}

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
	defer func() {
		_ = resp.Body.Close()
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, data, nil
}

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

func prefix(text string, n int) string {
	runes := []rune(text)
	if len(runes) > n {
		runes = runes[:n]
	}
	return string(runes)
}

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

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

const FetchLimit = 32 << 20

var ErrTooLarge = errors.New("RESPONSE_TOO_LARGE")

type Fetched struct {
	Status      int
	ContentType string
	Location    string
	Body        []byte
}

type manualRedirectsKey struct{}

func WithManualRedirects(ctx context.Context) context.Context {
	return context.WithValue(ctx, manualRedirectsKey{}, true)
}

func ManualRedirects(ctx context.Context) bool {
	manual, _ := ctx.Value(manualRedirectsKey{}).(bool)
	return manual
}

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
	var limited *RateLimitedError
	for attempt := 0; attempt < Attempts; attempt++ {
		if err := c.Gate.AdmitBuckets(ctx, host, budget.FetchConstraints(host, u.Path, params, payload), priority); err != nil {
			return nil, limited.or(err)
		}
		release, err := c.Gate.Enter(ctx, host, priority)
		if err != nil {
			return nil, limited.or(err)
		}
		got, err := c.raw(ctx, method, u, params, headers, body)
		if err != nil {
			release()
			c.count(host, false)
			if ctx.Err() != nil {
				return nil, limited.or(ctx.Err())
			}
			if errors.Is(err, ErrTooLarge) {
				return nil, err
			}
			return nil, fmt.Errorf("%s: %s", host, errorName(err))
		}
		if got.Status == http.StatusTooManyRequests {
			limited = &RateLimitedError{Host: host, RetryAfter: got.header.Get("Retry-After")}
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
	limited.Cause = fmt.Errorf("%s: rate limited after bounded retries", host)
	return nil, limited
}

type RateLimitedError struct {
	Host       string
	RetryAfter string
	Cause      error
}

func (e *RateLimitedError) Error() string { return e.Cause.Error() }

func (e *RateLimitedError) Unwrap() error { return e.Cause }

func (e *RateLimitedError) or(err error) error {
	if e == nil {
		return err
	}
	e.Cause = err
	return e
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
	client := c.HTTP
	if ManualRedirects(ctx) {
		manual := *c.HTTP
		manual.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &manual
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	data, err := io.ReadAll(io.LimitReader(resp.Body, FetchLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > FetchLimit {
		return nil, ErrTooLarge
	}
	fetched := Fetched{resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Location"), data}
	return &rawResponse{fetched, resp.Header}, nil
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
