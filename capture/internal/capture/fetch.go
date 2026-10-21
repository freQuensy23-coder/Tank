package capture

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

var FetchHosts = map[string]bool{
	"api.binance.com": true, "fapi.binance.com": true, "www.binance.com": true, "data.binance.vision": true,
	"api.exchange.coinbase.com": true, "api.kraken.com": true, "www.okx.com": true, "api.bybit.com": true,
	"api.hyperliquid.xyz": true,
}

type Fetcher interface {
	Fetch(ctx context.Context, method, rawURL string, params, headers map[string]string, body any, priority int) (*rest.Fetched, error)
	Download(ctx context.Context, rawURL string, dst io.Writer, priority int) (int, int64, error)
}

const fetchPriority = -1

func (s *Service) runFetch(ctx context.Context, row map[string]any, budgetS time.Duration, finish func(string, any, any)) {
	req := pyval.Map(row["request"])
	method := strings.ToUpper(text(req["method"]))
	if method == "" {
		method = "GET"
	}
	raw := text(req["url"])
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || !FetchHosts[u.Hostname()] {
		finish("FAILED", "FETCH_HOST_NOT_ALLOWED", map[string]any{"url": raw})
		return
	}
	if method != "GET" && method != "POST" {
		finish("FAILED", "FETCH_METHOD_NOT_ALLOWED", map[string]any{"method": method})
		return
	}
	if s.opt.Fetcher == nil {
		finish("FAILED", "FETCH_UNAVAILABLE", nil)
		return
	}
	params := map[string]string{}
	for k, v := range pyval.Map(req["params"]) {
		params[k] = text(v)
	}
	headers := map[string]string{}
	for k, v := range pyval.Map(req["headers"]) {
		headers[k] = text(v)
	}
	if budgetS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budgetS)
		defer cancel()
	}
	got, err := s.opt.Fetcher.Fetch(ctx, method, raw, params, headers, req["body"], fetchPriority)
	if err != nil {
		reason := "FETCH_FAILED"
		var banned *budget.BannedError
		switch {
		case errors.Is(err, rest.ErrTooLarge):
			reason = "RESPONSE_TOO_LARGE"
		case errors.As(err, &banned):
			reason = "HOST_BANNED"
		case errors.Is(err, context.DeadlineExceeded):
			reason = "REQUEST_BUDGET_EXHAUSTED"
		}
		finish("FAILED", reason, map[string]any{"error": err.Error(), "source_id": req["source_id"]})
		return
	}
	sum := sha256.Sum256(got.Body)
	result := map[string]any{
		"status_code": int64(got.Status), "content_type": got.ContentType, "sha256": hex.EncodeToString(sum[:]),
		"fetched_at": pytime.ISO(s.clock.Now()), "bytes": int64(len(got.Body)), "source_id": req["source_id"],
	}
	if utf8.Valid(got.Body) {
		result["body"] = string(got.Body)
	} else {
		result["body_b64"] = base64.StdEncoding.EncodeToString(got.Body)
	}
	finish("COMPLETE", nil, result)
}
