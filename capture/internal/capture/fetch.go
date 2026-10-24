package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

var FetchHosts = map[string]bool{
	"api.binance.com": true, "fapi.binance.com": true, "www.binance.com": true, "data.binance.vision": true,
	"api.exchange.coinbase.com": true, "api.kraken.com": true, "www.okx.com": true, "api.bybit.com": true,
	"api.hyperliquid.xyz": true, "openapi.okx.com": true,
}

type FetchRequest struct {
	Method   string
	URL      string
	SourceID *string
	Params   map[string]string
	Headers  map[string]string
	Body     json.RawMessage
}

type fetchWireRequest struct {
	Method   string            `json:"method"`
	URL      string            `json:"url"`
	SourceID json.RawMessage   `json:"source_id"`
	Params   map[string]string `json:"params"`
	Headers  map[string]string `json:"headers"`
	Body     json.RawMessage   `json:"body"`
}

type FetchResult struct {
	StatusCode  int     `json:"status_code"`
	ContentType string  `json:"content_type"`
	SHA256      string  `json:"sha256"`
	FetchedAt   string  `json:"fetched_at"`
	Bytes       int64   `json:"bytes"`
	SourceID    *string `json:"source_id"`
	Body        *string `json:"body,omitempty"`
	BodyB64     *string `json:"body_b64,omitempty"`
}

func (r FetchResult) storageValue() map[string]any {
	var sourceID any
	if r.SourceID != nil {
		sourceID = *r.SourceID
	}
	value := map[string]any{
		"status_code": int64(r.StatusCode), "content_type": r.ContentType,
		"sha256": r.SHA256, "fetched_at": r.FetchedAt, "bytes": r.Bytes,
		"source_id": sourceID,
	}
	if r.Body != nil {
		value["body"] = *r.Body
	}
	if r.BodyB64 != nil {
		value["body_b64"] = *r.BodyB64
	}
	return value
}

func fetchFailure(message string, source *string) map[string]any {
	var sourceID any
	if source != nil {
		sourceID = *source
	}
	return map[string]any{"error": message, "source_id": sourceID}
}

func decodeFetchRequest(value any) (FetchRequest, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return FetchRequest{}, err
	}
	var wire fetchWireRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return FetchRequest{}, err
	}
	if wire.Method == "" || wire.URL == "" || len(wire.SourceID) == 0 {
		return FetchRequest{}, errors.New("fetch request requires method, url and source_id")
	}
	var sourceID *string
	if err := json.Unmarshal(wire.SourceID, &sourceID); err != nil {
		return FetchRequest{}, fmt.Errorf("invalid fetch source_id: %w", err)
	}
	return FetchRequest{
		Method: wire.Method, URL: wire.URL, SourceID: sourceID,
		Params: wire.Params, Headers: wire.Headers, Body: wire.Body,
	}, nil
}

type Fetcher interface {
	Fetch(ctx context.Context, method, rawURL string, params, headers map[string]string, body json.RawMessage, priority int) (*rest.Fetched, error)
}

const fetchPriority = -1

func (s *Service) runFetch(ctx context.Context, row map[string]any, budgetS time.Duration, finish func(string, any, any)) {
	req, err := decodeFetchRequest(row["request"])
	if err != nil {
		finish("FAILED", "FETCH_REQUEST_INVALID", fetchFailure(err.Error(), nil))
		return
	}
	u, err := url.Parse(req.URL)
	if err != nil || u.Scheme != "https" || !FetchHosts[u.Hostname()] {
		finish("FAILED", "FETCH_HOST_NOT_ALLOWED", map[string]any{"url": req.URL})
		return
	}
	if req.Method != "GET" && req.Method != "POST" {
		finish("FAILED", "FETCH_METHOD_NOT_ALLOWED", map[string]any{"method": req.Method})
		return
	}
	if s.opt.Fetcher == nil {
		finish("FAILED", "FETCH_UNAVAILABLE", nil)
		return
	}
	if budgetS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budgetS)
		defer cancel()
	}
	got, err := s.opt.Fetcher.Fetch(ctx, req.Method, req.URL, req.Params, req.Headers, req.Body, fetchPriority)
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
		finish("FAILED", reason, fetchFailure(err.Error(), req.SourceID))
		return
	}
	if got == nil {
		finish("FAILED", "FETCH_FAILED", fetchFailure("fetcher returned no response", req.SourceID))
		return
	}
	sum := sha256.Sum256(got.Body)
	result := FetchResult{
		StatusCode: got.Status, ContentType: got.ContentType, SHA256: hex.EncodeToString(sum[:]),
		FetchedAt: pytime.ISO(s.clock.Now()), Bytes: int64(len(got.Body)), SourceID: req.SourceID,
	}
	if utf8.Valid(got.Body) {
		body := string(got.Body)
		result.Body = &body
	} else {
		body := base64.StdEncoding.EncodeToString(got.Body)
		result.BodyB64 = &body
	}
	finish("COMPLETE", nil, result.storageValue())
}
