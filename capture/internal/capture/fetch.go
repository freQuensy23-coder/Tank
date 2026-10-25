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
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
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

type FetchResult = requeststate.FetchResult

func decodeFetchRequest(data json.RawMessage) (FetchRequest, error) {
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

func (s *Service) runFetch(ctx context.Context, raw json.RawMessage, budgetS time.Duration) requeststate.Outcome {
	req, err := decodeFetchRequest(raw)
	if err != nil {
		return requeststate.Failure(requeststate.FetchRequestInvalid, requeststate.FetchFailure{Error: err.Error()})
	}
	u, err := url.Parse(req.URL)
	if err != nil || u.Scheme != "https" || !FetchHosts[u.Hostname()] {
		return requeststate.Failure(requeststate.FetchHostNotAllowed, requeststate.RejectedURL{URL: req.URL})
	}
	if req.Method != "GET" && req.Method != "POST" {
		return requeststate.Failure(requeststate.FetchMethodNotAllowed, requeststate.RejectedMethod{Method: req.Method})
	}
	if s.opt.Fetcher == nil {
		return requeststate.Failure(requeststate.FetchUnavailable, nil)
	}
	if budgetS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budgetS)
		defer cancel()
	}
	got, err := s.opt.Fetcher.Fetch(ctx, req.Method, req.URL, req.Params, req.Headers, req.Body, fetchPriority)
	if err != nil {
		reason := requeststate.FetchFailed
		var banned *budget.BannedError
		switch {
		case errors.Is(err, rest.ErrTooLarge):
			reason = requeststate.ResponseTooLarge
		case errors.As(err, &banned):
			reason = requeststate.HostBanned
		case errors.Is(err, context.DeadlineExceeded):
			reason = requeststate.BudgetExhausted
		}
		return requeststate.Failure(reason, requeststate.FetchFailure{Error: err.Error(), SourceID: req.SourceID})
	}
	if got == nil {
		return requeststate.Failure(requeststate.FetchFailed, requeststate.FetchFailure{Error: "fetcher returned no response", SourceID: req.SourceID})
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
	return requeststate.Completed(result)
}
