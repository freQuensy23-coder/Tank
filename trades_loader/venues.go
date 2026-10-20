package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Cursor is the per-instrument position the loader resumes from (kept in WATERMARK.json).
type Cursor struct {
	LastID   int64 // last trade ID written (all venues here have integer IDs)
	LastTsMs int64
	Since    string // Kraken's opaque `last` cursor
}

// Batch is what one fetch returns: trades after the cursor, ascending.
type Batch struct {
	Trades []Trade
	Head   bool // the venue had nothing newer at request time
	At     time.Time
	Gap    *Gap // IDs missing between the cursor and this batch that REST cannot return
}

type Venue interface {
	// Start finds the first trade at or after since. It may return trades already.
	Start(ctx context.Context, in Instrument, since time.Time) (Cursor, []Trade, error)
	Next(ctx context.Context, in Instrument, c Cursor) (Cursor, Batch, error)
}

type httpClient struct {
	c   *http.Client
	lim *Limiter
	ua  string
}

var errStatus = errors.New("http status")

func (h *httpClient) get(ctx context.Context, u string, out any) error {
	for attempt := 0; ; attempt++ {
		if err := h.lim.Wait(ctx); err != nil {
			return err
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		req.Header.Set("User-Agent", h.ua)
		req.Header.Set("Accept", "application/json")
		resp, err := h.c.Do(req)
		if err == nil {
			body, rerr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
			resp.Body.Close()
			switch {
			case rerr != nil:
				err = rerr
			case resp.StatusCode == 429 || resp.StatusCode == 418:
				wait := 30 * time.Second
				if s, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && s > 0 {
					wait = time.Duration(s) * time.Second
				}
				h.lim.Backoff(wait)
				err = fmt.Errorf("%w %d", errStatus, resp.StatusCode)
			case resp.StatusCode >= 500:
				err = fmt.Errorf("%w %d", errStatus, resp.StatusCode)
			case resp.StatusCode != 200:
				return fmt.Errorf("%w %d: %.200s", errStatus, resp.StatusCode, body)
			default:
				return json.Unmarshal(body, out)
			}
		}
		if attempt >= 4 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
	}
}

// ---------------------------------------------------------------- Binance (aggTrades)

type binance struct {
	h    *httpClient
	base string // https://api.binance.com/api/v3 or https://fapi.binance.com/fapi/v1
}

func (b *binance) page(ctx context.Context, q url.Values) ([]json.RawMessage, error) {
	var rows []json.RawMessage
	err := b.h.get(ctx, b.base+"/aggTrades?"+q.Encode(), &rows)
	return rows, err
}

func binanceRow(raw json.RawMessage) (int64, int64, error) {
	var r struct {
		A int64 `json:"a"`
		T int64 `json:"T"`
	}
	err := json.Unmarshal(raw, &r)
	return r.A, r.T, err
}

func (b *binance) Start(ctx context.Context, in Instrument, since time.Time) (Cursor, []Trade, error) {
	for s := since; s.Before(time.Now()); s = s.Add(time.Hour) {
		rows, err := b.page(ctx, url.Values{"symbol": {in.Symbol}, "startTime": {ms(s)}, "endTime": {ms(s.Add(time.Hour - time.Millisecond))}, "limit": {"1"}})
		if err != nil {
			return Cursor{}, nil, err
		}
		if len(rows) == 1 {
			id, ts, err := binanceRow(rows[0])
			return Cursor{LastID: id - 1, LastTsMs: ts - 1}, nil, err
		}
	}
	return Cursor{}, nil, fmt.Errorf("%s: no trades since %s", in.ID, since)
}

func (b *binance) Next(ctx context.Context, in Instrument, c Cursor) (Cursor, Batch, error) {
	at := time.Now()
	rows, err := b.page(ctx, url.Values{"symbol": {in.Symbol}, "fromId": {strconv.FormatInt(c.LastID+1, 10)}, "limit": {"1000"}})
	if err != nil {
		return c, Batch{}, err
	}
	batch := Batch{Head: len(rows) < 1000, At: at}
	for _, raw := range rows {
		id, ts, err := binanceRow(raw)
		if err != nil {
			return c, Batch{}, err
		}
		if id != c.LastID+1 && batch.Gap == nil { // fromId paging is contiguous; record anything else
			batch.Gap = &Gap{Start: time.UnixMilli(c.LastTsMs).UTC(), End: time.UnixMilli(ts).UTC(), Reason: "BINANCE_AGGTRADE_ID_GAP"}
		}
		batch.Trades = append(batch.Trades, Trade{TsMs: ts, ID: strconv.FormatInt(id, 10), Raw: raw})
		c.LastID, c.LastTsMs = id, ts
	}
	return c, batch, nil
}

// ---------------------------------------------------------------- Coinbase Exchange

type coinbase struct{ h *httpClient }

type cbRow struct {
	ID   int64  `json:"trade_id"`
	Time string `json:"time"`
}

func (cb *coinbase) page(ctx context.Context, product string, after int64, limit int) ([]json.RawMessage, []cbRow, error) {
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if after > 0 {
		q.Set("after", strconv.FormatInt(after, 10)) // trades with trade_id < after, newest first
	}
	var raws []json.RawMessage
	if err := cb.h.get(ctx, "https://api.exchange.coinbase.com/products/"+product+"/trades?"+q.Encode(), &raws); err != nil {
		return nil, nil, err
	}
	rows := make([]cbRow, len(raws))
	for i, r := range raws {
		if err := json.Unmarshal(r, &rows[i]); err != nil {
			return nil, nil, err
		}
	}
	return raws, rows, nil
}

func cbMs(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	return t.UnixMilli(), err
}

func (cb *coinbase) Start(ctx context.Context, in Instrument, since time.Time) (Cursor, []Trade, error) {
	_, latest, err := cb.page(ctx, in.Symbol, 0, 1)
	if err != nil || len(latest) == 0 {
		return Cursor{}, nil, fmt.Errorf("%s: latest trade: %v", in.ID, err)
	}
	hi := latest[0].ID // known: time >= since (or the newest)
	lo := hi - 4_000_000
	if lo < 1 {
		lo = 1
	}
	at := func(id int64) (int64, error) { // time of trade `id`
		_, r, err := cb.page(ctx, in.Symbol, id+1, 1)
		if err != nil || len(r) == 0 {
			return 0, err
		}
		return cbMs(r[0].Time)
	}
	target := since.UnixMilli()
	if t, err := at(lo); err != nil {
		return Cursor{}, nil, err
	} else if t >= target {
		return Cursor{LastID: lo - 1, LastTsMs: t - 1}, nil, nil
	}
	for hi-lo > 1 { // invariant: time(lo) < target <= time(hi)
		mid := (lo + hi) / 2
		t, err := at(mid)
		if err != nil {
			return Cursor{}, nil, err
		}
		if t < target {
			lo = mid
		} else {
			hi = mid
		}
	}
	t, err := at(lo)
	return Cursor{LastID: lo, LastTsMs: t}, nil, err
}

func (cb *coinbase) Next(ctx context.Context, in Instrument, c Cursor) (Cursor, Batch, error) {
	at := time.Now()
	raws, rows, err := cb.page(ctx, in.Symbol, c.LastID+1001, 1000) // IDs LastID+1 .. LastID+1000
	if err != nil {
		return c, Batch{}, err
	}
	idx := make([]int, 0, len(rows))
	for i, r := range rows {
		if r.ID > c.LastID {
			idx = append(idx, i)
		}
	}
	sort.Slice(idx, func(a, b int) bool { return rows[idx[a]].ID < rows[idx[b]].ID })
	batch := Batch{Head: len(idx) < 1000, At: at}
	for _, i := range idx {
		ts, err := cbMs(rows[i].Time)
		if err != nil {
			return c, Batch{}, err
		}
		if rows[i].ID != c.LastID+1 && batch.Gap == nil {
			batch.Gap = &Gap{Start: time.UnixMilli(c.LastTsMs).UTC(), End: time.UnixMilli(ts).UTC(), Reason: "COINBASE_TRADE_ID_GAP"}
		}
		batch.Trades = append(batch.Trades, Trade{TsMs: ts, ID: strconv.FormatInt(rows[i].ID, 10), Raw: raws[i]})
		c.LastID, c.LastTsMs = rows[i].ID, ts
	}
	return c, batch, nil
}

// ---------------------------------------------------------------- OKX
// Trade IDs increase but are not consecutive, so completeness comes from overlap: a
// fetch is complete when it reaches back to a trade at or before the cursor.

type okx struct{ h *httpClient }

type okxResp struct {
	Code string            `json:"code"`
	Msg  string            `json:"msg"`
	Data []json.RawMessage `json:"data"`
}

type okxRow struct {
	ID string `json:"tradeId"`
	Ts string `json:"ts"`
}

func (o *okx) get(ctx context.Context, path string, q url.Values) ([]json.RawMessage, []okxRow, error) {
	var r okxResp
	if err := o.h.get(ctx, "https://www.okx.com"+path+"?"+q.Encode(), &r); err != nil {
		return nil, nil, err
	}
	if r.Code != "0" {
		return nil, nil, fmt.Errorf("okx %s: %s %s", path, r.Code, r.Msg)
	}
	rows := make([]okxRow, len(r.Data))
	for i, d := range r.Data {
		if err := json.Unmarshal(d, &rows[i]); err != nil {
			return nil, nil, err
		}
	}
	return r.Data, rows, nil
}

// back pages older trades (history-trades by tradeId) from `before` until stop says so.
func (o *okx) back(ctx context.Context, in Instrument, before string, stop func(id, ts int64) bool) ([]Trade, error) {
	var out []Trade
	cur := before
	for {
		raws, rows, err := o.get(ctx, "/api/v5/market/history-trades", url.Values{"instId": {in.Symbol}, "type": {"1"}, "after": {cur}, "limit": {"100"}})
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return out, nil
		}
		for i, r := range rows {
			id, _ := strconv.ParseInt(r.ID, 10, 64)
			ts, _ := strconv.ParseInt(r.Ts, 10, 64)
			if stop(id, ts) {
				return out, nil
			}
			out = append(out, Trade{TsMs: ts, ID: r.ID, Raw: raws[i]})
		}
		cur = rows[len(rows)-1].ID
	}
}

func (o *okx) Start(ctx context.Context, in Instrument, since time.Time) (Cursor, []Trade, error) {
	raws, rows, err := o.get(ctx, "/api/v5/market/trades", url.Values{"instId": {in.Symbol}, "limit": {"1"}})
	if err != nil || len(rows) == 0 {
		return Cursor{}, nil, fmt.Errorf("%s: latest trade: %v", in.ID, err)
	}
	target := since.UnixMilli()
	head := Trade{ID: rows[0].ID, Raw: raws[0]}
	head.TsMs, _ = strconv.ParseInt(rows[0].Ts, 10, 64)
	older, err := o.back(ctx, in, rows[0].ID, func(_, ts int64) bool { return ts < target })
	if err != nil {
		return Cursor{}, nil, err
	}
	trades := append(older, head)
	id, _ := strconv.ParseInt(head.ID, 10, 64)
	return Cursor{LastID: id, LastTsMs: head.TsMs}, trades, nil
}

func (o *okx) Next(ctx context.Context, in Instrument, c Cursor) (Cursor, Batch, error) {
	at := time.Now()
	raws, rows, err := o.get(ctx, "/api/v5/market/trades", url.Values{"instId": {in.Symbol}, "limit": {"500"}})
	if err != nil {
		return c, Batch{}, err
	}
	var fresh []Trade
	reached := false
	oldest := ""
	for i, r := range rows { // newest first
		id, _ := strconv.ParseInt(r.ID, 10, 64)
		ts, _ := strconv.ParseInt(r.Ts, 10, 64)
		if id <= c.LastID {
			reached = true
			break
		}
		fresh = append(fresh, Trade{TsMs: ts, ID: r.ID, Raw: raws[i]})
		oldest = r.ID
	}
	if !reached && oldest != "" { // more than one page since the cursor: page back to it
		older, err := o.back(ctx, in, oldest, func(id, _ int64) bool { return id <= c.LastID })
		if err != nil {
			return c, Batch{}, err
		}
		fresh = append(fresh, older...)
	}
	sort.Slice(fresh, func(a, b int) bool {
		ia, _ := strconv.ParseInt(fresh[a].ID, 10, 64)
		ib, _ := strconv.ParseInt(fresh[b].ID, 10, 64)
		return ia < ib
	})
	for _, t := range fresh {
		c.LastID, _ = strconv.ParseInt(t.ID, 10, 64)
		if t.TsMs > c.LastTsMs {
			c.LastTsMs = t.TsMs
		}
	}
	return c, Batch{Trades: fresh, Head: true, At: at}, nil
}

// ---------------------------------------------------------------- Kraken

type kraken struct{ h *httpClient }

func krakenPair(symbol string) string { return strings.ReplaceAll(symbol, "/", "") }

func (k *kraken) page(ctx context.Context, in Instrument, since string) ([]json.RawMessage, string, error) {
	var r struct {
		Error  []string                   `json:"error"`
		Result map[string]json.RawMessage `json:"result"`
	}
	q := url.Values{"pair": {krakenPair(in.Symbol)}, "since": {since}, "count": {"1000"}}
	if err := k.h.get(ctx, "https://api.kraken.com/0/public/Trades?"+q.Encode(), &r); err != nil {
		return nil, "", err
	}
	if len(r.Error) > 0 {
		return nil, "", fmt.Errorf("kraken: %v", r.Error)
	}
	var last string
	var rows []json.RawMessage
	for key, v := range r.Result {
		if key == "last" {
			json.Unmarshal(v, &last)
			continue
		}
		if err := json.Unmarshal(v, &rows); err != nil {
			return nil, "", err
		}
	}
	return rows, last, nil
}

func krakenRow(raw json.RawMessage) (int64, int64, error) {
	var a []json.RawMessage
	if err := json.Unmarshal(raw, &a); err != nil || len(a) < 7 {
		return 0, 0, fmt.Errorf("kraken row %.80s", raw)
	}
	var t float64
	if err := json.Unmarshal(a[2], &t); err != nil {
		return 0, 0, err
	}
	var id int64
	if err := json.Unmarshal(a[6], &id); err != nil {
		return 0, 0, err
	}
	return id, int64(t * 1000), nil
}

func (k *kraken) Start(ctx context.Context, in Instrument, since time.Time) (Cursor, []Trade, error) {
	return Cursor{Since: strconv.FormatInt(since.UnixNano(), 10)}, nil, nil
}

func (k *kraken) Next(ctx context.Context, in Instrument, c Cursor) (Cursor, Batch, error) {
	at := time.Now()
	rows, last, err := k.page(ctx, in, c.Since)
	if err != nil {
		return c, Batch{}, err
	}
	batch := Batch{Head: len(rows) < 1000, At: at}
	for _, raw := range rows {
		id, ts, err := krakenRow(raw)
		if err != nil {
			return c, Batch{}, err
		}
		if id <= c.LastID {
			continue // `since` is inclusive by time
		}
		if c.LastID != 0 && id != c.LastID+1 && batch.Gap == nil {
			batch.Gap = &Gap{Start: time.UnixMilli(c.LastTsMs).UTC(), End: time.UnixMilli(ts).UTC(), Reason: "KRAKEN_TRADE_ID_GAP"}
		}
		batch.Trades = append(batch.Trades, Trade{TsMs: ts, ID: strconv.FormatInt(id, 10), Raw: raw})
		c.LastID, c.LastTsMs = id, ts
	}
	if last != "" {
		c.Since = last
	}
	return c, batch, nil
}

func ms(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }
