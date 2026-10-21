// Package capture is the tank-capture runtime: the hot set, the public WebSocket stream
// groups, the flow sealer, REST repairs, capture obligations and requests, the pollers
// and the stream state Tank reads. It is tank.market.service.MarketService without the
// reads core keeps (snapshots, Market Truth inputs).
package capture

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// Store is the PostgreSQL side the runtime reads (internal/store). Written collections
// resolve to the service's schema; hot tokens, capture requests and BackfillRuns read
// the live tables core writes.
type Store interface {
	Get(ctx context.Context, collection, key string) (map[string]any, error)
	Range(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error)
	Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, ids []string) ([]map[string]any, error)
	LatestTrades(ctx context.Context, ids []string, start time.Time) (map[string]map[string]any, error)
	LatestRecords(ctx context.Context, collection string, groupBy []string, timeField string, ids []string) ([]map[string]any, error)
	HotTokens(ctx context.Context) ([]map[string]any, error)
	BackfillRuns(ctx context.Context, statuses []string) ([]map[string]any, error)
	ClaimRequest(ctx context.Context, writer string, now time.Time) (map[string]any, error)
	FinishRequest(ctx context.Context, row map[string]any) error
}

// Writer queues typed rows and writes them in batches (internal/store.Writer).
type Writer interface {
	Sink(points.Row)
	Flush(ctx context.Context) error
}

// Derivatives is VenueAdapter.derivatives(): (oi, current, settled, interval, mark).
type Derivatives struct {
	OI       []any
	Current  map[string]any
	Settled  []any
	Interval any
	Mark     any
}

// Venue is the REST surface of one instrument at one gate priority (internal/rest).
type Venue interface {
	Discover(ctx context.Context) (map[string]any, error)
	Klines(ctx context.Context, start, end *time.Time) ([]any, error)
	Ticker(ctx context.Context) (map[string]any, error)
	Derivatives(ctx context.Context) (*Derivatives, error)
	Trades(ctx context.Context, start, end *time.Time, cursor any) ([]any, any, error)
	OIHistory(ctx context.Context, start, end time.Time, cursor string) ([]any, string, error)
	DepthSnapshot(ctx context.Context) (map[string]any, error)
	IndependentBook(ctx context.Context, stream map[string]any) (map[string]any, error)
	RecentTrades(ctx context.Context, limit int) ([]any, error)
}

// Venues builds instrument adapters and reads the stablecoin USD rate.
type Venues interface {
	Venue(inst *venue.Instrument, priority int) Venue
	FXRate(ctx context.Context, quote string) (string, error)
}

// Conn is one WebSocket connection (gorilla/websocket.Conn satisfies it).
type Conn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	Close() error
}

// Dialer opens a WebSocket connection.
type Dialer func(ctx context.Context, url string) (Conn, error)

// Clock is the collection clock and a monotonic one.
type Clock interface {
	Now() time.Time
	Monotonic() time.Duration
}

// SystemClock is the process clock at Python's microsecond precision.
type SystemClock struct{ start time.Time }

// NewSystemClock starts the monotonic clock now.
func NewSystemClock() *SystemClock { return &SystemClock{start: time.Now()} }

// Now is utcnow().
func (c *SystemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// Monotonic is time.monotonic() since the clock started.
func (c *SystemClock) Monotonic() time.Duration { return time.Since(c.start) }

// Options are the configuration the runtime reads (internal/config).
type Options struct {
	Instruments   map[string]*venue.Instrument // active registry instruments
	Order         []string                     // their ids in registry order
	Tokens        map[string]map[string]any
	Thresholds    map[string]any
	ConfigVersion string
	CodeVersion   string
	// Writer names the rows this process claims or reports ("live" or "shadow").
	Writer string
	// Shadow runs obligations core already runs, writing their results to the shadow
	// schema only, and leaves capture requests to core's in-process capture.
	Shadow bool
	Log    *slog.Logger
}

// Threshold is thresholds.get(name, default) as a float.
func (o *Options) Threshold(name string, def float64) float64 {
	v, ok := o.Thresholds[name]
	if !ok || v == nil {
		return def
	}
	d, err := pyval.Decimal(v)
	if err != nil {
		return def
	}
	return d.Float64()
}

// Window constants of tank.market.service.
const (
	WindowBefore     = 24 * time.Hour
	WindowAfter      = 72 * time.Hour
	RepairChunk      = time.Hour
	ReferenceSpan    = 24 * time.Hour
	RESTOnlyWindow   = 48 * time.Hour
	BybitRecent      = time.Hour
	HandoffRetry     = 15 * time.Second
	HandoffRetries   = 2
	PagesPerFlush    = 25
	HotPollS         = 30.0
	HotTickerS       = 20.0
	RepairSlots      = 3
	CursorLookback   = 48 * time.Hour
	streamStateEvery = 5 * time.Second
)

// ReferenceVenues is REFERENCE_VENUES: the spot venues tried first for the 03 §5.7
// reference input.
var ReferenceVenues = []string{"binance", "coinbase", "okx", "kraken", "bybit"}

// NoTradeHistory is NO_TRADE_HISTORY.
var NoTradeHistory = map[string]string{"hyperliquid": "NO_LISTED_TRADE_BACKFILL"}

// BybitLimit is BYBIT_LIMIT.
const BybitLimit = "SOURCE_LIMIT: Bybit recent-trade lists only the last 1000 prints"

// ErrStopped is returned by work cut short by shutdown.
var ErrStopped = errors.New("capture stopped")

func decimalOf(v any) (pydec.Dec, bool) {
	d, err := pyval.Decimal(v)
	return d, err == nil
}

// text is str(value) for a present value and "" for None (pyval.Str renders None as
// Python's "None", which must never become an ID or a cursor).
func text(v any) string {
	if v == nil {
		return ""
	}
	return pyval.Str(v)
}
