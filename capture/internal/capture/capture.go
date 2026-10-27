package capture

import (
	"context"
	"errors"
	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"log/slog"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/captureparams"
	"github.com/freQuensy23-coder/Tank/capture/internal/coverage"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type Store interface {
	Get(ctx context.Context, collection, key string) (map[string]any, error)
	CoverageMinutes(context.Context, coverage.Query) ([]coverage.Minute, error)
	Range(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) ([]map[string]any, error)
	Projection(ctx context.Context, collection string, fields []string, field string, start, end time.Time, ids []string) ([]map[string]any, error)
	Earliest(ctx context.Context, collection, field string, start, end time.Time, filters map[string]any) (time.Time, bool, error)
	LatestTrades(ctx context.Context, ids []string, start time.Time) (map[string]map[string]any, error)
	LatestRecords(ctx context.Context, collection string, groupBy []string, timeField string, ids []string) ([]map[string]any, error)
	HotTokens(ctx context.Context) ([]hotstate.Record, error)
	BackfillAnchors(ctx context.Context, scopes []hotstate.Scope) ([]hotstate.AnchorRecord, error)
	BackfillObligations(ctx context.Context) ([]backfillstate.Observation, error)
	StartBackfill(ctx context.Context, start backfillstate.Start) (backfillstate.Acknowledgement, error)
	FinishBackfill(ctx context.Context, completion backfillstate.Completion) (backfillstate.Acknowledgement, error)
	ClaimRequest(ctx context.Context, writer string, now time.Time) (*requeststate.Claimed, error)
	PruneFetches(ctx context.Context, before time.Time) (int64, error)
	FinishRequest(ctx context.Context, completion requeststate.Completion) error
}

type Writer interface {
	Sink(points.Row)
	Flush(ctx context.Context) error
}

type Derivatives struct {
	OI       []any
	Current  map[string]any
	Settled  []any
	Interval any
	Mark     any
}

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

type Venues interface {
	Venue(inst *venue.Instrument, priority int) Venue
	FXRate(ctx context.Context, quote string) (string, error)
}

type Conn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	Close() error
}

type Dialer func(ctx context.Context, url string) (Conn, error)

type Clock interface {
	Now() time.Time
	Monotonic() time.Duration
}

type SystemClock struct{ start time.Time }

func NewSystemClock() *SystemClock { return &SystemClock{start: time.Now()} }

func (c *SystemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func (c *SystemClock) Monotonic() time.Duration { return time.Since(c.start) }

type Options struct {
	Instruments   map[string]*venue.Instrument
	Order         []string
	Tokens        map[string]map[string]any
	Parameters    captureparams.Parameters
	ConfigVersion string
	CodeVersion   string

	Writer string

	CanWrite func() bool

	Leading <-chan struct{}

	Fetcher Fetcher

	Raw RawTrades

	Log *slog.Logger
}

type RawTrades interface {
	Add(t *normalize.Trade)
	Flush(now time.Time) error
}

const rawFlushEvery = 5 * time.Second

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
	RepairSlots      = 3
	CursorLookback   = 48 * time.Hour
	streamStateEvery = 5 * time.Second
)

var ReferenceVenues = []string{"binance", "coinbase", "okx", "kraken", "bybit"}

var NoTradeHistory = map[string]string{"hyperliquid": "NO_LISTED_TRADE_BACKFILL"}

const BybitLimit = "SOURCE_LIMIT: Bybit recent-trade lists only the last 1000 prints"

var ErrStopped = errors.New("capture stopped")

func text(v any) string {
	if v == nil {
		return ""
	}
	return pyval.Str(v)
}
