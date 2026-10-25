package capture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type hotWindowSource struct {
	Scope                  hotstate.Scope
	Origin                 hotstate.WindowOrigin
	UnavailableBackfillIDs []string
}

func clockLogValue(clock hotstate.ClockEvidence) slog.Value {
	values := []slog.Attr{
		slog.Time("at", clock.Value),
		slog.Bool("precision_present", clock.Precision != nil),
		slog.Bool("reason_present", clock.Reason != nil),
	}
	if clock.Precision != nil {
		values = append(values, slog.String("precision", *clock.Precision))
	}
	if clock.Reason != nil {
		values = append(values, slog.String("reason", *clock.Reason))
	}
	return slog.GroupValue(values...)
}
func optionalClockLogValue(clock hotstate.OptionalClockEvidence) slog.Value {
	values := []slog.Attr{
		slog.Bool("value_present", clock.Value != nil),
		slog.Bool("precision_present", clock.Precision != nil),
		slog.Bool("reason_present", clock.Reason != nil),
	}
	if clock.Value != nil {
		values = append(values, slog.Time("at", *clock.Value))
	}
	if clock.Precision != nil {
		values = append(values, slog.String("precision", *clock.Precision))
	}
	if clock.Reason != nil {
		values = append(values, slog.String("reason", *clock.Reason))
	}
	return slog.GroupValue(values...)
}
func originLogValue(origin hotstate.WindowOrigin) slog.Value {
	values := []slog.Attr{
		slog.String("kind", string(origin.Kind)),
		{Key: "clock", Value: clockLogValue(origin.Clock)},
		slog.Bool("backfill_id_present", origin.BackfillID != nil),
		slog.Bool("started_clock_present", origin.StartedClock != nil),
	}
	if origin.BackfillID != nil {
		values = append(values, slog.String("backfill_id", *origin.BackfillID))
	}
	if origin.StartedClock != nil {
		values = append(values, slog.Attr{Key: "started_clock", Value: optionalClockLogValue(*origin.StartedClock)})
	}
	return slog.GroupValue(values...)
}
func sourceLogValue(sources []hotWindowSource) slog.Value {
	values := make([]slog.Attr, 0, len(sources))
	for index, source := range sources {
		fields := []slog.Attr{
			slog.String("token_id", source.Scope.TokenID), slog.String("event_id", source.Scope.EventID),
			{Key: "origin", Value: originLogValue(source.Origin)},
			slog.Int("unavailable_backfill_count", len(source.UnavailableBackfillIDs)),
		}
		for index, id := range source.UnavailableBackfillIDs {
			fields = append(fields, slog.String("unavailable_backfill_"+strconv.Itoa(index), id))
		}
		values = append(values, slog.Attr{Key: "source_" + strconv.Itoa(index), Value: slog.GroupValue(fields...)})
	}
	return slog.GroupValue(values...)
}

type startupRepairRetry struct {
	bound   time.Time
	running bool
}

func (s *Service) hotOrigins(ctx context.Context, hot []hotstate.Record) (map[hotstate.Scope]hotstate.Selection, error) {
	scopes := make([]hotstate.Scope, 0, len(hot))
	for _, record := range hot {
		scope, err := record.Scope()
		if err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	anchors, err := s.store.BackfillAnchors(ctx, scopes)
	if err != nil {
		return nil, fmt.Errorf("startup backfill anchor lookup unavailable: %w", err)
	}
	return hotstate.ResolveOrigins(hot, anchors)
}

func (s *Service) startupRepairs(ctx context.Context, hot []hotstate.Record, started time.Time) error {
	selections, err := s.hotOrigins(ctx, hot)
	if err != nil {
		return err
	}
	type window struct {
		start, end time.Time
		sources    []hotWindowSource
	}
	spans := map[string]*window{}
	var order []string
	var refusals []error
	for _, record := range hot {
		scope, err := record.Scope()
		if err != nil {
			return err
		}
		selection, found := selections[scope]
		if !found {
			return errors.New("startup window selection omitted a requested scope")
		}
		origin, usable := selection.Origin()
		if !usable {
			refused, declared := selection.Refusal()
			if !declared {
				return errors.New("startup window selection has no origin or refusal")
			}
			refusals = append(refusals, fmt.Errorf("%s/%s: %s: %s", scope.TokenID, scope.EventID, refused.Reason, refused.Detail))
			continue
		}
		at := origin.Clock.Value
		w := spans[scope.TokenID]
		if w == nil {
			w = &window{start: at.Add(-WindowBefore), end: at.Add(WindowAfter)}
			spans[scope.TokenID] = w
			order = append(order, scope.TokenID)
		}
		w.start = minTime(w.start, at.Add(-WindowBefore))
		w.end = maxTime(w.end, at.Add(WindowAfter))
		w.sources = append(w.sources, hotWindowSource{Scope: scope, Origin: origin, UnavailableBackfillIDs: selection.UnavailableBackfillIDs()})
	}
	for _, token := range order {
		w := spans[token]
		if err := ctx.Err(); err != nil {
			return errors.Join(append(refusals, err)...)
		}
		s.log.Info("startup repair", "token", token, "start", pytime.ISO(w.start), "end", pytime.ISO(minTime(started, w.end)), slog.Attr{Key: "recorded_window_sources", Value: sourceLogValue(w.sources)})
		s.repairUnrepaired(ctx, token, w.start, pytime.MinuteFloor(minTime(started, w.end)), false)
	}
	return errors.Join(refusals...)
}

func (s *Service) startupAfterLeading(ctx context.Context, started time.Time) {
	if !s.waitLeading(ctx) {
		return
	}
	bound := maxTime(started, s.clock.Now())
	hot, err := s.store.HotTokens(ctx)
	if err != nil {
		s.log.Warn("authoritative startup membership unavailable", "error", err)
		if ctx.Err() == nil {
			s.queueStartupRepairRetry(bound)
		}
		return
	}
	s.syncHot(ctx, hot)
	s.flush(ctx)
	s.sweepAt(ctx, bound)
	s.startStartupRepairs(ctx, hot, bound)
}

func (s *Service) startStartupRepairs(ctx context.Context, hot []hotstate.Record, bound time.Time) {
	if err := s.startupRepairs(ctx, hot, bound); err != nil {
		s.log.Warn("startup repair pass unavailable", "error", err)
		if ctx.Err() == nil {
			s.queueStartupRepairRetry(bound)
		}
	}
}

func (s *Service) queueStartupRepairRetry(bound time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startupRetry == nil {
		s.startupRetry = &startupRepairRetry{bound: bound}
	}
}
func (s *Service) retryStartupRepairs(ctx context.Context, hot []hotstate.Record) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	retry := s.startupRetry
	if retry == nil || retry.running {
		s.mu.Unlock()
		return
	}
	retry.running = true
	s.mu.Unlock()
	s.spawn(func(_ context.Context) {
		succeeded := false
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.startupRetry != retry {
				return
			}
			if succeeded {
				s.startupRetry = nil
			} else {
				retry.running = false
			}
		}()
		s.syncHot(ctx, hot)
		s.flush(ctx)
		s.sweepAt(ctx, retry.bound)
		if err := s.startupRepairs(ctx, hot, retry.bound); err != nil {
			if ctx.Err() == nil {
				s.log.Warn("startup repair pass remains unavailable", "error", err)
			}
			return
		}
		succeeded = true
	})
}
