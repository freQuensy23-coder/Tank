package main

import (
	"context"
	"log"
	"strconv"
	"time"
)

// Settle is how long after a request the loader waits before treating a minute as
// final: venues publish a trade a little after its timestamp.
const Settle = 5 * time.Second

type job struct {
	in       Instrument
	venue    Venue
	store    *Store
	backfill time.Duration
	retain   time.Duration
	idle     time.Duration // pause between polls once the head is reached
}

func (j *job) run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := j.loop(ctx); err != nil && ctx.Err() == nil {
			log.Printf("%s: %v; retry in 30s", j.in.ID, err)
			select {
			case <-ctx.Done():
			case <-time.After(30 * time.Second):
			}
		}
	}
}

func (j *job) cursor(ctx context.Context) (Cursor, error) {
	wm := &j.store.wm
	since := time.Now().UTC().Add(-j.backfill)
	if wm.LastID != "" && time.UnixMilli(wm.LastTsMs).After(since) {
		id, _ := strconv.ParseInt(wm.LastID, 10, 64)
		return Cursor{LastID: id, LastTsMs: wm.LastTsMs, Since: wm.CursorSince}, nil
	}
	// No usable position: start over at the backfill horizon. Anything published before
	// stays published; the restart point is recorded as the start of coverage.
	c, first, err := j.venue.Start(ctx, j.in, since)
	if err != nil {
		return c, err
	}
	if wm.LastID != "" {
		j.store.AddGap(Gap{Start: time.UnixMilli(wm.LastTsMs).UTC(), End: since, Reason: "LOADER_RESTARTED_BEYOND_RETENTION"})
	}
	wm.CoveredFrom = since.Truncate(time.Minute).Add(time.Minute)
	wm.SealedThrough = wm.CoveredFrom
	if err := j.store.Append(first); err != nil {
		return c, err
	}
	if c.Since != "" {
		wm.CursorSince = c.Since
	}
	return c, j.store.Publish(wm.SealedThrough)
}

func (j *job) loop(ctx context.Context) error {
	c, err := j.cursor(ctx)
	if err != nil {
		return err
	}
	lastPrune := time.Time{}
	for ctx.Err() == nil {
		var b Batch
		c, b, err = j.venue.Next(ctx, j.in, c)
		if err != nil {
			return err
		}
		if b.Gap != nil {
			j.store.AddGap(*b.Gap)
			log.Printf("%s: gap %s .. %s (%s)", j.in.ID, b.Gap.Start.Format(time.RFC3339), b.Gap.End.Format(time.RFC3339), b.Gap.Reason)
		}
		if err := j.store.Append(b.Trades); err != nil {
			return err
		}
		j.store.wm.LastID, j.store.wm.LastTsMs, j.store.wm.CursorSince = strconv.FormatInt(c.LastID, 10), c.LastTsMs, c.Since
		sealed := j.store.wm.SealedThrough
		if b.Head {
			// Everything up to the request time (less the settle margin) has been read.
			sealed = b.At.Add(-Settle).UTC().Truncate(time.Minute)
		} else if len(b.Trades) > 0 {
			// Mid-backfill: the minutes before the last trade's minute are complete.
			sealed = time.UnixMilli(b.Trades[len(b.Trades)-1].TsMs).UTC().Truncate(time.Minute)
		}
		if time.Since(lastPrune) > 10*time.Minute {
			j.store.Prune(j.retain)
			lastPrune = time.Now()
		}
		if err := j.store.Publish(sealed); err != nil {
			return err
		}
		if b.Head {
			select {
			case <-ctx.Done():
			case <-time.After(j.idle):
			}
		}
	}
	return ctx.Err()
}
