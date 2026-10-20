package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Output layout (one direction: the loader writes, the main service only reads):
//
//	<out>/<instrument dir>/<YYYY-MM-DDTHH>.ndjson  one line per trade, ascending time
//	<out>/<instrument dir>/WATERMARK.json          written by atomic rename
//
// A reader may use the bytes of an hour file only up to segments[hour] and only the
// minutes before sealed_through; minutes overlapping a gap are incomplete.

// Trade is one line of an hour file. Raw is the venue's REST object, unchanged, so the
// main service normalizes it with its own code.
type Trade struct {
	TsMs int64           `json:"ts_ms"`
	ID   string          `json:"id"`
	Raw  json.RawMessage `json:"raw"`
}

type Gap struct {
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Reason string    `json:"reason"`
}

type Watermark struct {
	Version       int              `json:"version"`
	InstrumentID  string           `json:"instrument_id"`
	Venue         string           `json:"venue"`
	CoveredFrom   time.Time        `json:"covered_from"`
	SealedThrough time.Time        `json:"sealed_through"`
	LastID        string           `json:"last_id"`
	LastTsMs      int64            `json:"last_ts_ms"`
	CursorSince   string           `json:"cursor_since,omitempty"`
	Segments      map[string]int64 `json:"segments"`
	Gaps          []Gap            `json:"gaps"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

type Store struct {
	dir  string
	wm   Watermark
	pend map[string]int64 // hour file sizes written but not yet published
}

func dirName(id string) string {
	return strings.NewReplacer(":", "__", "/", "-").Replace(id)
}

func hourKey(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02T15") }

func OpenStore(out string, in Instrument) (*Store, error) {
	dir := filepath.Join(out, dirName(in.ID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, wm: Watermark{Version: 1, InstrumentID: in.ID, Venue: in.Venue, Segments: map[string]int64{}}}
	if raw, err := os.ReadFile(filepath.Join(dir, "WATERMARK.json")); err == nil {
		if err := json.Unmarshal(raw, &s.wm); err != nil {
			return nil, fmt.Errorf("%s: corrupt watermark: %w", in.ID, err)
		}
		if s.wm.Segments == nil {
			s.wm.Segments = map[string]int64{}
		}
	}
	// Drop bytes and files written after the last watermark: they were never published.
	files, _ := filepath.Glob(filepath.Join(dir, "*.ndjson"))
	for _, f := range files {
		hour := strings.TrimSuffix(filepath.Base(f), ".ndjson")
		size, ok := s.wm.Segments[hour]
		if !ok {
			os.Remove(f)
		} else if err := os.Truncate(f, size); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Append writes trades (any order within the batch) to their hour files. They are not
// published until Publish.
func (s *Store) Append(trades []Trade) error {
	if len(trades) == 0 {
		return nil
	}
	sort.SliceStable(trades, func(i, j int) bool {
		if trades[i].TsMs != trades[j].TsMs {
			return trades[i].TsMs < trades[j].TsMs
		}
		a, _ := strconv.ParseInt(trades[i].ID, 10, 64)
		b, _ := strconv.ParseInt(trades[j].ID, 10, 64)
		return a < b
	})
	byHour := map[string][]Trade{}
	for _, t := range trades {
		byHour[hourKey(t.TsMs)] = append(byHour[hourKey(t.TsMs)], t)
	}
	for hour, rows := range byHour {
		path := filepath.Join(s.dir, hour+".ndjson")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		for _, t := range rows {
			line, err := json.Marshal(t)
			if err != nil {
				f.Close()
				return err
			}
			w.Write(line)
			w.WriteByte('\n')
		}
		if err := w.Flush(); err == nil {
			err = f.Sync()
		}
		info, _ := f.Stat()
		f.Close()
		if err != nil {
			return err
		}
		s.pending(hour, info.Size())
	}
	last := trades[len(trades)-1]
	if last.TsMs >= s.wm.LastTsMs {
		s.wm.LastTsMs, s.wm.LastID = last.TsMs, last.ID
	}
	return nil
}

func (s *Store) pending(hour string, size int64) {
	if s.pend == nil {
		s.pend = map[string]int64{}
	}
	s.pend[hour] = size
}

// Publish makes everything appended so far visible and moves sealed_through forward.
func (s *Store) Publish(sealed time.Time) error {
	for hour, size := range s.pend {
		s.wm.Segments[hour] = size
	}
	s.pend = nil
	sealed = sealed.UTC().Truncate(time.Minute)
	if sealed.After(s.wm.SealedThrough) {
		s.wm.SealedThrough = sealed
	}
	s.wm.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(s.wm, "", " ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, ".WATERMARK.json.tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, "WATERMARK.json"))
}

func (s *Store) AddGap(g Gap) {
	s.wm.Gaps = append(s.wm.Gaps, g)
	if len(s.wm.Gaps) > 200 {
		s.wm.Gaps = s.wm.Gaps[len(s.wm.Gaps)-200:]
	}
}

// Prune deletes hour files older than the retention and keeps the watermark consistent.
func (s *Store) Prune(retention time.Duration) {
	cut := time.Now().UTC().Add(-retention)
	for hour := range s.wm.Segments {
		t, err := time.Parse("2006-01-02T15", hour)
		if err == nil && t.Add(time.Hour).Before(cut) {
			os.Remove(filepath.Join(s.dir, hour+".ndjson"))
			delete(s.wm.Segments, hour)
		}
	}
	if s.wm.CoveredFrom.Before(cut) {
		s.wm.CoveredFrom = cut.Truncate(time.Hour)
	}
	kept := s.wm.Gaps[:0]
	for _, g := range s.wm.Gaps {
		if g.End.After(cut) {
			kept = append(kept, g)
		}
	}
	s.wm.Gaps = kept
}
