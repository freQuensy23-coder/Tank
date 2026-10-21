// Package rawstore keeps every captured print on disk (HLD §7.2): one Parquet file per
// instrument and UTC hour at raw/trades/{venue}/{market}/{symbol}/{yyyy-mm-dd}/{hh}.parquet,
// all columns text, the layout tank.market.archive.TradeArchive reads.
//
// Prints are buffered in memory and appended every flush to that hour's JSON-lines journal
// (.{hh}.capture.jsonl, fsynced), so a restart loses nothing it flushed. An hour whose end
// is `grace` past is sealed: the journal and any earlier file of the hour are merged,
// duplicates (same trade_id) dropped, and {hh}.parquet replaced atomically.
package rawstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

// Columns are the stored fields, sorted as the Python writer sorts them.
var Columns = []string{
	"capture_source", "code_version", "config_version", "instrument_id", "market_type", "metric_key",
	"notional_usd", "price", "qty_native", "quote_asset", "quote_notional", "raw_first_id", "raw_last_id",
	"received_ts", "received_ts_prec", "received_ts_prec_reason", "sequence", "sequence_semantics", "side",
	"source_payload_json", "token_id", "trade_aggregation", "trade_id", "ts", "ts_prec", "ts_prec_reason",
	"usd_reason", "venue", "venue_symbol",
}

const journalSuffix = ".capture.jsonl"

// Grace is how long after its end an hour stays open for late prints (TradeArchive grace_s).
const Grace = 2 * time.Minute

// Row is one stored print: column -> text, absent for null.
type Row map[string]*string

// Trades writes the hourly trade files under root/raw/trades.
type Trades struct {
	root         string
	code, config string
	grace        time.Duration
	log          *slog.Logger
	mu           sync.Mutex
	buf          map[string][]Row // hour file -> prints not yet journaled
	io           sync.Mutex       // journal appends and seals never interleave
	journaled    map[string]bool  // hour files with a journal on disk
	recovered    bool
}

// New writes under dataDir/raw/trades; rows carry code and config versions.
func New(dataDir, code, config string, grace time.Duration, log *slog.Logger) *Trades {
	return &Trades{
		root: filepath.Join(dataDir, "raw", "trades"), code: code, config: config, grace: grace, log: log,
		buf: map[string][]Row{}, journaled: map[string]bool{},
	}
}

// component is urllib.parse.quote(value, safe="").
func component(v string) (string, error) {
	if v == "" || v == "." || v == ".." {
		return "", fmt.Errorf("raw archive path identity is incomplete")
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.IndexByte("_.-~", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String(), nil
}

// Path is the hour file of a print.
func (t *Trades) Path(venueName, market, symbol string, ts time.Time) (string, error) {
	parts := make([]string, 3)
	for i, v := range []string{strings.ToLower(venueName), strings.ToLower(market), symbol} {
		c, err := component(v)
		if err != nil {
			return "", err
		}
		parts[i] = c
	}
	ts = ts.UTC()
	return filepath.Join(t.root, parts[0], parts[1], parts[2], ts.Format("2006-01-02"), ts.Format("15")+".parquet"), nil
}

func text(v any) *string {
	var s string
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		s = x
	case fmt.Stringer:
		s = x.String()
	default:
		s = pyjson.Canonical(x)
	}
	return &s
}

// Add buffers one normalized print.
func (t *Trades) Add(tr *normalize.Trade) {
	rec := tr.Record()
	rec["code_version"], rec["config_version"] = t.code, t.config
	row := make(Row, len(Columns))
	for _, c := range Columns {
		if p := text(rec[c]); p != nil {
			row[c] = p
		}
	}
	symbol := ""
	if p := text(tr.VenueSymbol); p != nil {
		symbol = *p
	}
	path, err := t.Path(tr.Venue, tr.MarketType, symbol, tr.TS)
	if err != nil {
		t.log.Warn("raw trade not stored", "instrument", tr.InstrumentID, "error", err.Error())
		return
	}
	t.mu.Lock()
	t.buf[path] = append(t.buf[path], row)
	t.mu.Unlock()
}

func journalOf(hourFile string) string {
	dir, base := filepath.Split(hourFile)
	return filepath.Join(dir, "."+strings.TrimSuffix(base, ".parquet")+journalSuffix)
}

func hourOf(hourFile string) (time.Time, error) {
	day := filepath.Base(filepath.Dir(hourFile))
	hh := strings.TrimSuffix(filepath.Base(hourFile), ".parquet")
	return time.Parse("2006-01-02T15", day+"T"+hh)
}

// Flush appends the buffered prints to their hour journals, then seals finished hours.
func (t *Trades) Flush(now time.Time) error {
	t.mu.Lock()
	buf := t.buf
	t.buf = map[string][]Row{}
	t.mu.Unlock()
	t.io.Lock()
	defer t.io.Unlock()
	if !t.recovered {
		t.recover()
	}
	var firstErr error
	for path, rows := range buf {
		if err := appendJournal(journalOf(path), rows); err != nil {
			t.restore(path, rows)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		t.journaled[path] = true
	}
	paths := make([]string, 0, len(t.journaled))
	for path := range t.journaled {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		hour, err := hourOf(path)
		if err != nil || now.Before(hour.Add(time.Hour+t.grace)) {
			continue
		}
		if err := Seal(path); err != nil {
			t.log.Warn("raw trade seal failed", "path", path, "error", err.Error())
			continue // the journal stays; the hour is retried next flush
		}
		delete(t.journaled, path)
	}
	return firstErr
}

// restore puts back prints whose journal append failed, ahead of later ones.
func (t *Trades) restore(path string, rows []Row) {
	t.mu.Lock()
	t.buf[path] = append(rows, t.buf[path]...)
	t.mu.Unlock()
}

// recover finds journals an earlier process left, so their hours get sealed.
func (t *Trades) recover() {
	t.recovered = true
	_ = filepath.WalkDir(t.root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), journalSuffix) {
			hh := strings.TrimSuffix(strings.TrimPrefix(d.Name(), "."), journalSuffix)
			t.journaled[filepath.Join(filepath.Dir(p), hh+".parquet")] = true
		}
		return nil
	})
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func appendJournal(path string, rows []Row) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if st.Size() > 0 {
		// An earlier process cut mid-line: end that line so the next row stands alone.
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, st.Size()-1); err == nil && last[0] != '\n' {
			if _, err := f.Write([]byte{'\n'}); err != nil {
				f.Close()
				return err
			}
			if st, err = f.Stat(); err != nil {
				f.Close()
				return err
			}
		}
	}
	// A failed append (disk full) is cut back to the last whole line, so the rows are
	// retried and nothing after them is hidden behind a torn line.
	fail := func(err error) error {
		_ = f.Truncate(st.Size())
		f.Close()
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return fail(err)
		}
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(dir)
}

func schema() *parquet.Schema {
	g := parquet.Group{}
	for _, c := range Columns {
		g[c] = parquet.Optional(parquet.String())
	}
	return parquet.NewSchema("schema", g)
}

// Seal merges an hour's journal (and its earlier file) into {hh}.parquet. A journal of an
// already sealed hour (late or repaired prints) usually repeats prints the file has: only
// the file's trade_id column is read to find that out, and the file is rewritten only
// when the journal brings a new print.
func Seal(hourFile string) error {
	journal := journalOf(hourFile)
	if _, err := os.Stat(hourFile); err == nil {
		ids, err := readIDs(hourFile)
		if err != nil {
			return err
		}
		fresh := false
		if err := readJournal(journal, func(r Row) error {
			if p := r["trade_id"]; p == nil || !ids[*p] {
				fresh = true
			}
			return nil
		}); err != nil {
			return err
		}
		if !fresh {
			if err := os.Remove(journal); err != nil && !os.IsNotExist(err) {
				return err
			}
			return syncDir(filepath.Dir(hourFile))
		}
	}
	sch := schema()
	index := map[string]int{}
	for i, path := range sch.Columns() {
		index[path[0]] = i
	}
	tmp := filepath.Join(filepath.Dir(hourFile), "."+filepath.Base(hourFile)+".tmp")
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	w := parquet.NewWriter(out, sch, parquet.Compression(&zstd.Codec{}))
	seen := map[string]bool{}
	batch := make([]parquet.Row, 0, 4096)
	put := func(r Row) error {
		id := ""
		if p := r["trade_id"]; p != nil {
			id = *p
		}
		if seen[id] {
			return nil
		}
		seen[id] = true
		row := make(parquet.Row, len(Columns))
		for _, c := range Columns {
			i := index[c]
			if p := r[c]; p != nil {
				row[i] = parquet.ByteArrayValue([]byte(*p)).Level(0, 1, i)
			} else {
				row[i] = parquet.NullValue().Level(0, 0, i)
			}
		}
		batch = append(batch, row)
		if len(batch) == cap(batch) {
			if _, err := w.WriteRows(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	}
	if err := readParquet(hourFile, put); err != nil {
		out.Close()
		return err
	}
	if err := readJournal(journal, put); err != nil {
		out.Close()
		return err
	}
	if len(batch) > 0 {
		if _, err := w.WriteRows(batch); err != nil {
			out.Close()
			return err
		}
	}
	if err := w.Close(); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, hourFile); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(hourFile)); err != nil {
		return err
	}
	if err := os.Remove(journal); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(hourFile))
}

// readIDs is the trade_id column of an hour file (the other columns are not decoded).
func readIDs(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	type idRow struct {
		TradeID *string `parquet:"trade_id,optional"`
	}
	r := parquet.NewGenericReader[idRow](f)
	defer r.Close()
	ids := map[string]bool{}
	buf := make([]idRow, 4096)
	for {
		n, err := r.Read(buf)
		for _, row := range buf[:n] {
			if row.TradeID != nil {
				ids[*row.TradeID] = true
			}
		}
		if err == io.EOF {
			return ids, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// readParquet streams the rows of an existing hour file (any writer's text columns).
func readParquet(path string, put func(Row) error) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return err
	}
	names := make([]string, 0)
	for _, path := range pf.Schema().Columns() {
		names = append(names, path[0])
	}
	rows := make([]parquet.Row, 1024)
	for _, rg := range pf.RowGroups() {
		r := rg.Rows()
		for {
			n, err := r.ReadRows(rows)
			for _, pr := range rows[:n] {
				row := Row{}
				for _, v := range pr {
					if !v.IsNull() {
						s := v.String()
						row[names[v.Column()]] = &s
					}
				}
				if perr := put(row); perr != nil {
					r.Close()
					return perr
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				r.Close()
				return err
			}
		}
		r.Close()
	}
	return nil
}

// readJournal streams a journal; a torn line (a crash or a full disk mid-append) is
// skipped, the lines after it still count.
func readJournal(path string, put func(Row) error) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var row Row
		if json.Unmarshal(sc.Bytes(), &row) != nil {
			continue
		}
		if err := put(row); err != nil {
			return err
		}
	}
	return sc.Err()
}
