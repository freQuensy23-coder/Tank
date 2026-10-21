package points

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

var cached = map[string]bool{"mkt.flow_bars_1m": true, "mkt.tickers_24h": true, "mkt.funding_obs": true}

const cacheSize = 10000

type Row struct {
	Collection, Key string
	Record          map[string]any
}

type Sink func(Row)

type lru struct {
	rows  map[string]map[string]any
	order []string
}

func (l *lru) get(key string) map[string]any { return l.rows[key] }

func (l *lru) put(key string, row map[string]any) {
	if _, ok := l.rows[key]; !ok {
		l.order = append(l.order, key)
	}
	l.rows[key] = row
	if len(l.rows) > cacheSize {
		oldest := l.order[0]
		l.order = l.order[1:]
		delete(l.rows, oldest)
	}
}

func (l *lru) drop(key string) {
	if _, ok := l.rows[key]; ok {
		delete(l.rows, key)
		for i, k := range l.order {
			if k == key {
				l.order = append(l.order[:i], l.order[i+1:]...)
				break
			}
		}
	}
}

type priceContext struct {
	markPrice, markTS, indexPrice, indexTS string
	markProvenance, indexProvenance        map[string]any
}

type Points struct {
	mu            sync.Mutex
	clock         pytime.Clock
	sink          Sink
	ConfigVersion string
	CodeVersion   string
	fx            *normalize.FX
	caches        map[string]*lru
	prices        map[string]*priceContext
}

func New(clock pytime.Clock, sink Sink, configVersion, codeVersion string, fx *normalize.FX) *Points {
	return &Points{
		clock: clock, sink: sink, ConfigVersion: configVersion, CodeVersion: codeVersion, fx: fx,
		caches: map[string]*lru{}, prices: map[string]*priceContext{},
	}
}

func (p *Points) cache(collection string) *lru {
	c := p.caches[collection]
	if c == nil {
		c = &lru{rows: map[string]map[string]any{}}
		p.caches[collection] = c
	}
	return c
}

func (p *Points) now() time.Time { return p.clock.Now() }

func semanticContent(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			if k == "config_version" || k == "code_version" || k == "semantic_fingerprint" {
				continue
			}
			skip := false
			for _, f := range []string{"received_ts", "recorded_at", "observed_at", "updated_at"} {
				if k == f || strings.HasPrefix(k, f+"_prec") {
					skip = true
					break
				}
			}
			if !skip {
				out[k] = semanticContent(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = semanticContent(item)
		}
		return out
	}
	return value
}

func sameContent(a, b map[string]any) bool {
	return string(pyjson.MustDumps(semanticContent(a), true)) == string(pyjson.MustDumps(semanticContent(b), true))
}

func (p *Points) Put(collection, key string, row map[string]any) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.put(collection, key, row)
}

func (p *Points) put(collection, key string, row map[string]any) map[string]any {
	row = copyMap(row)
	if collection == "mkt.flow_bars_1m" {
		for k, v := range pytime.Fields("minute_start", row["minute_start"], pytime.Bucket60s) {
			row[k] = v
		}
		if row["received_ts"] != nil {
			row["received_ts_prec"] = pytime.CollectionOnly
		}
	}

	row["config_version"], row["code_version"] = p.ConfigVersion, p.CodeVersion
	if _, ok := row["recorded_at"]; !ok || row["recorded_at"] == nil {
		row["recorded_at"] = pytime.ISO(p.now())
	}
	recorded, err := pytime.Parse(row["recorded_at"])
	if err == nil {
		pytime.CollectionClock(recorded).Into(row, "recorded_at")
	}
	if cached[collection] {
		c := p.cache(collection)
		if old := c.get(key); old != nil && sameContent(old, row) {
			return old
		}
		c.put(key, row)
	}
	p.sink(Row{collection, key, row})
	return row
}

func (p *Points) Cached(collection, key string) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cache(collection).get(key)
}

func (p *Points) Restore(collection, key string, row map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache(collection).get(key) == nil {
		p.cache(collection).put(key, row)
	}
}

func (p *Points) Forget(collection, key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache(collection).drop(key)
}

func copyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = copyValue(v)
	}
	return out
}

func copyValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return copyMap(x)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = copyValue(item)
		}
		return out
	}
	return v
}

func truthy(v any) bool { return pyval.Truthy(v) }

func str(v any) string { return pyval.Str(v) }

func iso(v any) string {
	s, err := pytime.ISOOf(v)
	if err != nil {
		panic(err)
	}
	return s
}

func after(a, b any) bool { return pytime.MustParse(a).After(pytime.MustParse(b)) }

func precisionOf(fact map[string]any) string {
	switch fact["clock_basis"] {
	case "SOURCE_TIME":
		return pytime.Exact
	case "COLLECTION_TIME":
		return pytime.CollectionOnly
	}
	return ""
}

func normalizeFact(fact map[string]any) {
	precision := precisionOf(fact)
	fact["as_of_prec"] = none(precision)
	for k, v := range pytime.Fields("as_of", fact["as_of"], precision) {
		fact[k] = v
	}
	receivedPrec := ""
	if truthy(fact["received_ts"]) {
		receivedPrec = pytime.CollectionOnly
	}
	for k, v := range pytime.Fields("received_ts", fact["received_ts"], receivedPrec) {
		fact[k] = v
	}
}

func none(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func equalValues(a, b any) bool {
	return string(pyjson.MustDumps(a, true)) == string(pyjson.MustDumps(b, true))
}

func (p *Points) SaveTickerObservation(inst *venue.Instrument, row map[string]any) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saveTicker(inst, row)
}

func (p *Points) saveTicker(inst *venue.Instrument, input map[string]any) map[string]any {
	prec, _ := input["ts_prec"].(string)
	row := copyMap(input)
	for k, v := range pytime.Fields("ts", input["ts"], prec) {
		row[k] = v
	}
	key := inst.ID + ":" + row["ts"].(string)
	previous := p.cache("mkt.tickers_24h").get(key)
	incoming := map[string]map[string]any{}
	if fs, ok := row["field_sources"].(map[string]any); ok {
		for field, fact := range fs {
			incoming[field] = copyMap(fact.(map[string]any))
		}
	}
	delete(row, "field_sources")
	for _, fact := range incoming {
		normalizeFact(fact)
	}
	sources := map[string]map[string]any{}
	unknown := map[string]any{}
	if previous != nil {
		if fs, ok := previous["field_sources"].(map[string]any); ok {
			for field, fact := range fs {
				sources[field] = copyMap(fact.(map[string]any))
			}
		}
		if uc, ok := previous["unclocked_components"].(map[string]any); ok {
			for k, v := range uc {
				unknown[k] = copyValue(v)
			}
		}
	}
	for _, component := range []string{"mark", "index"} {
		priceField, asOf := component+"_price", component+"_as_of"
		if row[priceField] != nil && !truthy(row[asOf]) {
			unknown[priceField] = map[string]any{
				"value": row[priceField], "as_of": nil, "received_ts": row["received_ts"],
				"capture_source": row["capture_source"], "reason": "SOURCE_CLOCK_UNAVAILABLE",
			}
			delete(row, priceField)
			delete(row, asOf)
			delete(incoming, priceField)
		} else if previous != nil && truthy(previous[asOf]) && truthy(row[asOf]) && after(previous[asOf], row[asOf]) {
			delete(row, priceField)
			delete(row, asOf)
			delete(incoming, priceField)
		}
	}
	merged := map[string]any{}
	for k, v := range previous {
		merged[k] = copyValue(v)
	}
	for field, value := range row {
		if field == "capture_source" || field == "capture_sources" {
			continue
		}
		if value != nil || merged[field] == nil {
			merged[field] = value
			if fact, ok := incoming[field]; ok && value != nil {
				prior := sources[field]
				if previous == nil || !equalValues(previous[field], value) || !equalValues(comparable(prior), comparable(fact)) {
					sources[field] = fact
				}
			}
		}
	}
	for _, field := range []string{"mark_price", "index_price", "last", "quote_volume", "volume_native", "volume_usd"} {
		if merged[field] != nil && sources[field] == nil {
			clockField := "ts"
			switch field {
			case "mark_price":
				clockField = "mark_as_of"
			case "index_price":
				clockField = "index_as_of"
			}
			var legacySource, receivedTS any
			if previous != nil {
				legacySource, receivedTS = previous["capture_source"], previous["received_ts"]
			}
			sources[field] = map[string]any{
				"capture_source": nil, "legacy_capture_source": legacySource, "as_of": merged[clockField],
				"received_ts": receivedTS, "clock_basis": "LEGACY_SOURCE_UNSPECIFIED",
			}
		}
	}
	fieldSources := map[string]any{}
	for field, fact := range sources {
		normalizeFact(fact)
		if field == "mark_price" || field == "index_price" {
			name := strings.TrimSuffix(field, "_price") + "_as_of"
			for k, v := range pytime.Fields(name, merged[name], precisionOf(fact)) {
				merged[k] = v
			}
		}
		fieldSources[field] = fact
	}
	merged["field_sources"] = fieldSources
	if truthy(merged["received_ts"]) {
		pytime.CollectionClock(pytime.MustParse(merged["received_ts"])).Into(merged, "received_ts")
	}
	if len(unknown) > 0 {
		merged["unclocked_components"] = unknown
	}
	transportSet := map[string]bool{}
	complete := len(sources) > 0
	for _, fact := range sources {
		if s, ok := fact["capture_source"].(string); ok && s != "" {
			transportSet[s] = true
		} else {
			complete = false
		}
	}
	transports := make([]string, 0, len(transportSet))
	for t := range transportSet {
		transports = append(transports, t)
	}
	sort.Strings(transports)
	list := make([]any, len(transports))
	for i, t := range transports {
		list[i] = t
	}
	merged["capture_sources"] = list
	merged["source_provenance_complete"] = complete
	merged["capture_source"] = nil
	if len(transports) == 1 && complete {
		merged["capture_source"] = transports[0]
	}
	if merged["index_price"] != nil {
		merged["index_reason"] = nil
	}
	if merged["volume_usd"] != nil {
		merged["usd_reason"] = nil
	}
	return p.put("mkt.tickers_24h", key, merged)
}

func comparable(fact map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range fact {
		if k != "received_ts" {
			out[k] = v
		}
	}
	return out
}

type MarkInput struct {
	Price, TS, Index, IndexTS any
	Source                    string
	Received                  any
	IndexClockBasis           string
	Persist                   bool
}

func (p *Points) SaveMark(inst *venue.Instrument, in MarkInput) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saveMark(inst, in)
}

func (p *Points) context(iid string) *priceContext {
	c := p.prices[iid]
	if c == nil {
		c = &priceContext{}
		p.prices[iid] = c
	}
	return c
}

func (p *Points) saveMark(inst *venue.Instrument, in MarkInput) {
	if in.Source == "" {
		in.Source = "WEBSOCKET"
	}
	ctx := p.context(inst.ID)
	receivedValue := in.Received
	if receivedValue == nil {
		receivedValue = p.now()
	}
	received := iso(receivedValue)
	key := inst.Key()
	restPath := ":rest:mark-price"
	switch {
	case key == "hyperliquid.perp":
		restPath = ":rest:metaAndAssetCtxs"
	case strings.HasPrefix(key, "bybit"):
		restPath = ":rest:tickers"
	case key == "binance.usdm":
		restPath = ":rest:premiumIndex"
	}
	sourcePath := key + ":websocket:mark_context"
	basis := "SOURCE_TIME"
	if in.Source == "PUBLIC_REST" {
		sourcePath, basis = key+restPath, "COLLECTION_TIME"
	}
	markFact := map[string]any{
		"capture_source": in.Source, "source_path": sourcePath, "as_of": iso(in.TS),
		"received_ts": received, "clock_basis": basis,
	}
	if ctx.markTS == "" || !pytime.MustParse(in.TS).Before(pytime.MustParse(ctx.markTS)) {
		ctx.markPrice, ctx.markTS, ctx.markProvenance = str(in.Price), iso(in.TS), markFact
	}
	indexTSValue := in.IndexTS
	if !truthy(indexTSValue) {
		indexTSValue = in.TS
	}
	incomingIndexTS := iso(indexTSValue)
	if in.Index != nil && (ctx.indexTS == "" || !pytime.MustParse(incomingIndexTS).Before(pytime.MustParse(ctx.indexTS))) {
		indexPath := sourcePath
		if key == "okx.swap" && in.Source == "PUBLIC_REST" {
			indexPath = key + ":rest:index-tickers"
		}
		clockBasis := in.IndexClockBasis
		if clockBasis == "" {
			clockBasis = basis
		}
		ctx.indexPrice, ctx.indexTS = str(in.Index), incomingIndexTS
		ctx.indexProvenance = map[string]any{
			"capture_source": in.Source, "source_path": indexPath, "as_of": incomingIndexTS,
			"received_ts": received, "clock_basis": clockBasis,
		}
	}
	if !in.Persist {
		return
	}
	markTS := pytime.MustParse(ctx.markTS)
	aligned := false
	if ctx.indexPrice != "" {
		d := pytime.MustParse(ctx.indexTS).Sub(markTS)
		aligned = d <= time.Minute && d >= -time.Minute
	}
	observed := markTS
	if aligned {
		if it := pytime.MustParse(ctx.indexTS); it.After(observed) {
			observed = it
		}
	}
	var facts []map[string]any
	for _, fact := range []map[string]any{ctx.markProvenance, ctx.indexProvenance} {
		if len(fact) > 0 && pytime.MustParse(fact["as_of"]).Equal(observed) {
			facts = append(facts, fact)
		}
	}
	precision := ""
	if len(facts) > 0 {
		allSource, allKnown := true, true
		for _, fact := range facts {
			b := fact["clock_basis"]
			allSource = allSource && b == "SOURCE_TIME"
			allKnown = allKnown && (b == "SOURCE_TIME" || b == "COLLECTION_TIME")
		}
		if allSource {
			precision = pytime.Exact
		} else if allKnown {
			precision = pytime.CollectionOnly
		}
	}
	receivedRow := in.Received
	if receivedRow == nil {
		receivedRow = p.now()
	}
	row := map[string]any{
		"instrument_id": inst.ID, "token_id": inst.TokenID, "ts": pytime.ISO(observed), "ts_prec": none(precision),
		"received_ts": iso(receivedRow), "kind": "MARK_PRICE", "mark_price": ctx.markPrice, "mark_as_of": ctx.markTS,
		"index_price": nil, "index_as_of": nil, "index_reason": "ALIGNED_INDEX_PRICE_UNAVAILABLE",
		"capture_source": in.Source, "quote_asset": inst.QuoteValue(),
	}
	if aligned {
		row["index_price"], row["index_as_of"], row["index_reason"] = ctx.indexPrice, ctx.indexTS, nil
	}
	sources := map[string]any{}
	if len(ctx.markProvenance) > 0 {
		sources["mark_price"] = ctx.markProvenance
	}
	if len(ctx.indexProvenance) > 0 && aligned {
		sources["index_price"] = ctx.indexProvenance
	}
	row["field_sources"] = sources
	saved := p.saveTicker(inst, row)
	ctx.markPrice, ctx.markTS = str(saved["mark_price"]), str(saved["mark_as_of"])
	fs, _ := saved["field_sources"].(map[string]any)
	if f, ok := fs["mark_price"].(map[string]any); ok && len(f) > 0 {
		ctx.markProvenance = f
	}
	if saved["index_price"] != nil {
		ctx.indexPrice, ctx.indexTS = str(saved["index_price"]), str(saved["index_as_of"])
		if f, ok := fs["index_price"].(map[string]any); ok && len(f) > 0 {
			ctx.indexProvenance = f
		}
	}
}

func (p *Points) OKXIndex(inst *venue.Instrument, idxPx, ts any, persist func() bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx := p.context(inst.ID)
	ctx.indexPrice, ctx.indexTS = str(idxPx), iso(ts)
	if ctx.markPrice != "" {
		p.saveMark(inst, MarkInput{Price: ctx.markPrice, TS: ctx.markTS, Index: idxPx, IndexTS: ts, Persist: persist()})
	}
}

func (p *Points) Funding(inst *venue.Instrument, raw map[string]any, kind string, interval any, source string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if source == "" {
		source = "PUBLIC_REST"
	}
	row, err := normalize.FundingRecord(inst, raw, kind, interval, nil, p.now())
	if err != nil {
		return err
	}
	row["capture_source"] = source
	row["source_path"] = inst.Key() + ":rest:funding"
	if source == "WEBSOCKET" {
		row["source_path"] = inst.Key() + ":websocket:funding"
	}
	key := inst.ID + ":" + kind + ":" + row["as_of"].(string)
	previous := p.cache("mkt.funding_obs").get(key)
	observations := map[string]any{}
	if previous != nil {
		if obs, ok := previous["source_observations"].(map[string]any); ok {
			for k, v := range obs {
				observations[k] = copyValue(v)
			}
		}
		if len(observations) == 0 {
			prior := "UNKNOWN"
			if truthy(previous["source_archive"]) {
				prior = "ARCHIVE"
			}
			if v, ok := previous["capture_source"]; ok {
				prior = str(v)
			}
			observations[prior] = copyMap(previous)
		}
	}
	metadata := map[string]bool{
		"recorded_at": true, "recorded_at_prec": true, "recorded_at_prec_reason": true,
		"received_ts": true, "code_version": true, "config_version": true,
	}
	content := func(v map[string]any) string {
		out := map[string]any{}
		for k, item := range v {
			if !metadata[k] {
				out[k] = item
			}
		}
		return pyjson.SHA256(out)
	}
	prior, _ := observations[source].(map[string]any)
	if prior == nil || content(prior) != content(row) {
		observation := copyMap(row)
		pytime.CollectionClock(p.now()).Into(observation, "recorded_at")
		observations[source] = observation
	}
	primary := "WEBSOCKET"
	if kind == "SETTLED" {
		primary = "PUBLIC_REST"
	}
	if observations[primary] == nil {
		if observations["PUBLIC_REST"] != nil {
			primary = "PUBLIC_REST"
		} else {
			primary = jsonbFirstKey(observations)
		}
	}
	merged := copyMap(observations[primary].(map[string]any))
	values := map[string]any{}
	signatures := map[string]bool{}
	for name, v := range observations {
		obs := v.(map[string]any)
		values[name] = map[string]any{"rate": obs["rate"], "interval_h": obs["interval_h"], "rate_8h_eq": obs["rate_8h_eq"]}
		var sig []string
		for _, f := range []string{"rate", "interval_h", "rate_8h_eq"} {
			d, err := pyval.Decimal(obs[f])
			if err != nil {
				return err
			}
			sig = append(sig, valueKey(d))
		}
		signatures[strings.Join(sig, "|")] = true
	}
	merged["source_observations"] = observations
	merged["primary_source"] = primary
	merged["source_disagreement"] = map[string]any{"present": len(signatures) > 1, "values": values}
	p.put("mkt.funding_obs", key, merged)
	return nil
}

func valueKey(d pydec.Dec) string {
	s := d.FormatF()
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	if s == "-0" || s == "" {
		s = "0"
	}
	return s
}

func jsonbFirstKey(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) < len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys[0]
}

func (p *Points) Ticker(inst *venue.Instrument, raw map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	received := pyval.GetOr(raw, "_capture_observed_at", pytime.ISO(p.now()))
	key := inst.Key()
	var native, quote, price any
	switch {
	case strings.HasPrefix(key, "binance"):
		native, quote, price = raw["volume"], raw["quoteVolume"], raw["lastPrice"]
	case strings.HasPrefix(key, "okx"):
		native, quote, price = raw["vol24h"], raw["volCcy24h"], raw["last"]
	case strings.HasPrefix(key, "bybit"):
		native, quote, price = raw["volume24h"], raw["turnover24h"], raw["lastPrice"]
	case strings.HasPrefix(key, "coinbase"):
		native, price = raw["volume"], raw["last"]
	case strings.HasPrefix(key, "kraken"):
		v, c := pyval.List(raw["v"]), pyval.List(raw["c"])
		if len(v) < 2 || len(c) < 1 {
			return pyval.Semantic("Kraken ticker without v/c")
		}
		native, price = v[1], c[0]
	default:
		quote, price = raw["dayNtlVlm"], raw["markPx"]
	}
	if key == "okx.swap" {
		native, quote = raw["volCcy24h"], nil
	}
	var usd any
	if quote != nil && inst.QuoteUSDRate != nil {
		q, err := pyval.Decimal(quote)
		if err != nil {
			return err
		}
		r, err := pyval.Decimal(inst.QuoteUSDRate)
		if err != nil {
			return err
		}
		usd = pydec.Mul(q, r, pydec.DefaultPrec).String()
	}
	clocked := raw["ts"] != nil || raw["closeTime"] != nil
	tsValue := pyval.GetOr(raw, "ts", pyval.GetOr(raw, "closeTime", received))
	row := map[string]any{
		"instrument_id": inst.ID, "token_id": inst.TokenID, "ts": iso(tsValue), "ts_prec": pytime.CollectionOnly,
		"received_ts": received, "received_ts_prec": pytime.CollectionOnly,
		"volume_native": jsonish(native), "quote_volume": jsonish(quote), "volume_usd": usd, "last": jsonish(price),
		"mark_price": jsonish(pyval.GetOr(raw, "markPrice", raw["markPx"])), "quote_asset": inst.QuoteValue(),
		"usd_reason": nil, "capture_source": "PUBLIC_REST",
	}
	if clocked {
		row["ts_prec"] = pytime.Exact
	}
	if usd == nil {
		row["usd_reason"] = "24H_QUOTE_NOTIONAL_UNAVAILABLE"
	}
	sourcePath := key + ":rest:ticker"
	if key == "hyperliquid.perp" {
		sourcePath = key + ":rest:metaAndAssetCtxs"
	} else if strings.HasPrefix(key, "bybit") {
		sourcePath = key + ":rest:tickers"
	}
	basis := "COLLECTION_TIME"
	if clocked {
		basis = "SOURCE_TIME"
	}
	sources := map[string]any{}
	for _, field := range []string{"volume_native", "quote_volume", "volume_usd", "last"} {
		if row[field] != nil {
			sources[field] = map[string]any{
				"capture_source": "PUBLIC_REST", "source_path": sourcePath, "as_of": row["ts"],
				"received_ts": received, "clock_basis": basis,
			}
		}
	}
	if row["mark_price"] != nil {

		row["mark_as_of"] = received
		sources["mark_price"] = map[string]any{
			"capture_source": "PUBLIC_REST", "source_path": sourcePath, "as_of": received,
			"received_ts": received, "clock_basis": "COLLECTION_TIME",
		}
	}
	row["field_sources"] = sources
	p.saveTicker(inst, row)
	return nil
}

func jsonish(v any) any {
	switch x := v.(type) {
	case pydec.Dec:
		return x.String()
	case nil, string, bool:
		return v
	default:
		if n, ok := x.(interface{ String() string }); ok {
			s := n.String()
			if d, err := pydec.Parse(s); err == nil && !strings.ContainsAny(s, ".eE") {
				if i, ok := d.Int64(); ok {
					return i
				}
			}
			return str(v)
		}
	}
	return v
}

func (p *Points) StoreKline(inst *venue.Instrument, row map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if row["quote_volume"] == nil {
		flow := p.cache("mkt.flow_bars_1m").get(inst.ID + ":" + row["open_time"].(string))
		if flow != nil && flow["complete"] == true && flow["quote_volume"] != nil {
			buy, e1 := pyval.Decimal(flow["buy_qty"])
			sell, e2 := pyval.Decimal(flow["sell_qty"])
			native, e3 := pyval.Decimal(row["volume_native"])
			if e1 == nil && e2 == nil && e3 == nil && pydec.Add(buy, sell, pydec.Precise).Equal(native) {
				row["quote_volume"] = flow["quote_volume"]
				row["quote_volume_method"] = "COMPLETE_MATCHING_RAW_PRINTS"
				if flow["volume_usd"] != nil {
					row["quote_volume_usd"] = flow["volume_usd"]
					row["quote_volume_usd_reason"] = nil
				}
			}
		}
	}
	p.put("mkt.klines_1m", inst.ID+":"+row["open_time"].(string), row)
}

func (p *Points) PutFlow(key string, row map[string]any) map[string]any {
	return p.Put("mkt.flow_bars_1m", key, row)
}
