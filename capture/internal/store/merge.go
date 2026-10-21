package store

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

var strippedPrefixes = []string{"received_ts", "recorded_at", "updated_at", "observed_at"}

func strip(value any) any {
	switch v := value.(type) {
	case map[string]any:
		if v == nil {
			return nil
		}
		out := make(map[string]any, len(v))
		for k, item := range v {
			if isNone(item) || k == "code_version" || k == "config_version" || k == "semantic_fingerprint" {
				continue
			}
			skip := false
			for _, prefix := range strippedPrefixes {
				if strings.HasPrefix(k, prefix) {
					skip = true
					break
				}
			}
			if !skip {
				out[k] = strip(item)
			}
		}
		return out
	case []any:
		if v == nil {
			return nil
		}
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = strip(item)
		}
		return out
	}
	return value
}

func fingerprintValue(value any) string {
	sum := sha256.Sum256(pyjson.MustDumps(strip(value), true))
	return hex.EncodeToString(sum[:])
}

func deepCopy(value any) any {
	switch v := value.(type) {
	case map[string]any:
		if v == nil {
			return v
		}
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = deepCopy(item)
		}
		return out
	case []any:
		if v == nil {
			return v
		}
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = deepCopy(item)
		}
		return out
	}
	return value
}

func copyRecord(m map[string]any) map[string]any {
	out, _ := deepCopy(m).(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func orEmpty(value any) map[string]any {
	if m, ok := value.(map[string]any); ok && len(m) > 0 {
		return m
	}
	return map[string]any{}
}

func get(m any, key string) any {
	if d, ok := m.(map[string]any); ok {
		return d[key]
	}
	return nil
}

func has(m any, key string) bool {
	d, ok := m.(map[string]any)
	if !ok {
		return false
	}
	_, found := d[key]
	return found
}

var fundingChosen = []string{
	"rate", "interval_h", "rate_8h_eq", "as_of", "as_of_prec", "as_of_prec_reason",
	"funding_time", "funding_time_prec", "funding_time_prec_reason",
	"received_ts", "received_ts_prec", "received_ts_prec_reason",
	"recorded_at", "recorded_at_prec", "recorded_at_prec_reason", "source_path",
}

func mergePoint(collection string, old, incoming map[string]any) (map[string]any, error) {
	merged := copyRecord(old)
	for k, v := range incoming {
		if !isNone(v) {
			merged[k] = v
		}
	}
	switch collection {
	case "mkt.funding_obs":
		sources := copyRecord(orEmpty(old["source_observations"]))
		for key, value := range orEmpty(incoming["source_observations"]) {
			if fingerprintValue(sources[key]) != fingerprintValue(value) {
				sources[key] = deepCopy(value)
			}
		}
		merged["source_observations"] = sources
		var preferred any = "WEBSOCKET"
		if merged["kind"] == "SETTLED" {
			preferred = "PUBLIC_REST"
		}
		if _, ok := sources[preferred.(string)]; !ok {
			preferred = incoming["capture_source"]
		}
		if p, ok := preferred.(string); ok {
			if chosen, found := sources[p]; found {
				for _, field := range fundingChosen {
					if has(chosen, field) {
						merged[field] = get(chosen, field)
					}
				}
				merged["capture_source"], merged["primary_source"] = p, p
			}
		}
		rates := map[string]bool{}
		values := map[string]any{}
		for key, value := range sources {
			if rate := get(value, "rate_8h_eq"); rate != nil {
				rates[pyval.Str(rate)] = true
			}
			values[key] = map[string]any{"rate": get(value, "rate"), "interval_h": get(value, "interval_h"), "rate_8h_eq": get(value, "rate_8h_eq")}
		}
		merged["source_disagreement"] = map[string]any{"present": len(rates) > 1, "values": values}
	case "mkt.tickers_24h":
		sources := copyRecord(orEmpty(old["field_sources"]))
		incomingSources := orEmpty(incoming["field_sources"])
		for field, meta := range incomingSources {
			prior := sources[field]
			newer := false
			if asOf := get(meta, "as_of"); prior != nil && pyval.Truthy(asOf) {
				newer = true
				if priorAsOf := get(prior, "as_of"); pyval.Truthy(priorAsOf) {
					a, err := pytime.Parse(asOf)
					if err != nil {
						return nil, err
					}
					b, err := pytime.Parse(priorAsOf)
					if err != nil {
						return nil, err
					}
					newer = !a.Before(b)
				}
			}
			if prior == nil || newer {
				sources[field] = deepCopy(meta)
			} else if v, ok := old[field]; ok {
				merged[field] = v
			}
		}
		for field := range orEmpty(old["field_sources"]) {
			if _, ok := incomingSources[field]; !ok {
				if v, ok := old[field]; ok {
					merged[field] = v
				}
			}
		}
		merged["field_sources"] = sources
		seen := map[string]bool{}
		var captureSources []string
		for _, meta := range sources {
			if s := get(meta, "capture_source"); pyval.Truthy(s) {
				if name := pyval.Str(s); !seen[name] {
					seen[name] = true
					captureSources = append(captureSources, name)
				}
			}
		}
		sort.Strings(captureSources)
		list := make([]any, len(captureSources))
		for i, s := range captureSources {
			list[i] = s
		}
		merged["capture_sources"] = list
		for _, prefix := range []string{"mark", "index"} {
			if meta := sources[prefix+"_price"]; pyval.Truthy(meta) {
				for _, suffix := range []string{"as_of", "as_of_prec", "as_of_prec_reason"} {
					merged[prefix+"_"+suffix] = get(meta, suffix)
				}
			}
		}
		components := copyRecord(orEmpty(old["unclocked_components"]))
		for k, v := range orEmpty(incoming["unclocked_components"]) {
			components[k] = v
		}
		merged["unclocked_components"] = components
	}
	merged["semantic_fingerprint"] = fingerprintValue(merged)
	return merged, nil
}
