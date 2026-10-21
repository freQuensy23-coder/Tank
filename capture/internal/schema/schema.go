// Package schema is tank.market_schema plus tank.market_storage.encode and
// semantic_fingerprint: the typed market columns, aliases and identities, and the exact
// encoding and fingerprint the Python writer applies before PostgreSQL.
package schema

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

// Column is one typed market column.
type Column struct {
	Name string
	Kind string // text, numeric, bigint, boolean, timestamptz, jsonb, text[]
}

//go:embed market_schema.json
var exported []byte

var (
	// Columns is MARKET_COLUMNS in declaration order.
	Columns map[string][]Column
	// Aliases is MARKET_ALIASES (producer field -> column).
	Aliases map[string]map[string]string
	// Identities is MARKET_IDENTITIES.
	Identities map[string][]string
	// Partitions is MARKET_PARTITIONS.
	Partitions map[string]string
	// Required is MARKET_REQUIRED.
	Required map[string][]string
	kinds    map[string]map[string]string
)

func init() {
	var raw struct {
		Columns    map[string][][2]string       `json:"columns"`
		Aliases    map[string]map[string]string `json:"aliases"`
		Identities map[string][]string          `json:"identities"`
		Partitions map[string]string            `json:"partitions"`
		Required   map[string][]string          `json:"required"`
	}
	if err := json.Unmarshal(exported, &raw); err != nil {
		panic("schema: market_schema.json: " + err.Error())
	}
	Columns, kinds = map[string][]Column{}, map[string]map[string]string{}
	for collection, columns := range raw.Columns {
		kinds[collection] = map[string]string{}
		for _, c := range columns {
			Columns[collection] = append(Columns[collection], Column{c[0], c[1]})
			kinds[collection][c[0]] = c[1]
		}
	}
	Aliases, Identities, Partitions, Required = raw.Aliases, raw.Identities, raw.Partitions, raw.Required
}

// Kind is the column type of collection.name, "" when unknown.
func Kind(collection, name string) string { return kinds[collection][name] }

// Known reports whether collection is a typed market collection.
func Known(collection string) bool { _, ok := kinds[collection]; return ok }

// Encode is tank.market_storage.encode: aliases folded into columns, timestamp
// precision propagated through the shared contract, numerics as decimals, timestamps
// as times. Unknown fields and conflicting aliases are errors, as in Python.
func Encode(collection string, record map[string]any) (map[string]any, error) {
	columns, ok := kinds[collection]
	if !ok {
		return nil, fmt.Errorf("unknown market collection %s", collection)
	}
	aliases := Aliases[collection]
	timestamps := map[string]bool{}
	for name, kind := range columns {
		if kind == "timestamptz" {
			timestamps[name] = true
		}
	}
	timeAliases := map[string]string{}
	for alias, target := range aliases {
		if timestamps[target] {
			timeAliases[alias] = target
		}
	}
	precisionNames := map[string]bool{}
	for field := range timestamps {
		precisionNames[field+"_precision"] = true
	}
	for alias := range timeAliases {
		precisionNames[alias+"_precision"] = true
	}
	result := map[string]any{}
	for key, value := range record {
		if precisionNames[key] || precisionNames[strings.TrimSuffix(key, "_reason")] {
			continue
		}
		target := key
		if t, ok := aliases[key]; ok {
			target = t
		}
		if _, ok := columns[target]; !ok {
			return nil, fmt.Errorf("unknown typed market field %s.%s", collection, key)
		}
		if timestamps[target] && value != nil {
			t, err := pytime.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", collection, key, err)
			}
			value = t
		}
		if previous, seen := result[target]; seen && !same(previous, value) {
			if strings.HasSuffix(target, "_prec") {
				continue
			}
			return nil, fmt.Errorf("conflicting market aliases %s.%s", collection, key)
		}
		result[target] = value
	}
	for field := range timestamps {
		var associated []string
		for alias, target := range timeAliases {
			if target == field {
				associated = append(associated, alias)
			}
		}
		var precisionAliases []string
		for _, alias := range associated {
			precisionAliases = append(precisionAliases, alias+"_prec", alias+"_precision")
		}
		if collection == "mkt.oi_obs" && field == "ts" {
			precisionAliases = append(precisionAliases, "timestamp_precision")
		}
		_, declared := result[field]
		if !declared {
			for _, name := range append([]string{field + "_prec", field + "_precision"}, precisionAliases...) {
				if record[name] != nil {
					declared = true
					break
				}
			}
		}
		if !declared {
			continue
		}
		selected := make(map[string]any, len(record)+1)
		for k, v := range record {
			selected[k] = v
		}
		if v, ok := record[field]; ok {
			selected[field] = v
		} else {
			value := result[field]
			for _, alias := range associated {
				if v, ok := record[alias]; ok {
					value = v
					break
				}
			}
			selected[field] = value
		}
		stamp, err := pytime.Propagate(selected, field, precisionAliases, "", "")
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", collection, field, err)
		}
		for k, v := range stamp.Fields(field) {
			result[k] = v
		}
	}
	for name, value := range result {
		if value == nil {
			continue
		}
		switch columns[name] {
		case "timestamptz":
			t, err := pytime.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", collection, name, err)
			}
			result[name] = t
		case "numeric":
			d, err := toDecimal(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", collection, name, err)
			}
			result[name] = d
		case "text":
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("%s.%s requires text", collection, name)
			}
		case "bigint":
			n, err := toInt(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", collection, name, err)
			}
			result[name] = n
		}
	}
	return result, nil
}

func same(a, b any) bool {
	ta, aok := a.(time.Time)
	tb, bok := b.(time.Time)
	if aok && bok {
		return ta.Equal(tb)
	}
	return string(pyjson.MustDumps(a, true)) == string(pyjson.MustDumps(b, true))
}

func toDecimal(value any) (pydec.Dec, error) {
	switch v := value.(type) {
	case pydec.Dec:
		return v, nil
	case string:
		return pydec.Parse(v)
	case json.Number:
		return pydec.Parse(string(v))
	case int:
		return pydec.FromInt(int64(v)), nil
	case int64:
		return pydec.FromInt(v), nil
	}
	return pydec.Dec{}, fmt.Errorf("not a decimal: %T", value)
}

func toInt(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case json.Number:
		return v.Int64()
	case bool:
		// Python bool is an int subclass; Tank never stores one in a bigint column.
		return 0, fmt.Errorf("boolean in bigint column")
	}
	return 0, fmt.Errorf("not an integer: %T", value)
}

// fingerprintExcluded is FINGERPRINT_EXCLUDED.
var fingerprintExcluded = map[string]map[string]bool{"mkt.book_summaries": {"bids": true, "asks": true}}

var strippedPrefixes = []string{"received_ts", "recorded_at", "updated_at", "observed_at"}

// isNone is Python None for a record value: nil, or a nil map or slice.
func isNone(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case map[string]any:
		return x == nil
	case []any:
		return x == nil
	}
	return false
}

func strip(value any) any {
	switch v := value.(type) {
	case map[string]any:
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
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = strip(item)
		}
		return out
	}
	return value
}

// Fingerprint is semantic_fingerprint(record, collection) of an encoded record.
func Fingerprint(record map[string]any, collection string) string {
	if excluded := fingerprintExcluded[collection]; excluded != nil {
		kept := make(map[string]any, len(record))
		for k, v := range record {
			if !excluded[k] {
				kept[k] = v
			}
		}
		record = kept
	}
	sum := sha256.Sum256(pyjson.MustDumps(strip(record), true))
	return hex.EncodeToString(sum[:])
}

// Identity is the identity tuple values of an encoded record (storage_key uses key).
func Identity(collection string, record map[string]any, key string) ([]any, error) {
	var values []any
	for _, name := range Identities[collection] {
		var v any
		if name == "storage_key" {
			v = key
		} else {
			v = record[name]
		}
		if v == nil || v == "" {
			return nil, fmt.Errorf("missing typed market identity %s: %v", collection, Identities[collection])
		}
		values = append(values, v)
	}
	return values, nil
}
