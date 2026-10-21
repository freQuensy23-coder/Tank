package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/jackc/pgx/v5"
)

func quote(name string) string { return pgx.Identifier{name}.Sanitize() }

func pgTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000Z") }

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

func pgText(collection, name, kind string, value any) (*string, error) {
	if isNone(value) {
		return nil, nil
	}
	var s string
	switch kind {
	case "text":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%s.%s requires text", collection, name)
		}
		s = v
	case "numeric":
		d, ok := value.(pydec.Dec)
		if !ok {
			return nil, fmt.Errorf("%s.%s is not a decimal: %T", collection, name, value)
		}
		s = d.String()
	case "bigint":
		switch v := value.(type) {
		case int64:
			s = strconv.FormatInt(v, 10)
		case int:
			s = strconv.Itoa(v)
		default:
			return nil, fmt.Errorf("%s.%s is not an integer: %T", collection, name, value)
		}
	case "boolean":
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("%s.%s is not a boolean: %T", collection, name, value)
		}
		s = strconv.FormatBool(v)
	case "timestamptz":
		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("%s.%s is not a time: %T", collection, name, value)
		}
		s = pgTime(v)
	case "jsonb", "text[]":
		out, err := pyjson.Dumps(value, false)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", collection, name, err)
		}
		s = string(out)
	default:
		return nil, fmt.Errorf("%s.%s: unknown column type %s", collection, name, kind)
	}
	return &s, nil
}

func pyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	case pydec.Dec:
		return x.String()
	case time.Time:

		t := x.UTC()
		s := t.Format("2006-01-02 15:04:05")
		if us := t.Nanosecond() / 1000; us != 0 {
			s += fmt.Sprintf(".%06d", us)
		}
		return s + "+00:00"
	}
	return fmt.Sprint(v)
}

func identityKey(values []any) string {
	var b strings.Builder
	for _, v := range values {
		switch x := v.(type) {
		case time.Time:
			fmt.Fprintf(&b, "t%d", x.UnixMicro())
		case string:
			b.WriteString("s" + x)
		case pydec.Dec:
			b.WriteString("d" + x.String())
		default:
			fmt.Fprintf(&b, "v%v", x)
		}
		b.WriteByte(0)
	}
	return b.String()
}

func selectExpression(alias, name, kind string) string {
	ref := quote(name)
	if alias != "" {
		ref = alias + "." + ref
	}
	switch kind {
	case "numeric", "jsonb":
		return ref + "::text AS " + quote(name)
	case "text[]":
		return "array_to_json(" + ref + ")::text AS " + quote(name)
	}
	return ref + " AS " + quote(name)
}

func selectList(collection, alias string) string {
	columns := schema.Columns[collection]
	parts := make([]string, 0, len(columns)+1)
	parts = append(parts, selectExpression(alias, "storage_key", "text"))
	for _, c := range columns {
		parts = append(parts, selectExpression(alias, c.Name, c.Kind))
	}
	return strings.Join(parts, ",")
}

func loadJSON(text string) (any, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(text)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return pythonNumbers(v), nil
}

func pythonNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, item := range x {
			x[k] = pythonNumbers(item)
		}
		return x
	case []any:
		for i, item := range x {
			x[i] = pythonNumbers(item)
		}
		return x
	case json.Number:
		s := string(x)
		if !strings.ContainsAny(s, ".eE") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
			if _, ok := new(big.Int).SetString(s, 10); ok {
				return x
			}
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return x
		}
		return f
	}
	return v
}

func decodeNumeric(text string) string {
	d, err := pydec.Parse(text)
	if err != nil {
		return text
	}
	return d.String()
}

func decodeValue(kind string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch kind {
	case "timestamptz":
		t, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("timestamp column read as %T", value)
		}
		return pytime.ISO(t), nil
	case "numeric":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("numeric column read as %T", value)
		}
		return decodeNumeric(s), nil
	case "jsonb", "text[]":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("json column read as %T", value)
		}
		return loadJSON(s)
	case "bigint":
		switch x := value.(type) {
		case int64:
			return x, nil
		case int32:
			return int64(x), nil
		}
		return nil, fmt.Errorf("bigint column read as %T", value)
	}
	return value, nil
}

func decodeRow(collection string, fields []string, values []any) (map[string]any, string, error) {
	kinds := map[string]string{}
	for _, c := range schema.Columns[collection] {
		kinds[c.Name] = c.Kind
	}
	result := make(map[string]any, len(kinds)+4)
	key := ""
	for i, name := range fields {
		if name == "storage_key" {
			key, _ = values[i].(string)
			continue
		}
		kind, ok := kinds[name]
		if !ok {
			continue
		}
		v, err := decodeValue(kind, values[i])
		if err != nil {
			return nil, "", fmt.Errorf("%s.%s: %w", collection, name, err)
		}
		result[name] = v
	}
	for name := range kinds {
		if _, ok := result[name]; !ok {
			result[name] = nil
		}
	}
	for alias, target := range schema.Aliases[collection] {
		if v, ok := result[target]; ok {
			result[alias] = v
		}
	}
	return result, key, nil
}

func collectRows(collection string, rows pgx.Rows) ([]map[string]any, []string, error) {
	defer rows.Close()
	fields := make([]string, 0, len(rows.FieldDescriptions()))
	for _, f := range rows.FieldDescriptions() {
		fields = append(fields, f.Name)
	}
	var out []map[string]any
	var keys []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		row, key, err := decodeRow(collection, fields, values)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, row)
		keys = append(keys, key)
	}
	return out, keys, rows.Err()
}
