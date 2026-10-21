// Package pyval gives Go code Python's view of a decoded exchange frame: Tank parses
// frames with json.loads(parse_float=Decimal), so integers are int, other numbers are
// Decimal and str() renders them the Python way. Frames here are decoded with
// json.Decoder.UseNumber, which keeps every number's text.
package pyval

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

// ErrSemantic matches every normalize.SemanticError (errors.Is).
var ErrSemantic = errors.New("semantic error")

// SemanticError is normalize.SemanticError: its text is the Python message.
type SemanticError struct{ Msg string }

func (e *SemanticError) Error() string { return e.Msg }

// Is makes errors.Is(err, ErrSemantic) true.
func (e *SemanticError) Is(target error) bool { return target == ErrSemantic }

// Semantic is a SemanticError with a formatted message.
func Semantic(format string, args ...any) error {
	return &SemanticError{Msg: fmt.Sprintf(format, args...)}
}

// Decode parses a frame as Python would see it.
func Decode(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// IsInt reports whether a JSON number is a Python int (no fraction or exponent).
func IsInt(n json.Number) bool {
	s := strings.TrimPrefix(string(n), "-")
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Str is Python str(value) for scalar frame values.
func Str(v any) string {
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
	case json.Number:
		if IsInt(x) {
			i, err := strconv.ParseInt(string(x), 10, 64)
			if err == nil {
				return strconv.FormatInt(i, 10)
			}
			return strings.TrimLeft(string(x), "+")
		}
		d, err := pydec.Parse(string(x))
		if err != nil {
			return string(x)
		}
		return d.String()
	case pydec.Dec:
		return x.String()
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		// A YAML float (configuration values): repr(float).
		switch {
		case math.IsNaN(x):
			return "nan"
		case math.IsInf(x, 1):
			return "inf"
		case math.IsInf(x, -1):
			return "-inf"
		}
		s, _ := pyjson.FloatRepr(x)
		return s
	}
	return fmt.Sprint(v)
}

// Decimal is tank.market.normalize.decimal: None and booleans are missing, the value
// goes through str() and must be finite.
func Decimal(v any) (pydec.Dec, error) {
	switch x := v.(type) {
	case nil, bool:
		return pydec.Dec{}, Semantic("missing or invalid decimal")
	case pydec.Dec:
		return x, nil
	case map[string]any, []any:
		return pydec.Dec{}, fmt.Errorf("invalid decimal %T", v)
	}
	d, err := pydec.Parse(Str(v))
	if errors.Is(err, pydec.ErrNonFinite) {
		return pydec.Dec{}, Semantic("nonfinite decimal")
	}
	if err != nil {
		return pydec.Dec{}, fmt.Errorf("decimal conversion syntax: %q", Str(v))
	}
	return d, nil
}

// Int is Python int(value) for ints, numeric strings and decimals (truncating).
func Int(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case json.Number:
		if IsInt(x) {
			return strconv.ParseInt(string(x), 10, 64)
		}
		d, err := pydec.Parse(string(x))
		if err != nil {
			return 0, err
		}
		i, ok := d.Int64()
		if !ok {
			return 0, fmt.Errorf("int overflow %s", x)
		}
		return i, nil
	case pydec.Dec:
		i, ok := x.Int64()
		if !ok {
			return 0, fmt.Errorf("int overflow %s", x)
		}
		return i, nil
	case string:
		s := strings.TrimSpace(strings.ReplaceAll(x, "_", ""))
		return strconv.ParseInt(s, 10, 64)
	case float64:
		// int(float) truncates toward zero; infinities and NaN cannot convert.
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) >= 9.223372036854775807e18 {
			return 0, fmt.Errorf("cannot convert float %v to integer", x)
		}
		return int64(x), nil
	}
	return 0, fmt.Errorf("invalid literal for int(): %v", v)
}

// Map is v as a JSON object, nil otherwise.
func Map(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// List is v as a JSON array, nil otherwise.
func List(v any) []any {
	l, _ := v.([]any)
	return l
}

// Get is m.get(key) (nil when absent).
func Get(m map[string]any, key string) any { return m[key] }

// GetOr is m.get(key, fallback): the fallback only when the key is absent.
func GetOr(m map[string]any, key string, fallback any) any {
	if v, ok := m[key]; ok {
		return v
	}
	return fallback
}

// Has reports key in m.
func Has(m map[string]any, key string) bool { _, ok := m[key]; return ok }

// Need is m[key] with Python's KeyError as an error.
func Need(m map[string]any, key string) (any, error) {
	v, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("KeyError: %q", key)
	}
	return v, nil
}

// Truthy is Python truthiness of a frame value.
func Truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		d, err := pydec.Parse(string(x))
		return err == nil && !d.IsZero()
	case map[string]any:
		return len(x) > 0
	case []any:
		return len(x) > 0
	case int:
		return x != 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	case pydec.Dec:
		return !x.IsZero()
	}
	return true
}

// Equal is Python == for frame scalars (ints, numbers and strings compare by kind).
func Equal(a, b any) bool {
	an, aNum := a.(json.Number)
	bn, bNum := b.(json.Number)
	if aNum && bNum {
		da, ea := pydec.Parse(string(an))
		db, eb := pydec.Parse(string(bn))
		return ea == nil && eb == nil && da.Equal(db)
	}
	if aNum != bNum {
		return false
	}
	return a == b
}
