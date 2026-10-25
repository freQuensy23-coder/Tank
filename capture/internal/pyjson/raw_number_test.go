package pyjson

import (
	"encoding/json"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

func TestExactNumberPreservesFrozenPythonFloatLexemesWithoutFloatDecoding(t *testing.T) {
	var vectors struct {
		Floats [][2]json.RawMessage `json:"floats"`
	}
	testutil.Decode(t, &vectors, "pyvectors", "json.json.gz")
	if len(vectors.Floats) < 100 {
		t.Fatal("Frozen numeric corpus is incomplete")
	}
	for _, pair := range vectors.Floats {
		var expected string
		if err := json.Unmarshal(pair[1], &expected); err != nil {
			t.Fatal(err)
		}
		actual, err := ExactJSONNumber(pair[0])
		if err != nil || string(actual) != expected {
			t.Errorf("Exact raw %s became %s, expected %s: %v", pair[0], actual, expected, err)
		}
	}
}

func TestExactNumberRetainsArbitraryPrecisionAndNumericKind(t *testing.T) {
	cases := []struct{ raw, expected string }{
		{"0.123456789012345678901", "0.123456789012345678901"},
		{"123456789012345678901234567890", "123456789012345678901234567890"},
		{"1.000", "1.0"}, {"1", "1"}, {"-0.00", "-0.0"},
		{"1.234567890123456789e-9", "1.234567890123456789e-09"},
	}
	for _, candidate := range cases {
		actual, err := ExactJSONNumber([]byte(candidate.raw))
		if err != nil || string(actual) != candidate.expected {
			t.Errorf("%s became %s, expected %s: %v", candidate.raw, actual, candidate.expected, err)
		}
	}
}
