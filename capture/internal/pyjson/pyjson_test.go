package pyjson

import (
	"encoding/json"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

func TestDumpsMatchesPython(t *testing.T) {
	var v struct {
		Values []struct {
			Value json.RawMessage `json:"value"`
			ASCII string          `json:"ascii"`
			UTF8  string          `json:"utf8"`
		} `json:"values"`
		Floats [][2]json.RawMessage `json:"floats"`
	}
	testutil.Decode(t, &v, "pyvectors", "json.json.gz")
	if len(v.Values) < 100 {
		t.Fatal("too few vectors")
	}
	for _, c := range v.Values {
		var value any
		dec := json.NewDecoder(bytesReader(c.Value))
		dec.UseNumber()
		if err := dec.Decode(&value); err != nil {
			t.Fatal(err)
		}
		if got := string(MustDumps(value, true)); got != c.ASCII {
			t.Errorf("ascii\n got %s\nwant %s", got, c.ASCII)
		}
		if got := string(MustDumps(value, false)); got != c.UTF8 {
			t.Errorf("utf8\n got %s\nwant %s", got, c.UTF8)
		}
	}
	for _, pair := range v.Floats {
		var f float64
		var want string
		if err := json.Unmarshal(pair[0], &f); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(pair[1], &want); err != nil {
			t.Fatal(err)
		}
		got, err := FloatRepr(f)
		if err != nil || got != want {
			t.Errorf("repr(%v) = %q, want %q", f, got, want)
		}
	}
}

func TestJSONNumbersFollowPythonParseFloatDecimal(t *testing.T) {
	value := map[string]any{"i": json.Number("12"), "d": json.Number("1e-05"), "f": json.Number("0.10")}
	if got := string(MustDumps(value, true)); got != `{"d":"0.00001","f":"0.10","i":12}` {
		t.Fatal(got)
	}
}
