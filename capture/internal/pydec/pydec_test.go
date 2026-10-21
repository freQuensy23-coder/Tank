package pydec

import (
	"encoding/json"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

type vectors struct {
	Literals []struct {
		Text string `json:"text"`
		Str  string `json:"str"`
		F    string `json:"f"`
	} `json:"literals"`
	Ops []struct {
		Op     string          `json:"op"`
		A      string          `json:"a"`
		B      string          `json:"b"`
		Prec   int             `json:"prec"`
		Result json.RawMessage `json:"result"`
	} `json:"ops"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	var v vectors
	if err := json.Unmarshal(testutil.Open(t, "pyvectors", "decimal.json.gz"), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestLiteralsRenderLikeCPython(t *testing.T) {
	v := loadVectors(t)
	if len(v.Literals) < 500 {
		t.Fatalf("too few literals: %d", len(v.Literals))
	}
	for _, c := range v.Literals {
		d, err := Parse(c.Text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.Text, err)
		}
		if got := d.String(); got != c.Str {
			t.Errorf("str(Decimal(%q)) = %q, want %q", c.Text, got, c.Str)
		}
		if got := d.FormatF(); got != c.F {
			t.Errorf("format(Decimal(%q), 'f') = %q, want %q", c.Text, got, c.F)
		}
	}
}

func TestArithmeticMatchesCPython(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Ops {
		a, b := MustParse(c.A), MustParse(c.B)
		var want []any
		if err := json.Unmarshal(c.Result, &want); err != nil {
			t.Fatal(err)
		}
		var got Dec
		var err error
		switch c.Op {
		case "add":
			got = Add(a, b, c.Prec)
		case "sub":
			got = Sub(a, b, c.Prec)
		case "mul":
			got = Mul(a, b, c.Prec)
		case "div":
			got, err = Div(a, b, c.Prec)
		}
		if want == nil {
			if err == nil {
				t.Errorf("%s(%s, %s) at %d: want error, got %s", c.Op, c.A, c.B, c.Prec, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s(%s, %s) at %d: %v", c.Op, c.A, c.B, c.Prec, err)
			continue
		}
		if got.String() != want[0].(string) || got.Exp() != int(want[1].(float64)) {
			t.Errorf("%s(%s, %s) at %d = %s (exp %d), want %s (exp %v)",
				c.Op, c.A, c.B, c.Prec, got, got.Exp(), want[0], want[1])
		}
	}
}

func TestParseRejectsWhatPythonRejects(t *testing.T) {
	for _, text := range []string{"", " ", "abc", "1.2.3", "--1", "1e", "e5", ".", "1__0", "_1", "0x10"} {
		if _, err := Parse(text); err == nil {
			t.Errorf("Parse(%q) accepted", text)
		}
	}
	for _, text := range []string{"Infinity", "-inf", "NaN", "sNaN"} {
		if _, err := Parse(text); err != ErrNonFinite {
			t.Errorf("Parse(%q) = %v, want ErrNonFinite", text, err)
		}
	}
}

func TestMaxMinKeepFirstOfEqualValues(t *testing.T) {
	a, b := MustParse("100.0"), MustParse("100.00")
	if Max(a, b).String() != "100.0" || Min(a, b).String() != "100.0" {
		t.Fatal("Python max/min return the first of equal values")
	}
	if Max(a, MustParse("100.01")).String() != "100.01" {
		t.Fatal("max picks the larger")
	}
}

func TestIntegerAndFloatViews(t *testing.T) {
	if !MustParse("300.000").IsInteger() || MustParse("0.5").IsInteger() {
		t.Fatal("IsInteger")
	}
	if v, ok := MustParse("1781654400123").Int64(); !ok || v != 1781654400123 {
		t.Fatal("Int64")
	}
	if MustParse("1790249583.872436").Float64() != 1790249583.872436 {
		t.Fatal("Float64")
	}
}
