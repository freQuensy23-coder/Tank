package pyval

import (
	"math"
	"testing"
)

// Configuration floats (yaml.safe_load) behave as Python's str(x), Decimal(str(x)) and
// int(x); the expected values are CPython's.
func TestFloatsLikePython(t *testing.T) {
	cases := []struct {
		x            float64
		str, decimal string
		integer      int64
	}{
		{0.1, "0.1", "0.1", 0},
		{1e-05, "1e-05", "0.00001", 0},
		{1e16, "1e+16", "1E+16", 10000000000000000},
		{8.0, "8.0", "8.0", 8},
		{123456789012345678.0, "1.2345678901234568e+17", "1.2345678901234568E+17", 123456789012345680},
		{-2.7, "-2.7", "-2.7", -2},
		{1.5, "1.5", "1.5", 1},
		{2.5e-07, "2.5e-07", "2.5E-7", 0},
		{math.Copysign(0, -1), "-0.0", "-0.0", 0},
	}
	for _, c := range cases {
		if got := Str(c.x); got != c.str {
			t.Errorf("str(%v) = %q, want %q", c.x, got, c.str)
		}
		d, err := Decimal(c.x)
		if err != nil || d.String() != c.decimal {
			t.Errorf("Decimal(str(%v)) = %q %v, want %q", c.x, d.String(), err, c.decimal)
		}
		if got, err := Int(c.x); err != nil || got != c.integer {
			t.Errorf("int(%v) = %d %v, want %d", c.x, got, err, c.integer)
		}
	}
	for _, x := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		if _, err := Decimal(x); err == nil {
			t.Errorf("Decimal(%v) must fail as normalize.decimal does", x)
		}
		if _, err := Int(x); err == nil {
			t.Errorf("int(%v) must fail", x)
		}
	}
	if Str(math.Inf(-1)) != "-inf" || Str(math.NaN()) != "nan" {
		t.Error("non-finite str()")
	}
	if !Truthy(0.5) || Truthy(0.0) || !Truthy(math.NaN()) {
		t.Error("float truthiness")
	}
	if Str(8) != "8" || !Truthy(8) || Truthy(0) {
		t.Error("YAML int")
	}
}
