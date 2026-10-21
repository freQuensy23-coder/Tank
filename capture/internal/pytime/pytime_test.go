package pytime

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

type timeVectors struct {
	Times []struct {
		Kind  string          `json:"kind"`
		Input json.RawMessage `json:"input"`
		ISO   string          `json:"iso"`
	} `json:"times"`
	Precisions []struct {
		Record  map[string]any `json:"record"`
		Field   string         `json:"field"`
		Aliases []string       `json:"aliases"`
		Fields  map[string]any `json:"fields"`
	} `json:"precisions"`
}

func TestTimesMatchPython(t *testing.T) {
	var v timeVectors
	testutil.Decode(t, &v, "pyvectors", "time.json.gz")
	for _, c := range v.Times {
		var input any
		switch c.Kind {
		case "str", "decimal":
			var s string
			if err := json.Unmarshal(c.Input, &s); err != nil {
				t.Fatal(err)
			}
			input = s
			if c.Kind == "decimal" {
				input = pydec.MustParse(s)
			}
		case "int":
			n, err := strconv.ParseInt(string(c.Input), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			input = n
		case "float":
			f, err := strconv.ParseFloat(string(c.Input), 64)
			if err != nil {
				t.Fatal(err)
			}
			input = f
		}
		got, err := ISOOf(input)
		if err != nil || got != c.ISO {
			t.Errorf("iso(%s %s) = %q (%v), want %q", c.Kind, c.Input, got, err, c.ISO)
		}
	}
}

func TestPrecisionPropagationMatchesPython(t *testing.T) {
	var v timeVectors
	testutil.Decode(t, &v, "pyvectors", "time.json.gz")
	if len(v.Precisions) == 0 {
		t.Fatal("no precision vectors")
	}
	for _, c := range v.Precisions {
		record := map[string]any{}
		for k, val := range c.Record {
			if n, ok := val.(json.Number); ok {
				i, err := n.Int64()
				if err != nil {
					t.Fatal(err)
				}
				val = i
			}
			record[k] = val
		}
		stamp, err := Propagate(record, c.Field, c.Aliases, "", "")
		if err != nil {
			t.Fatal(err)
		}
		if got := stamp.Fields(c.Field); !reflect.DeepEqual(got, c.Fields) {
			t.Errorf("propagate(%v)\n got %v\nwant %v", c.Record, got, c.Fields)
		}
	}
}

func TestFieldsHelperMatchesTimestampFields(t *testing.T) {
	got := Fields("open_time", "2026-10-21T09:00:00Z", Bucket60s)
	want := map[string]any{"open_time": "2026-10-21T09:00:00Z", "open_time_prec": "BUCKET(60s)", "open_time_prec_reason": nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	got = Fields("quote_usd_rate_as_of", nil, "")
	want = map[string]any{"quote_usd_rate_as_of": nil, "quote_usd_rate_as_of_prec": nil, "quote_usd_rate_as_of_prec_reason": "INCOMPLETE"}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}
