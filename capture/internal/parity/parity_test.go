package parity

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

// Every recorded fixture gives the digests the Python reference committed
// (scripts/capture/parity.py --write).
func TestRecordedFramesMatchPython(t *testing.T) {
	paths, err := filepath.Glob(testutil.Path("frames", "*.jsonl.gz"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".jsonl.gz")
		t.Run(name, func(t *testing.T) {
			fx, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			out, err := Run(fx)
			if err != nil {
				t.Fatal(err)
			}
			var want map[string]any
			testutil.Decode(t, &want, "parity", name+".json.gz")
			got := normalizeJSON(t, Digests(out))
			for _, section := range []string{"trades", "rows", "bars", "errors"} {
				g, w := got[section].([]any), want[section].([]any)
				if len(g) != len(w) {
					t.Errorf("%s: %d items, want %d", section, len(g), len(w))
					continue
				}
				for i := range g {
					if !reflect.DeepEqual(g[i], w[i]) {
						t.Errorf("%s[%d] differs:\n got %v\nwant %v\nitem %s", section, i, g[i], w[i], item(out, section, i))
						break
					}
				}
			}
			for _, section := range []string{"fingerprints", "watermarks"} {
				if !reflect.DeepEqual(got[section], want[section]) {
					t.Errorf("%s differ:\n got %v\nwant %v", section, got[section], want[section])
				}
			}
		})
	}
}

func item(out *Output, section string, i int) string {
	var v any
	switch section {
	case "trades":
		v = out.Trades[i]
	case "rows":
		v = out.Rows[i]
	case "bars":
		v = out.Bars[i]
	default:
		v = out.Errors[i]
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func normalizeJSON(t *testing.T, v any) map[string]any {
	t.Helper()
	data := pyjson.MustDumps(v, false)
	var out map[string]any
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}
