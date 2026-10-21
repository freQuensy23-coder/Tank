package testutil

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func Root() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func Path(parts ...string) string {
	return filepath.Join(append([]string{Root(), "testdata"}, parts...)...)
}

func Open(t testing.TB, parts ...string) []byte {
	t.Helper()
	path := Path(parts...)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		r = gz
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func Decode(t testing.TB, target any, parts ...string) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(Open(t, parts...))))
	dec.UseNumber()
	if err := dec.Decode(target); err != nil {
		t.Fatal(err)
	}
}
