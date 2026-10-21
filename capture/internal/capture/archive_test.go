package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

type archiveSite struct {
	checksum  string
	zip       []byte
	downloads int
}

func (a *archiveSite) Fetch(_ context.Context, _, _ string, _, _ map[string]string, _ any, _ int) (*rest.Fetched, error) {
	return &rest.Fetched{Status: 200, Body: []byte(a.checksum)}, nil
}

func (a *archiveSite) Download(_ context.Context, _ string, dst io.Writer, _ int) (int, int64, error) {
	a.downloads++
	n, err := dst.Write(a.zip)
	return 200, int64(n), err
}

func TestArchiveFetchStoresVerifiedFileOnce(t *testing.T) {
	dir := t.TempDir()
	zip := []byte("PK\x03\x04 archive bytes")
	sum := sha256.Sum256(zip)
	name := "BTCUSDT-1m-2026-10-01.zip"
	site := &archiveSite{checksum: hex.EncodeToString(sum[:]) + "  " + name + "\n", zip: zip}
	s := &Service{opt: Options{Fetcher: site, DataDir: dir}, clock: NewSystemClock()}
	row := map[string]any{"request": map[string]any{
		"url": "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/" + name}}
	run := func() (string, any, map[string]any) {
		var status string
		var reason, result any
		s.runArchive(context.Background(), row, 0, func(st string, r, res any) { status, reason, result = st, r, res })
		out, _ := result.(map[string]any)
		return status, reason, out
	}
	status, reason, result := run()
	want := filepath.Join(dir, "archive", "binance", "spot", "daily", "klines", "BTCUSDT", "1m", name)
	if status != "COMPLETE" || result["path"] != want || result["reused"] != false {
		t.Fatalf("first fetch: %s %v %v", status, reason, result)
	}
	if got, _ := os.ReadFile(want); string(got) != string(zip) {
		t.Fatal("stored bytes differ")
	}
	if status, _, result = run(); status != "COMPLETE" || result["reused"] != true || site.downloads != 1 {
		t.Fatalf("second fetch downloaded again: %s %v downloads=%d", status, result, site.downloads)
	}
	// A corrupted download is never kept.
	os.Remove(want)
	site.zip = []byte("tampered")
	if status, reason, _ = run(); status != "FAILED" || reason != "ARCHIVE_CHECKSUM_MISMATCH" {
		t.Fatalf("tampered: %s %v", status, reason)
	}
	if _, err := os.Stat(want); !os.IsNotExist(err) {
		t.Fatal("tampered file stored")
	}
	row["request"] = map[string]any{"url": "https://api.binance.com/api/v3/klines"}
	if status, reason, _ = run(); status != "FAILED" || reason != "ARCHIVE_URL_NOT_ALLOWED" {
		t.Fatalf("non-archive url: %s %v", status, reason)
	}
}
