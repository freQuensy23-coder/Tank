package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

const ArchiveHost = "data.binance.vision"

const archivePriority = 1

func (s *Service) runArchive(ctx context.Context, row map[string]any, budgetS time.Duration, finish func(string, any, any)) {
	req := pyval.Map(row["request"])
	raw := text(req["url"])
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() != ArchiveHost || u.RawQuery != "" ||
		!strings.HasPrefix(u.Path, "/data/") || !strings.HasSuffix(u.Path, ".zip") || path.Clean(u.Path) != u.Path {
		finish("FAILED", "ARCHIVE_URL_NOT_ALLOWED", map[string]any{"url": raw})
		return
	}
	if s.opt.Fetcher == nil || s.opt.DataDir == "" {
		finish("FAILED", "FETCH_UNAVAILABLE", nil)
		return
	}
	if budgetS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budgetS)
		defer cancel()
	}
	name := path.Base(u.Path)
	got, err := s.opt.Fetcher.Fetch(ctx, "GET", raw+".CHECKSUM", nil, nil, nil, archivePriority)
	if err != nil {
		finish("FAILED", fetchReason(err), map[string]any{"error": err.Error()})
		return
	}
	if got.Status == 404 {
		finish("FAILED", "ARCHIVE_NOT_PUBLISHED", map[string]any{"status_code": int64(got.Status)})
		return
	}
	fields := strings.Fields(string(got.Body))
	if got.Status != 200 || len(fields) != 2 || len(fields[0]) != 64 {
		finish("FAILED", "ARCHIVE_CHECKSUM_UNREADABLE", map[string]any{"status_code": int64(got.Status)})
		return
	}
	if fields[1] != name {
		finish("FAILED", "ARCHIVE_CHECKSUM_NAME_MISMATCH", map[string]any{"checksum_name": fields[1]})
		return
	}
	want := strings.ToLower(fields[0])
	dest := filepath.Join(s.opt.DataDir, "archive", "binance", filepath.FromSlash(strings.TrimPrefix(u.Path, "/data/")))
	result := func(size int64, reused bool) map[string]any {
		return map[string]any{
			"path": dest, "sha256": want, "bytes": size, "checksum_text": string(got.Body),
			"fetched_at": pytime.ISO(s.clock.Now()), "reused": reused,
		}
	}
	if size, sum, err := fileSHA256(dest); err == nil && sum == want {
		finish("COMPLETE", nil, result(size, true))
		return
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		finish("FAILED", "ARCHIVE_STORE_FAILED", map[string]any{"error": err.Error()})
		return
	}
	tmp := filepath.Join(filepath.Dir(dest), "."+name+".part")
	f, err := os.Create(tmp)
	if err != nil {
		finish("FAILED", "ARCHIVE_STORE_FAILED", map[string]any{"error": err.Error()})
		return
	}
	defer func() {
		_ = os.Remove(tmp)
	}()
	h := sha256.New()
	status, size, err := s.opt.Fetcher.Download(ctx, raw, io.MultiWriter(f, h), archivePriority)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	switch {
	case err != nil:
		finish("FAILED", fetchReason(err), map[string]any{"error": err.Error()})
	case status != 200:
		finish("FAILED", "ARCHIVE_HTTP_STATUS", map[string]any{"status_code": int64(status)})
	case hexSum(h) != want:
		finish("FAILED", "ARCHIVE_CHECKSUM_MISMATCH", map[string]any{"sha256": hexSum(h), "expected": want})
	default:
		if err := os.Rename(tmp, dest); err != nil {
			finish("FAILED", "ARCHIVE_STORE_FAILED", map[string]any{"error": err.Error()})
			return
		}
		if err := os.WriteFile(dest+".CHECKSUM", got.Body, 0o644); err != nil {
			finish("FAILED", "ARCHIVE_STORE_FAILED", map[string]any{"error": err.Error()})
			return
		}
		finish("COMPLETE", nil, result(size, false))
	}
}

func hexSum(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

func fileSHA256(p string) (int64, string, error) {
	f, err := os.Open(p)
	if err != nil {
		return 0, "", err
	}
	defer func() {
		_ = f.Close()
	}()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, hexSum(h), nil
}

func fetchReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "REQUEST_BUDGET_EXHAUSTED"
	}
	return "FETCH_FAILED"
}
