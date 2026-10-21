package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func pythonVersion(t *testing.T, root string) string {
	t.Helper()
	cmd := exec.Command("uv", "run", "--no-sync", "python", "-c", `import sys
from pathlib import Path
from tank.settings import _CONFIG_SUFFIXES, _capture_tree, _config_version, _digest
root = Path(sys.argv[1])
print(_config_version(root, _digest(_capture_tree(root, _CONFIG_SUFFIXES))), end="")`, root)
	cmd.Dir = repoRoot()
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("tank Python environment unavailable: %v", err)
	}
	return string(out)
}

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.email=t@example.com", "-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
}

func TestCaptureTreeAndDigest(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "a.yaml", "a: 1\n")
	write(t, dir, "sub/b.json", "{}")
	write(t, dir, "sub/notes.md", "x")
	write(t, dir, "ignored.txt", "x")
	write(t, dir, ".hidden.yaml", "x")
	write(t, dir, ".git/config.yaml", "x")
	write(t, dir, "__pycache__/c.yaml", "x")
	captured, err := CaptureTree(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for name := range captured {
		names = append(names, name)
	}
	if strings.Join(sorted(names), ",") != "a.yaml,sub/b.json,sub/notes.md" {
		t.Fatalf("captured %v", names)
	}

	version, err := ConfigVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := pythonVersion(t, dir); version != want {
		t.Fatalf("version %q, Python %q", version, want)
	}
	if err := os.Symlink(filepath.Join(dir, "a.yaml"), filepath.Join(dir, "link.yaml")); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureTree(dir); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("a versioned symlink must fail: %v", err)
	}
	if _, err := CaptureTree(t.TempDir()); err == nil {
		t.Fatal("an empty configuration must fail")
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestManifestVersion(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "registry.yaml", "tokens: []\n")
	captured, err := CaptureTree(dir)
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest(captured)
	commit := strings.Repeat("ab", 20)
	cases := []struct{ manifest, want string }{
		{`{"git_commit":"` + commit + `","content_sha256":"` + digest + `","git_dirty":false}`, commit},
		{`{"git_commit":"` + commit + `","content_sha256":"` + digest + `","git_dirty":true}`, commit + "-dirty." + digest},
		{`{"git_commit":"` + commit + `","content_sha256":"other","git_dirty":false}`, commit + "-dirty." + digest},
		{`{"git_commit":"` + commit + `","content_sha256":"` + digest + `"}`, commit + "-dirty." + digest},
	}
	for _, c := range cases {
		write(t, dir, ".tank-config-version.json", c.manifest)
		got, err := ConfigVersion(dir)
		if err != nil || got != c.want {
			t.Errorf("manifest %s: %q %v, want %q", c.manifest, got, err, c.want)
		}
		if want := pythonVersion(t, dir); got != want {
			t.Errorf("manifest %s: %q, Python %q", c.manifest, got, want)
		}
	}
	write(t, dir, ".tank-config-version.json", `{"git_commit":"abc","content_sha256":"`+digest+`","git_dirty":false}`)
	if _, err := ConfigVersion(dir); err == nil {
		t.Fatal("a short commit must fail")
	}
}

func TestGitVersion(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git unavailable")
	}
	repo := t.TempDir()
	gitRun(t, repo, "init", "-q")
	config := filepath.Join(repo, "config")
	write(t, config, "registry.yaml", "tokens: []\n")
	write(t, config, "sub/semantics.yaml", "metrics: []\n")
	write(t, repo, "outside.yaml", "x: 1\n")
	gitRun(t, repo, "add", ".")
	gitRun(t, repo, "commit", "-q", "-m", "c")
	clean, err := ConfigVersion(config)
	if err != nil {
		t.Fatal(err)
	}
	if !fullCommit(clean) {
		t.Fatalf("a clean committed config is its commit: %q", clean)
	}
	if want := pythonVersion(t, config); clean != want {
		t.Fatalf("clean %q, Python %q", clean, want)
	}
	write(t, config, "registry.yaml", "tokens: [{token_id: a}]\n")
	dirty, err := ConfigVersion(config)
	if err != nil || !strings.HasPrefix(dirty, clean+"-dirty.") {
		t.Fatalf("an edited config is dirty: %q %v", dirty, err)
	}
	if want := pythonVersion(t, config); dirty != want {
		t.Fatalf("dirty %q, Python %q", dirty, want)
	}
}
