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
print(_config_version(None, _digest(_capture_tree(root, _CONFIG_SUFFIXES))), end="")`, root)
	cmd.Dir = repoRoot()
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("tank Python environment unavailable: %v", err)
	}
	return string(out)
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

func TestBuildVersion(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "registry.yaml", "tokens: []\n")
	captured, err := CaptureTree(dir)
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest(captured)
	defer func(saved string) { Build = saved }(Build)
	commit := strings.Repeat("ab", 20)
	for build, want := range map[string]string{"": "dev." + digest, "dev": "dev." + digest, commit: commit, "abc": "dev." + digest} {
		Build = build
		if got, err := ConfigVersion(dir); err != nil || got != want {
			t.Errorf("build %q: %q %v, want %q", build, got, err, want)
		}
	}
}
