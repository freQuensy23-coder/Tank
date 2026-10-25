package config

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type pythonIdentityReply struct {
	Accepted bool
	Version  *string
}

func pythonDeclaredVersion(t *testing.T, root string, build *string) pythonIdentityReply {
	t.Helper()
	encoded, err := json.Marshal(build)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("uv", "run", "--no-sync", "python", "-c", `import json
import sys
from pathlib import Path
from pydantic import BaseModel, ConfigDict
from tank.settings import _CONFIG_SUFFIXES, _capture_tree, _config_version, _digest

if sys.version_info[:3] != (3, 13, 13):
    raise RuntimeError("Python 3.13.13 is required for capture identity parity")

class IdentityReply(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    accepted: bool
    version: str | None

root = Path(sys.argv[1])
try:
    version = _config_version(json.loads(sys.argv[2]), _digest(_capture_tree(root, _CONFIG_SUFFIXES)))
except ValueError:
    reply = IdentityReply(accepted=False, version=None)
else:
    reply = IdentityReply(accepted=True, version=version)
print(reply.model_dump_json(), end="")
`, root, string(encoded))
	cmd.Dir = repoRoot()
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("mandatory Python identity peer unavailable: %v", err)
	}
	var wire struct {
		Accepted *bool           `json:"accepted"`
		Version  json.RawMessage `json:"version"`
	}
	decoder := json.NewDecoder(bytes.NewReader(out))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		t.Fatal(err)
	}
	if wire.Accepted == nil || wire.Version == nil {
		t.Fatal("Python identity peer omitted required result fields")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("Python identity peer contains trailing output: %v", err)
	}
	var version *string
	if err := json.Unmarshal(wire.Version, &version); err != nil {
		t.Fatal(err)
	}
	if *wire.Accepted != (version != nil) {
		t.Fatal("Python identity peer has inconsistent acceptance/version fields")
	}
	return pythonIdentityReply{Accepted: *wire.Accepted, Version: version}
}

func pythonVersion(t *testing.T, root string) string {
	t.Helper()
	reply := pythonDeclaredVersion(t, root, nil)
	if !reply.Accepted || reply.Version == nil {
		t.Fatal("Python rejected an absent development build declaration")
	}
	return *reply.Version
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
	for build, want := range map[string]string{"": "dev." + digest, "dev": "dev." + digest, commit: commit} {
		Build = build
		if got, err := ConfigVersion(dir); err != nil || got != want {
			t.Errorf("build %q: %q %v, want %q", build, got, err, want)
		}
	}
	for _, build := range []string{"abc", "dev ", " dev", "DEV", strings.Repeat("A", 40), strings.Repeat("a", 39), strings.Repeat("a", 41), "dev." + digest} {
		Build = build
		if _, err := ConfigVersion(dir); err == nil {
			t.Errorf("invalid declared build %q was silently treated as development", build)
		}
	}
	Build = "dev"
	previous, err := ConfigVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "registry.yaml", "tokens: []\nversion: changed\n")
	current, err := ConfigVersion(dir)
	if err != nil || current == previous || !strings.HasPrefix(current, "dev.") {
		t.Fatalf("explicit development identity did not track changed configuration: previous=%q current=%q error=%v", previous, current, err)
	}

}

func TestDeclaredBuildPolicyMatchesPython(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "registry.yaml", "tokens: []\n")
	defer func(saved string) { Build = saved }(Build)
	for _, test := range []struct {
		name  string
		build string
	}{
		{"absent", ""},
		{"development", "dev"},
		{"commit", strings.Repeat("ab", 20)},
		{"short", "abc"},
		{"uppercase", strings.Repeat("A", 40)},
		{"leading-space", " dev"},
		{"trailing-space", "dev "},
		{"uppercase-development", "DEV"},
		{"wrong-length", strings.Repeat("a", 41)},
		{"digest-is-not-a-declaration", "dev." + strings.Repeat("a", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			Build = test.build
			var declared *string
			if test.build != "" {
				declared = &test.build
			}
			peer := pythonDeclaredVersion(t, dir, declared)
			version, err := ConfigVersion(dir)
			if peer.Accepted != (err == nil) {
				t.Fatalf("Go/Python identity policy differs: Go=%q/%v Python=%+v", version, err, peer)
			}
			if err == nil && (peer.Version == nil || version != *peer.Version) {
				t.Fatalf("Go/Python identity differs: Go=%q Python=%+v", version, peer)
			}
		})
	}
	empty := ""
	if peer := pythonDeclaredVersion(t, dir, &empty); peer.Accepted {
		t.Fatal("Python accepted an explicitly empty declaration; Docker must reject the same input")
	}
}
