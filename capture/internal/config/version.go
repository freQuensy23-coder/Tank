package config

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

var Build string

func CodeVersion() string {
	if Build != "" {
		return "tank-capture/" + Build
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		revision, modified := "", false
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
		if revision != "" {
			if modified {
				revision += "-dirty"
			}
			return "tank-capture/" + revision
		}
	}
	return "tank-capture/dev"
}

var versionedSuffixes = map[string]bool{".yaml": true, ".yml": true, ".json": true, ".md": true}

func hidden(relative string) bool {
	for _, part := range strings.Split(relative, "/") {
		if strings.HasPrefix(part, ".") || part == "__pycache__" {
			return true
		}
	}
	return false
}

func CaptureTree(root string) (map[string][]byte, error) {
	captured := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative := filepath.ToSlash(rel)
		if hidden(relative) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() || !versionedSuffixes[filepath.Ext(path)] {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("Versioned input must not be a symlink: %s", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		captured[relative] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(captured) == 0 {
		return nil, fmt.Errorf("No versioned inputs in %s", root)
	}
	return captured, nil
}

func Digest(captured map[string][]byte) string {
	paths := make([]string, 0, len(captured))
	for p := range captured {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write(captured[p])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func fullCommit(commit string) bool {
	if len(commit) != 40 {
		return false
	}
	for _, c := range commit {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

func ConfigVersion(root string) (string, error) {
	captured, err := CaptureTree(root)
	if err != nil {
		return "", err
	}
	digest := Digest(captured)
	manifest, err := os.ReadFile(filepath.Join(root, ".tank-config-version.json"))
	if err == nil {
		var m map[string]any
		if err := json.Unmarshal(manifest, &m); err != nil {
			return "", fmt.Errorf("configuration manifest: %w", err)
		}
		commit, ok := m["git_commit"].(string)
		if !ok || !fullCommit(commit) {
			return "", errors.New("Configuration manifest requires a full Git commit")
		}
		dirty, isBool := m["git_dirty"].(bool)
		if content, _ := m["content_sha256"].(string); content == digest && isBool && !dirty {
			return commit, nil
		}
		return commit + "-dirty." + digest, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	commit, dirty, err := gitIdentity(root, digest)
	if err != nil {
		return "uncommitted." + digest, nil
	}
	if dirty {
		return commit + "-dirty." + digest, nil
	}
	return commit, nil
}

func git(dir string, input []byte, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"--literal-pathspecs", "-C", dir}, args...)...)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	return out, nil
}

func gitIdentity(root, digest string) (string, bool, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", false, err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	head, err := git(root, nil, "rev-parse", "HEAD")
	if err != nil {
		return "", false, err
	}
	commit := strings.TrimSpace(string(head))
	top, err := git(root, nil, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", false, err
	}
	repository := strings.TrimSpace(string(top))
	if resolved, err := filepath.EvalSymlinks(repository); err == nil {
		repository = resolved
	}
	prefix, err := filepath.Rel(repository, root)
	if err != nil {
		return "", false, err
	}
	prefix = filepath.ToSlash(prefix)
	tree, err := git(repository, nil, "ls-tree", "-r", "-z", "--full-tree", commit, "--", prefix)
	if err != nil {
		return "", false, err
	}
	type object struct{ name, oid string }
	var objects []object
	for _, row := range bytes.Split(tree, []byte{0}) {
		if len(row) == 0 {
			continue
		}
		metadata, name, ok := bytes.Cut(row, []byte{'\t'})
		if !ok {
			return commit, true, nil
		}
		fields := strings.Fields(string(metadata))
		if len(fields) != 3 {
			return commit, true, nil
		}
		mode, kind, oid := fields[0], fields[1], fields[2]
		relative := string(name)
		if prefix != "." {
			relative = strings.TrimPrefix(relative, prefix+"/")
		}
		if !versionedSuffixes[filepath.Ext(relative)] || hidden(relative) {
			continue
		}
		if kind != "blob" || mode == "120000" {
			return commit, true, nil
		}
		objects = append(objects, object{relative, oid})
	}
	payload := map[string][]byte{}
	if len(objects) > 0 {
		var input bytes.Buffer
		for _, o := range objects {
			input.WriteString(o.oid + "\n")
		}
		out, err := git(root, input.Bytes(), "cat-file", "--batch")
		if err != nil {
			return "", false, err
		}
		reader := bufio.NewReader(bytes.NewReader(out))
		for _, o := range objects {
			header, err := reader.ReadString('\n')
			if err != nil {
				return commit, true, nil
			}
			parts := strings.Fields(header)
			if len(parts) != 3 || parts[0] != o.oid || parts[1] != "blob" {
				return commit, true, nil
			}
			size, err := strconv.Atoi(parts[2])
			if err != nil {
				return commit, true, nil
			}
			content := make([]byte, size)
			if _, err := io.ReadFull(reader, content); err != nil {
				return commit, true, nil
			}
			if b, err := reader.ReadByte(); err != nil || b != '\n' {
				return commit, true, nil
			}
			payload[o.name] = content
		}
	}
	status, err := git(root, nil, "status", "--porcelain", "--", ".")
	if err != nil {
		return "", false, err
	}
	dirty := len(bytes.TrimSpace(status)) > 0
	return commit, dirty || Digest(payload) != digest, nil
}
