package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
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
	if Build != "" && Build != "dev" && !fullCommit(Build) {
		return "", errors.New("build identity must be a full lowercase commit SHA or explicit dev")
	}
	captured, err := CaptureTree(root)
	if err != nil {
		return "", err
	}
	if fullCommit(Build) {
		return Build, nil
	}
	return "dev." + Digest(captured), nil
}
