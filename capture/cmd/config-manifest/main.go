package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/freQuensy23-coder/Tank/capture/internal/config"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: config-manifest <config dir> <git commit>")
		os.Exit(2)
	}
	root, commit := os.Args[1], os.Args[2]
	captured, err := config.CaptureTree(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	manifest := map[string]any{"git_commit": commit, "content_sha256": config.Digest(captured), "git_dirty": false}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(root, ".tank-config-version.json"), append(data, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if version, err := config.ConfigVersion(root); err != nil || version != commit {
		fmt.Fprintln(os.Stderr, "manifest does not yield the commit:", version, err)
		os.Exit(1)
	}
}
