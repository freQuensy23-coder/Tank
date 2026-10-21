// Command capture-parity runs one recorded fixture through tank-capture's ports and
// prints the full output; scripts/capture/parity.py --go diffs it against Python.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/freQuensy23-coder/Tank/capture/internal/parity"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: capture-parity FIXTURE.jsonl.gz")
		os.Exit(2)
	}
	fx, err := parity.Load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out, err := parity.Run(fx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
