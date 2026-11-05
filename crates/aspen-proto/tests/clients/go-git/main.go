// A go-git fetch client for aspen-proto's v0 upload-pack interop tests
// (crates/aspen-proto/tests/interop/upload_v0.rs). go-git has no protocol
// v2, so every fetch it makes is v0.
//
// usage:
//
//	aspen-v0-client clone <url> <dir> [depth]
//	aspen-v0-client fetch <dir> [depth]
//
// Build with `go build -o <path> .` in this directory and point
// ASPEN_GO_GIT_CLIENT at the binary.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func depthArg(args []string, i int) (int, error) {
	if len(args) <= i {
		return 0, nil
	}
	return strconv.Atoi(args[i])
}

func run(args []string) error {
	if len(args) < 2 {
		return errors.New("usage: aspen-v0-client clone <url> <dir> [depth] | fetch <dir> [depth]")
	}
	switch args[0] {
	case "clone":
		if len(args) < 3 {
			return errors.New("clone needs <url> <dir>")
		}
		depth, err := depthArg(args, 3)
		if err != nil {
			return err
		}
		_, err = git.PlainClone(args[2], false, &git.CloneOptions{URL: args[1], Depth: depth})
		return err
	case "fetch":
		depth, err := depthArg(args, 2)
		if err != nil {
			return err
		}
		r, err := git.PlainOpen(args[1])
		if err != nil {
			return err
		}
		err = r.Fetch(&git.FetchOptions{
			RemoteName: "origin",
			Depth:      depth,
			RefSpecs:   []config.RefSpec{"+refs/heads/*:refs/remotes/origin/*"},
		})
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			return nil
		}
		return err
	}
	return fmt.Errorf("unknown command %q", args[0])
}
