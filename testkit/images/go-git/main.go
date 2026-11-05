// Client-matrix scenario for go-git.
//
// usage: aspen-scenario <url> <branch> [<branch-url>]
//
// Same steps and output as ../git/scenario.sh, minus `fsck` (run-matrix.sh
// runs it with stock Git afterwards). The second clone goes through
// <branch-url> (default <url>): go-git fetches with protocol v0, so on Aspen
// only the branch-scoped URL advertises <branch>.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func main() {
	if len(os.Args) != 3 && len(os.Args) != 4 {
		fail(errors.New("usage: aspen-scenario <url> <branch> [<branch-url>]"))
	}
	url, branch := os.Args[1], os.Args[2]
	branchURL := url
	if len(os.Args) == 4 {
		branchURL = os.Args[3]
	}
	work, err := os.MkdirTemp("", "go-git-matrix-")
	check(err)
	defer os.RemoveAll(work)

	fmt.Printf("client go-git %s\n", goGitVersion())

	a, err := git.PlainClone(filepath.Join(work, "a"), false, &git.CloneOptions{URL: url})
	check(err)
	head, err := a.Head()
	check(err)
	fmt.Printf("cloned %s\n", head.Hash())

	wt, err := a.Worktree()
	check(err)
	check(os.WriteFile(filepath.Join(work, "a", "matrix.txt"), []byte("pushed by go-git\n"), 0o644))
	_, err = wt.Add("matrix.txt")
	check(err)
	sig := &object.Signature{Name: "Aspen Matrix", Email: "matrix@aspen.invalid", When: time.Now()}
	commit, err := wt.Commit("matrix: "+branch, &git.CommitOptions{Author: sig, Committer: sig})
	check(err)
	spec := config.RefSpec(head.Name().String() + ":refs/heads/" + branch)
	check(a.Push(&git.PushOptions{RemoteName: "origin", RefSpecs: []config.RefSpec{spec}}))
	fmt.Printf("pushed %s\n", commit)

	b, err := git.PlainClone(filepath.Join(work, "b"), false, &git.CloneOptions{
		URL:           branchURL,
		ReferenceName: plumbing.NewBranchReferenceName(branch),
		SingleBranch:  true,
	})
	check(err)
	bHead, err := b.Head()
	check(err)
	fmt.Printf("fetched %s\n", bHead.Hash())

	err = a.Fetch(&git.FetchOptions{RemoteName: "origin"})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		fail(err)
	}
}

func goGitVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/go-git/go-git/v5" {
				return dep.Version
			}
		}
	}
	return "(unknown version)"
}

func check(err error) {
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "go-git scenario:", err)
	os.Exit(1)
}
