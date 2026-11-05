// Command s1-gogit is the spike S1 client harness for go-git.
//
//	s1-gogit <url> <repo-dir>
//	s1-gogit push <url> <repo-dir>
//
// It fetches +refs/heads/main:refs/remotes/origin/main from url into an
// existing repository (or, with push, pushes +refs/heads/main to url, for
// push_order.py) and exits non-zero with go-git's error on failure.
package main

import (
	"fmt"
	"os"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
)

func main() {
	if len(os.Args) == 4 && os.Args[1] == "push" {
		repo, err := git.PlainOpen(os.Args[3])
		var remote *git.Remote
		if err == nil {
			remote, err = repo.CreateRemoteAnonymous(&config.RemoteConfig{Name: "anonymous", URLs: []string{os.Args[2]}})
		}
		if err == nil {
			err = remote.Push(&git.PushOptions{
				RemoteName: "anonymous",
				RefSpecs:   []config.RefSpec{"+refs/heads/main:refs/heads/main"},
			})
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: s1-gogit <url> <repo-dir>")
		os.Exit(2)
	}
	repo, err := git.PlainOpen(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	remote, err := repo.CreateRemoteAnonymous(&config.RemoteConfig{Name: "anonymous", URLs: []string{os.Args[1]}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	err = remote.Fetch(&git.FetchOptions{
		RefSpecs: []config.RefSpec{"+refs/heads/main:refs/remotes/origin/main"},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
