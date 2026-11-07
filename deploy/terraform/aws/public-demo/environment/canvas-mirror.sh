# shellcheck shell=sh
# The canvas mirror sidecar (place.tf): once aspen-place answers after a
# start, and then every MIRROR_INTERVAL seconds (15 minutes by default),
# fetch the canvas's main
# from aspen-place in the same task and, when it moved, push it to the
# GitHub repository MIRROR_REPO: one push per run. The push is forced,
# because place starts main's history again from one root commit whenever
# it outgrows PLACE_HISTORY_MAX_BYTES; the GitHub repository is a read-only
# copy whose only writer is this sidecar. It is forced with a lease: only
# over the main this sidecar last pushed (read from GitHub at start), or a
# main from place's own history (another place task's mirror pushed it,
# from_place), so a write by anyone else is reported, never overwritten.
#
# MIRROR_CREDENTIAL is either an SSH deploy key (OpenSSH private key; SSH
# goes over port 443, ssh.github.com, the only egress the place task has)
# or a token with write access to that one repository (a fine-grained
# personal access token or an installation token), used over HTTPS.
set -eu
apk add --no-cache --quiet git openssh-client
umask 077
case "$MIRROR_CREDENTIAL" in
  -----BEGIN*)
    mkdir -p "$HOME/.ssh"
    printf '%s\n' "$MIRROR_CREDENTIAL" > "$HOME/.ssh/id_ed25519"
    # GitHub's published host key (https://api.github.com/meta, ssh_keys).
    printf '%s\n' '[ssh.github.com]:443 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl' > "$HOME/.ssh/known_hosts"
    export GIT_SSH_COMMAND="ssh -p 443 -o StrictHostKeyChecking=yes -o BatchMode=yes -o IdentitiesOnly=yes -i $HOME/.ssh/id_ed25519"
    remote="git@ssh.github.com:$MIRROR_REPO.git"
    ;;
  *)
    # The token goes to git through a credential helper reading a file,
    # never on a command line or in a URL.
    printf 'protocol=https\nhost=github.com\nusername=x-access-token\npassword=%s\n' "$MIRROR_CREDENTIAL" > "$HOME/.mirror-credential"
    git config --global credential.helper "!f() { cat \"$HOME/.mirror-credential\"; }; f"
    remote="https://github.com/$MIRROR_REPO.git"
    ;;
esac
unset MIRROR_CREDENTIAL
git init --quiet --bare "${TMPDIR:-/tmp}/canvas.git"
cd "${TMPDIR:-/tmp}/canvas.git"
# This sidecar starts with place, which listens only once it has loaded the
# canvas. Ask place for main every MIRROR_START_RETRY seconds (5) until it
# names one, for at most one MIRROR_INTERVAL, so the first push after a
# restart comes seconds after place is up rather than a whole interval
# later. Each ask is cut off after 10 seconds, so a place that accepts the
# connection and then stalls cannot hold the wait past its interval.
retry=${MIRROR_START_RETRY:-5}
case "$retry" in
  '' | *[!0-9]*) retry=0 ;;
esac
if [ "$retry" -le 0 ]; then
  echo "canvas-mirror: MIRROR_START_RETRY=${MIRROR_START_RETRY:-} is not a positive number of seconds; using 5"
  retry=5
fi
started=$(date +%s)
waiting=no
until [ -n "$(timeout 10 git ls-remote "$MIRROR_SOURCE" refs/heads/main 2>/dev/null)" ]; do
  waited=$(($(date +%s) - started))
  if [ "$waited" -ge "$MIRROR_INTERVAL" ]; then
    echo "canvas-mirror: place still does not answer at $MIRROR_SOURCE after ${waited}s"
    break
  fi
  if [ "$waiting" = no ]; then
    echo "canvas-mirror: waiting for place at $MIRROR_SOURCE"
    waiting=yes
  fi
  sleep "$retry"
done
# from_place <commit> <tip>: whether <commit> is a main of place's, that is
# in <tip>'s history or in the history before one of place's resets. Place
# starts main again from a root commit whose subject names the main it
# replaced ("Start the canvas again from main at <old>") and keeps <old>
# fetchable, so follow those roots back, at most 8 resets. A root that is
# not place's (someone else's history) ends the walk.
from_place() {
  head=$2
  hops=0
  while :; do
    git merge-base --is-ancestor "$1" "$head" 2>/dev/null && return 0
    [ "$hops" -lt 8 ] || return 1
    root=$(git rev-list --first-parent "$head" | tail -n 1)
    old=$(git log -1 --format=%s "$root" | sed -n 's/^Start the canvas again from main at \([0-9a-f]\{40,64\}\)$/\1/p')
    [ -n "$old" ] || return 1
    git cat-file -e "$old^{commit}" 2>/dev/null ||
      git fetch --quiet "$MIRROR_SOURCE" "$old" 2>/dev/null || return 1
    head=$old
    hops=$((hops + 1))
  done
}
# GitHub's main as this sidecar last left it ("" if it has none), once
# read.
pushed=""
known=no
while :; do
  if [ "$known" = no ] && remote_main=$(git ls-remote "$remote" refs/heads/main); then
    pushed=$(printf '%s' "$remote_main" | cut -f1)
    known=yes
  fi
  if [ "$known" = no ]; then
    echo "canvas-mirror: could not read main from $MIRROR_REPO"
  elif git fetch --quiet "$MIRROR_SOURCE" +refs/heads/main:refs/heads/main; then
    tip=$(git rev-parse refs/heads/main)
    if [ "$tip" != "$pushed" ]; then
      if git push --quiet --force-with-lease="refs/heads/main:$pushed" "$remote" refs/heads/main:refs/heads/main; then
        echo "canvas-mirror: pushed main $tip to $MIRROR_REPO"
        pushed=$tip
      else
        echo "canvas-mirror: push of $tip to $MIRROR_REPO failed (GitHub refused, or its main is not ${pushed:-absent}, as this mirror left it)"
        # The push may have landed all the same; or another place task's
        # mirror pushed first (during a start-first deploy the old task and
        # the new one run side by side for a minute). A main that came from
        # place itself (in place's history, also across its history resets,
        # from_place) is taken as the lease, and the next run pushes over
        # it. Any other main someone else wrote stays theirs until the
        # sidecar restarts.
        now=$(git ls-remote "$remote" refs/heads/main | cut -f1)
        if [ "$now" = "$tip" ]; then
          pushed=$tip
        elif [ -n "$now" ] && from_place "$now" "$tip"; then
          echo "canvas-mirror: $MIRROR_REPO's main $now is an earlier main of place's; pushing over it next run"
          pushed=$now
        fi
      fi
    fi
  else
    echo "canvas-mirror: fetch from $MIRROR_SOURCE failed"
  fi
  sleep "$MIRROR_INTERVAL"
done
