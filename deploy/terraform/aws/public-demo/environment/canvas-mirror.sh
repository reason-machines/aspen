# shellcheck shell=sh
# The canvas mirror sidecar (place.tf): at start and then every
# MIRROR_INTERVAL seconds (15 minutes by default), fetch the canvas's main
# from aspen-place in the same task and, when it moved, push it to the
# GitHub repository MIRROR_REPO: one fast-forward push per run. The push is never forced: a main that
# does not fast-forward is refused by GitHub and logged, and the mirror
# waits for an operator.
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
git init --quiet --bare /tmp/canvas.git
cd /tmp/canvas.git
pushed=""
while :; do
  if git fetch --quiet "$MIRROR_SOURCE" +refs/heads/main:refs/heads/main; then
    tip=$(git rev-parse refs/heads/main)
    if [ "$tip" != "$pushed" ]; then
      if git push --quiet "$remote" refs/heads/main:refs/heads/main; then
        echo "canvas-mirror: pushed main $tip to $MIRROR_REPO"
        pushed=$tip
      else
        echo "canvas-mirror: push of $tip to $MIRROR_REPO failed (not a fast-forward, or GitHub refused)"
      fi
    fi
  else
    echo "canvas-mirror: fetch from $MIRROR_SOURCE failed"
  fi
  sleep "$MIRROR_INTERVAL"
done
