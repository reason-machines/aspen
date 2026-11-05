#!/bin/sh
# Usage: new-repo <name>. Creates /srv/git/<name>.git, open for anonymous
# pushes (the container only listens on localhost).
set -eu
repo="/srv/git/$1.git"
git init --quiet --bare --initial-branch=main "$repo"
git -C "$repo" config http.receivepack true
chown -R www-data:www-data "$repo"
