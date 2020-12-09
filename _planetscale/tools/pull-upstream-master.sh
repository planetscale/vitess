#!/usr/bin/env bash

set -euo pipefail

# Stage changes for a merge, but don't commit it.
git pull --no-commit upstream master

# Bail out if there are no changes staged.
git diff --cached --quiet && exit 0

# Remember which upstream commit we last merged.
git rev-parse upstream/master > _planetscale/version/upstream_commit.txt
git add _planetscale/version/upstream_commit.txt

# Now commit the merge.
git commit -n -m "Merge branch 'master' of https://github.com/vitessio/vitess"
