# Release Instructions For Vitess-Private

We only need to create a new Vitess-Private `latest-*` branch when we do an RC-1 on Vitess. For example, when we do a v17.0.0-RC-1 on Vitess, we have to branch
`upstream` into `latest-17.0` as part of the release.

Apart from creating the `latest-*` branch, the following things are required to be done - 
1. Add the branch protection rules for `latest-*` branch.
2. Create the label - `Backport to: latest-*`. Also see if an older label can be dropped if some `latest-*` branch has stopped being deployed in PlanetScale.
3. For all the open PRs against `upstream`, add the `Backport to: latest -` label.
4. Update the `check_backport_labels.yml` workflow to reflect the addition of a new backport label (and possibly, the deletions of some).
5. Update the Vitess cherry-pick bot `constants.go` file to include the new latest branch and release branch in `LatestBranches` and `ReleaseBranches` lists respectively. Please have a look at this [PR](https://github.com/planetscale/vitess-cherrypick-bot/pull/32/files#diff-cdc7d16daf2a767578a6bb3cb40e145960555582283f094920f10eecc8f4fb33) for reference.
