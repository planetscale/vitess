# Vitess Private Fork

This is PlanetScale's private fork of Vitess with proprietary patches and extensions.

We plan to switch PSDB Cloud and PSDB Operator to run Vitess builds from this fork
once the continuous builds are configured and tested. This will allow us to deploy
builds with either temporary or permanent changes relative to upstream Vitess,
such as embargoed security fixes or extra, proprietary features.

Since images built from this fork contain proprietary bits that must be licensed,
we publish them to the access-controlled us.gcr.io/planetscale-operator/vitess registry
instead of the publicly-accessible us.gcr.io/planetscale-vitess registry.

You should _not_ use this fork if you're working on a pull request that will be sent
to the upstream, open-source [Vitess](https://github.com/vitessio/vitess) repository.
This fork is only for code that we intend to remain proprietary, either temporarily
or indefinitely.

Use our public fork, [planetscale/vitess](https://github.com/planetscale/vitess),
to prepare open-source pull requests. Note that any changes you submit to upstream
Vitess will automatically get pulled down to our fork, so you don't need to do
anything special after the upstream PR merges.

## Setup

Clone the repository as usual. You ideally shouldn't add `vitessio/vitess` as a
git remote at all, but if you do, please disable pushes to prevent accidentally
leaking proprietary branches:

```sh
git remote add upstream https://github.com/vitessio/vitess
git remote set-url upstream --push "I'm sorry, Dave. I'm afraid I can't do that."
```

## Pulling Upstream

You shouldn't normally need to manually pull upstream, since we do that automatically
on a regular period, or whenever a change is pushed to our fork's master branch.

However, if you do find you need to pull manually, please make sure to update the
`_planetscale/version/upstream_commit.txt` file with the upstream commit that you
merged.

The `_planetscale/tools/pull-upstream-master.sh` script shows how to do this.
You can just directly run that script if you don't expect to hit any merge conflicts.
If you do hit merge conflicts, the script will fail and you'll need to fix the conflicts
and manually finish the remaining steps.

## Fork Guidelines

We intend to merge upstream Vitess into this fork continuously and automatically.
To minimize the occurrence of merge conflicts that will need to be fixed manually,
please follow these guidelines:

1. Whenever possible, avoid modifying files that exist upstream.

   These modifications are difficult to track and maintain because they tend to
   cause merge conflicts and it's hard to tell exactly what we changed relative
   to upstream.

   In many cases, we should be able to add custom behavior by creating an entirely
   new Go package under the `/_planetscale` directory tree, then importing and
   registering it with a single `_planetscale_plugins.go` file somewhere in
   the `/go` directory tree.

1. If your change can't be made without modifying files that exist upstream,
   consider submitting a change upstream first to make the desired behavior
   pluggable, so we can then add our custom code purely by adding new files.

   Vitess already uses this plug-in pattern in many places because YouTube
   had many such plug-ins internally. You should be able to find an existing
   example of pluggability to use as a template. Ask in the `#eng-vitess` channel
   on the internal Slack if you need guidance.

1. If you do need to modify a file that exists upstream, such as to fix an urgent
   security issue without waiting for changes to land upstream, consider pushing
   the change (or a pluggability fix) upstream as soon as possible afterwards,
   so we can then get rid of the custom patch.

1. When adding new files or directories in a parent directory that exists upstream,
   add a `_planetscale` prefix whenever possible. For example, you might add a file
   like `go/cmd/vttablet/_planetscale_plugins.go` to register our vttablet plug-ins.

   This will ensure we don't accidentally collide with the names of files that may get
   added in upstream Vitess, and also helps us easily distinguish which files are local
   to this fork without having to consult git history.

   It is _not_ necessary to prefix files or directories inside a parent directory that
   only exists in our fork. The prefix just needs to be present at the first point along
   the file's full path at which we diverge from upstream.

   For example, this is fine:

   `go/test/endtoend/_planetscale_feature_x/feature_x_test.go`

