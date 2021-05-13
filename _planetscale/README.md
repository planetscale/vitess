# Vitess Private Fork

This is PlanetScale's private fork of Vitess with proprietary patches and extensions.

PSDB Cloud and PSDB Operator run Vitess builds from this fork. This allows us to deploy
builds with either temporary or permanent changes relative to upstream Vitess,
such as embargoed security fixes or extra, proprietary features.

Since images built from this fork contain proprietary bits that must be licensed,
we publish them to an access-controlled registry.

You should _not_ use this fork if you're working on a pull request that will be sent
to the upstream, open-source [Vitess](https://github.com/vitessio/vitess) repository.
This fork is only for code that we intend to remain proprietary, either temporarily
or indefinitely.

Use our public fork, [planetscale/vitess](https://github.com/planetscale/vitess),
to prepare open-source pull requests. Note that any changes you submit to upstream
Vitess will automatically get pulled down to our fork, so you don't need to do
anything special after the upstream PR merges.

## Setup

Clone the repository as usual. If you also develop for the open source vitess,
we recommend you name vitess-private as the `origin` remote. This way, an accidental
push will land in vitess-private instead of proprietary work gettting accidentally
pushed to the publicly accessible `planetscale/vitess` repo. Here is a sample
configurations:

```
~/...vitess> git remote -vv
origin  git@github.com:planetscale/vitess-private.git (fetch)
origin  git@github.com:planetscale/vitess-private.git (push)
upstream        git@github.com:vitessio/vitess.git (fetch)
upstream        I'm sorry, Dave. I'm afraid I can't do that. (push)
vtpublic        git@github.com:planetscale/vitess.git (fetch)
vtpublic        git@github.com:planetscale/vitess.git (push)
```

## Fork Guidelines

We intend to rebase this fork on top of upstream Vitess continuously and automatically.
To minimize the occurrence of conflicts that will need to be fixed manually,
please follow these guidelines:

1. Whenever possible, avoid modifying files that exist upstream.

   These modifications are difficult to track and maintain because they tend to
   cause conflicts and it's hard to tell exactly what we changed relative
   to upstream.

   In many cases, we should be able to add custom behavior by creating an entirely
   new Go package, then importing and registering it in the appropriate
   `go/cmd` directories.

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

## Branch maintenance instructions

The goal of the `main` branch is not to represent our change history. It will
instead represent how our branch differs from the open source vitess project:

* All commits on this branch will be on top of the latest pull of vitess.
* Each commit will represent the specific modification we want to make over
  the existing vitess repo. For example, there will be only one commit for
  `kmsbackup`, and it will be continuously replaced by new commits as work
  on it evolves.

This approach adds some maintenance burden at the time of merging commits.
However, our additions on top of open source vitess should be minimal.
So, this should not grow on us.

The normal developer workflow should not change: create a feature branch
as usual, push it, create PRs, and merge after review, just as usual.
The PRs along with the associated branches will essentially be our way
of tracking history.

After the PR is merged:
* Create a new branch from prior to the merge commit.
* Cherry-pick the commit inside the PR (not the merge commit).
* Rebase against upstream master and fixup the cherry-pick with the
  commit that represents the piece of work it modifies.
* Update the `Annotate` commit, which is a human readable version of
  the current state of the branch.
* Create a copy the original `main` branch (git checkout -b copied-branch)
* Replace `main` with the new branch (git push --force).
