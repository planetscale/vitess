[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc)
[![Build Status](https://travis-ci.org/vitessio/vitess.svg?branch=master)](https://travis-ci.org/vitessio/vitess/builds)
[![codebeat badge](https://codebeat.co/badges/51c9a056-1103-4522-9a9c-dc623821ea87)](https://codebeat.co/projects/github-com-youtube-vitess)
[![Go Report Card](https://goreportcard.com/badge/vitess.io/vitess)](https://goreportcard.com/report/vitess.io/vitess)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fvitessio%2Fvitess.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fvitessio%2Fvitess?ref=badge_shield)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1724/badge)](https://bestpractices.coreinfrastructure.org/projects/1724)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vitessio&metric=coverage)](https://sonarcloud.io/dashboard?id=vitessio)

# Branch description

This is a rebase of `latest` (which was based on OSS Vitess `release-14.0`) over OSS Vitess `release-15.0`.

The process followed was:

* Add tracking branch `release-15.0` to .git/config

```
[remote "upstream"]
        url = git@github.com:vitessio/vitess.git
        fetch = +refs/heads/*:refs/remotes/upstream/*
        push = notallowed
[branch "release-15.0"]
        remote = upstream
        merge = refs/heads/release-15.0

```

* Run the [git-replay](https://github.com/planetscale/git-replay) tool. Configure it like this:

```
config:
  target-start-commit: "8001aacfb00b3bb7a8a1e07f66cf8a39f7df659c"
  target-base-branch: "latest"
  target-replay-branch: "latest-15.0"
  target-root-directory: "/home/rohit/vitess-private"
  source-root-directory: "/home/rohit/vitess-private"
  source-base-branch: "release-15.0"
  ...
```

Using the tool, we replay all commits in `latest` starting from `8001aacfb00b3bb7a8a1e07f66cf8a39f7df659c`: the first
commit made on top of the last rebase (of `latest-14.0`).

The tool replays the commits on top of a new branch created like `git checkout -b latest-15.0`

### Note

Once we have run the replay the `latest-15.0` will contain a snapshot of `latest` and `release-15.0`. However, both of
the latter branches can keep getting new commits. So until we have copied over `latest-15.0` as the new `latest` (which
is done by infra after running Singularity and other psdb e2e tests) we need to keep replaying the new commits.

The new commits in `latest` can be replayed by `git-replay`:

* `git pull` on `latest`
* `git replay refresh` on `latest-15.0`, and,
* `git replay play`

The new commits in `release-15.0` will have to be manually cherry-picked into `latest-15.0`

Of course any conflicts will need to be resolved for both.

This [Google Sheet](https://docs.google.com/spreadsheets/d/1BBzIx-A3y7yCCnL8-FNV9SNiNoXk2LNL297h52Qpuwk/edit#gid=1511084840)
kept track of all commits replayed as part of the initial `git replay` process.

Additional commits to fix incorrect merges/conflict resolutions were done in this [corrective PR](https://github.com/planetscale/vitess-private/pull/1362)

# Vitess

Vitess is a database clustering system for horizontal scaling of MySQL through generalized sharding.

By encapsulating shard-routing logic, Vitess allows application code and database queries to remain agnostic to the
distribution of data onto multiple shards. With Vitess, you can even split and merge shards as your needs grow, with an
atomic cutover step that takes only a few seconds.

Vitess has been a core component of YouTube's database infrastructure since 2011, and has grown to encompass tens of
thousands of MySQL nodes.

For more about Vitess, please visit [vitess.io](https://vitess.io).

Vitess has a growing community. You can view the list of adopters
[here](https://github.com/vitessio/vitess/blob/main/ADOPTERS.md).

## Reporting a Problem, Issue, or Bug

To report a problem, the best way to get attention is to create a
GitHub [issue](.https://github.com/vitessio/vitess/issues ) using proper severity level based on
this [guide](https://github.com/vitessio/vitess/blob/main/SEVERITY.md).

For topics that are better discussed live, please join the [Vitess Slack](https://vitess.io/slack) workspace. You may
post any questions on the #general channel or join some of the special-interest channels.

Follow [Vitess Blog](https://blog.vitess.io/) for low-frequency updates like new features and releases.

## Security

### Reporting Security Vulnerabilities

To report a security vulnerability, please email [vitess-maintainers](mailto:cncf-vitess-maintainers@lists.cncf.io).

See [Security](SECURITY.md) for a full outline of the security process.

### Security Audit

A third party security audit was performed by Cure53. You can see the full report [here](doc/VIT-01-report.pdf).

## License

Unless otherwise noted, the Vitess source files are distributed under the Apache Version 2.0 license found in the
LICENSE file.

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fvitessio%2Fvitess.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fvitessio%2Fvitess?ref=badge_large)
