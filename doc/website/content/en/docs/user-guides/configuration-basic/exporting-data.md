---
title: Exporting data from Vitess
weight: 8
aliases: ['/docs/user-guides/exporting-data/'] 
---

Since [VTGate](../../../concepts/vtgate/) supports the MySQL protocol, in many
cases it is possible to use existing client utilities when connecting to
Vitess. This includes using logical dump tools such as `mysqldump`, in
certain cases.

This guide provides instructions on the required options when using these tools
against a VTGate server for the purposes of exporting data from Vitess. It is
recommended to follow the [Backup and Restore](../../operating-vitess/backup-and-restore/) guide
for regular backups, since this method is performed directly on the tablet
servers and is more efficient and safer for databases of any significant size.
The dump methods that follow are typically not suitable for production backups,
because Vitess does not implement all the locking constructs across a sharded
database that are necessary to do a consistent logical backup while writing
to the database.  As a result, you will only be guaranteed to get a 100%
consistent dump using these tools if you are sure that you are not writing
to the database while running the dump.

### mysqldump

The default invocation of `mysqldump` attempts to execute statements which are [not supported by Vitess](../../../reference/mysql-compatibility/), such as attempting to lock tables and dump GTID coordinates. The following options are required when using the `mysqldump` binary from MySQL 5.7 to export data from the `commerce` keyspace:

* `--lock-tables=off`: VTGate currently prohibits the syntax `LOCK TABLES` and `UNLOCK TABLES`.
* `--set-gtid-purged=OFF`: `mysqldump` attemps to dump GTID coordinates of a server, but in the case of VTGate this does not make sense since it could be routing to multiple servers.
* `--no-tablespaces`: This option disables dumping InnoDB tables by tablespace. This functionality is not yet supported by Vitess.
* `--skip-network-timeout`: This option is required when using `mysqldump` from MySQL 8.0 ([#5401](https://github.com/vitessio/vitess/issues/5401)) with Vitess versions before 7.0.

For example to export the `commerce` keyspace using the `mysqldump` binary from MySQL 5.7:

```sh
$ mysqldump  --lock-tables=off --set-gtid-purged=OFF --no-tablespaces commerce > commerce.sql
```

**NOTE:** You will be limited by the Vitess row limits in the size of the
tables that you can dump using this method.  The default Vitess row limit is
determined by the VTTablet option `-queryserver-config-max-result-size`
and defaults to 10000 rows.  So for an unsharded database, you will not be
able to dump tables with more than 10000 rows, or N x 10000 rows if the table
is fully sharded across N shards.  Note that you should not blindly raise your
row limits just because of this, it is an important Vitess operability
and reliability feature.  If you have large tables to dump, look into using
[go-mydumper](#go-mydumper) instead.

To restore dump files created by `mysqldump`, replay it against a Vitess
server or other MySQL server using the `mysql` command line client.

### go-mydumper

Alternatively, you can use a slight modification of the `go-mydumper` tool
to export logical dumps of a Vitess keyspace. `go-mydumper` has the
advantage of being multi-threaded, and so can run faster on a database
that has many tables.  For a database with just one or a handful of large
tables, `go-mydumper` may not be that much faster than `mysqldump`.

For information on the Vitess-compatible fork of `go-mydumper`, see
https://github.com/aquarapid/go-mydumper . Examples and instructions
are available in the [README.md](https://github.com/aquarapid/go-mydumper/blob/jacques_vitess/README.md)
in that repo.  You will need to be able to compile golang binaries
to use this tool.

`go-mydumper` creates multiple files for each backup.  To restore a
backup, you can use the `mysql` commandline client, but using the
`myloader` tool as described in the `go-mydumper` repo above is easier
and can be faster, since the loader is also multithreaded.
