---
title: Point In Time Recovery
aliases: ['/docs/recovery/pitr','/docs/reference/pitr/']
---

## Point in Time Recovery

### Supported Databases
- MySQL 5.7
### Believed to work, but untested
- MySQL 8.0

### Introduction

The Point in Time Recovery feature in Vitess enables recovery of data to a specific point time (timestamp). There can be multiple recovery requests active at the same time. It is possible to recover across sharding actions, i.e. you can recover to a time when there were two shards even though at present there are four.

Point in Time Recovery leverages two Vitess features:
1. The use of `SNAPSHOT` keyspaces for recovery of the last backup before a requested specific timestamp to restore to.
2. Integration with a binlog server to allow vttablet to apply binary logs from the recovered backup up to the specified timestamp.

### Use cases
- Accidental deletion of data, e.g. dropping a table by mistake, running an UPDATE or DELETE with an incorrect WHERE clause, etc.
- Corruption of data due to application bugs.
- Corruption of data due to MySQL bugs or underlying hardware (e.g. storage) problems.

### Preconditions
- There should be a Vitess backup taken before the desired point in time.
- There should be continuous binlogs available from the backup time to the desired point in time.
- This feature is tested using [Ripple](https://github.com/google/mysql-ripple) as the binlog server.  However, it should be possible to use a MySQL instance as source for the binlogs as well.

### Example usage

To use this feature, you need a usable backup of Vitess data and continuous binlogs.

Here is how you can create a backup.

```sh
$ vtctlclient -server <vtctld_host>:<vtctld_port> Backup zone1-101
```

Here `zone1-101` is the tablet alias of a replica tablet in the shard that you
want to back up.  Note that you can also use `vtctlclient BackupShard` to just
specify a keyspace and shard, and have Vitess choose the tablet to run the
backup for you, instead of having to specify the tablet alias explicitly.

To maintain continuous binlogs, you need to have a binlog server pointing to
the master (or a replica, assuming that the replica is also maintaining its
own binlogs, which is the default Vitess configuration). You can use
[Ripple](https://github.com/google/mysql-ripple) as a binlog server, although
there are other options; and you could use an existing MySQL server as well.

If you use Ripple, you will need to configure it yourself, and ensure you take
care of the following:
 - You should have a highly available binlog server setup. If the binlog
   server goes down, you need to ensure that it is back up and able
   to synchronize the MySQL binary logs from its upstream MySQL server
   before the upstream server deletes the current binlog.  If you
   do not do this, you will end up with gaps in your binlogs, which
   could make restoring to a specific point in time impossible. Make
   sure that you setup your operational and monitoring procedures
   accordingly.
 - The binlog files should be safely kept at some reliable and recoverable
   location (e.g. AWS S3, remote file storage).

Once the above is done, you can proceed with doing a recovery.

#### Recovery Procedure

First, you need to create a `SNAPSHOT` keyspace with a `base_keyspace`
pointing to the original keyspace you are recovering the backup of.
This can be done by using following:

```sh
$ vtctlclient -server <vtctld_host>:<vtctld_port> CreateKeyspace -keyspace_type=SNAPSHOT -base_keyspace=originalks -snapshot_time=2020-07-17T18:25:20Z restoreks
```

 Here:
 - `originalks` is the base keyspace, i.e. the keyspace we took a backup of,
 and are trying to recover.
 - `snapshot_time` is the timestamp of the point in time to we want to recover
 to. Note the use of the `Z` in the timestamp, indicating it is expressed
 in UTC.
 - `restoreks` is the name of recovery keyspace, i.e. the keyspace to which
 we are restoring our backup.

 Next, you can launch the vttablet, which as part of vttablet's normal
 initialization procedure will look for a backup to restore. It will
 detect the meta-information you added on the keyspace topology node
 when creating the keyspace above.  It will then use that information
 to restore the last backup earlier than the timestamp provided for the
 specific shard the vttablet is in.

 Here are the command line arguments vttablet uses in this
 process.  You may already be using some of these as part of your
 normal vttablet initialization parameters (e.g. if you are using the
 Vitess K8s operator):
 - `-init_keyspace restoreks` - here `restoreks` is the recovery keyspace
 name which we created earlier
 - `-init_db_name_override vt_originalks` - here `vt_originalks` is the
 name of the original underlying database for the keyspace that you backed
 up and want to restore.  Usually, this takes the form of `vt_` prepended
 to the keyspace name. However, the original underlying database could
 also have been using an `-init_db_name_override` directive of its own,
 and this value should then be set to match that.
 - `-init_shard 0` - here `0` is the shard name (or range) which we want
 to recover.
 - `-binlog_host x.x.x.x` - hostname or IP address of binlog server.
 - `-binlog_port XXXX` - TCP port of binlog server.
 - `-binlog_user XXXX` - username to access binlog server.
 - `-binlog_password YYYY` - password to access binlog server.
 - `-pitr_gtid_lookup_timeout duration` - See below for details.

And then, depending on your backup storage implementation, you can use a
variety of flags:
 - `-backup_storage_implementation file` - for plain file backup type.
 If you use this option, you will also need to specify:
   - `-file_backup_storage_root` - with a path pointing to your backup
 storage location.
 - `-backup_storage_implementation s3` - for backing up to S3. If you
 use this option, you may need additional flags like:
   - `-s3_backup_aws_region`
   - `-s3_backup_storage_bucket`
   - `-s3_backup_storage_root`
 - There are more `-backup_storage_implementation` options like `gcs` and
  others.

You will also probably want to use other flags for backup and restore like:
 - `-backup_engine_implementation xtrabackup` - Use Percona Xtrabackup to
 take online backups. Without this flag, the mysql instance on the replica
 being backed up will be shut down during the backup.
 - `-backup_storage_compress true` - gzip compress the backup (default is
 true).
You need to be consistent in your use of these flags for backup and restore.

Once the restore of the last backup earlier than the `snapshot_time` timestamp
is completed, the vttablet proceeds to use the `binlog_*` parameters to
connect to the binlog server and then apply all binlog events from the time
of the backup until the timestamp provided.

Since the last backup for each shard making up the keyspace could be taken at
different points in time, the amount of time that it takes to apply these events
may differ between restores of different shards in the keyspace.

Note that to restore to the specified `snapshot_time` timestamp, vttablet needs
to find the GTID corresponding to the last event before this timestamp from
the binlog server. This is an expensive operation and may take some time. By
default the timeout for this operation is one minute (1m). This can be changed
by setting the vttablet `-pitr_gtid_lookup_timeout` flag.

VTGate will automatically exclude tablets belonging to snapshot keyspaces from
query routing unless they are specifically addressed using `USE restoreks`
or by using queries of the form `SELECT ... FROM restoreks.table`.

The base keyspace's vschema will be copied over to the new snapshot keyspace
as a default. If desired this can be overwritten by the user. Care needs to
be taken to set `require_explicit_routing` to true when modifying a snapshot
keyspace's vschema, or you will bypass the VTGate routing safety feature
described above.
